//go:build native_step_integration

package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/inngest/inngest/pkg/consts"
	"github.com/inngest/inngest/pkg/execution/queue"
	"github.com/inngest/inngest/pkg/execution/state"
	"github.com/inngest/inngest/pkg/execution/state/redis_state"
	sv2 "github.com/inngest/inngest/pkg/execution/state/v2"
	queuehelper "github.com/inngest/inngest/tests/execution/queue/helper"
	"github.com/jonboulle/clockwork"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
)

// Only Now is overridden. The producer/shard use their existing clock options;
// no production Go hook, sleeps, or copied enqueue implementation is involved.
type oneshotStageClock struct {
	clockwork.Clock
	times []time.Time
	reads int
}

func (c *oneshotStageClock) Now() time.Time {
	if c.reads >= len(c.times) {
		panic("unexpected semantic clock read")
	}
	at := c.times[c.reads]
	c.reads++
	return at
}

func TestResponseOneShotClockStages(t *testing.T) {
	binary := os.Getenv("NATIVE_CLOCK_TEST_BINARY")
	require.NotEmpty(t, binary, "build the native-step-state --lib test artifact with test-helper")
	v, err := queuehelper.StartValkey(t)
	require.NoError(t, err)
	c, err := queuehelper.NewValkeyClient(v.Addr, v.Username, v.Password, false)
	require.NoError(t, err)
	defer c.Close()
	oneshotPreload(t, t.Context(), c)
	for _, tc := range []struct {
		name                          string
		priority                      int64
		zeroRun                       bool
		score, at, wall, eat, pointer int64
	}{
		{"advancing", 19, false, 1469922831259, 1699999981990, 1700000001005, 1700000001005, 1700000001},
		{"negative-priority", -19, false, 1469922869259, 1700000019990, 1700000001005, 1700000001005, 1700000001},
		{"zero-start", 19, true, 1700000000990, 1699999981990, 1700000001005, 1700000001005, 1700000001},
		{"future-producer", 19, false, 1700000000990, 1699999981990, 1700000000990, 1699999999005, 1700000000},
		{"future-boundary", 19, false, 1469922831259, 1699999981990, 1700000001005, 1700000001005, 1700000001},
		{"equal-ms", 0, true, 1700000000990, 1700000000990, 1700000000990, 1700000000990, 1700000000},
		{"sub-ms", 0, true, 1700000000990, 1700000000990, 1700000000990, 1700000000990, 1700000001},
		{"pending", 19, false, 0, 0, 0, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, lane := range []string{"go", "native"} {
				t.Run(lane, func(t *testing.T) {
					scenario := "fresh"
					if tc.name == "pending" {
						scenario = "pending"
					}
					s := newConstructedResponseSource()
					if tc.zeroRun {
						s.run.md.ID.RunID = ulid.ULID{}
					}
					s.run.item.PriorityFactor = &tc.priority
					// Create with the selected identity; preserve the same initial 17/6 claim.
					require.NoError(t, c.Do(t.Context(), c.B().Flushdb().Build()).Error())
					svc, err := oneshotState(t.Context(), c)
					require.NoError(t, err)
					created, err := svc.Create(t.Context(), sv2.CreateState{Metadata: s.run.md, Events: s.run.events})
					require.NoError(t, err)
					s.run.md = created.Metadata
					s.run.md.Metrics.MetadataSize, s.run.md.Metrics.MetadataSizeLoaded = 17, 6
					metadataKey := "{" + redis_state.StateDefaultKey + "}:metadata:" + s.run.md.ID.RunID.String()
					require.NoError(t, c.Do(t.Context(), c.B().Hset().Key(metadataKey).FieldValue().FieldValue("metadata_size", "6").Build()).Error())
					if scenario == "pending" {
						require.NoError(t, svc.SavePending(t.Context(), s.run.md.ID, []string{"s", "held"}))
					}
					if lane == "native" {
						var raw []byte
						require.NoError(t, s.withInput(t.Context(), false, func(input map[string]any) { raw, err = oneshotDescriptorBytes(s, v, input); require.NoError(t, err) }))
						data, err := json.Marshal(map[string]any{"case": tc.name, "descriptor": json.RawMessage(raw)})
						require.NoError(t, err)
						cmd := exec.CommandContext(t.Context(), binary, "--exact", "response::tests::advancing_owner_store", "--ignored", "--nocapture")
						cmd.Stdin = bytes.NewReader(data)
						output, err := cmd.CombinedOutput()
						require.NoError(t, err, "%s", output)
						require.Contains(t, string(output), "1 passed")
						t.Logf("FUNCTIONAL_PRIVATE_NATIVE case=%s output=%s", tc.name, output)
					} else {
						discovery := time.UnixMilli(1700000000990)
						producerTime := time.UnixMilli(1700000000995)
						enqueueTime, pointerTime := time.UnixMilli(1700000001005), time.UnixMilli(1700000001009)
						if tc.name == "future-producer" {
							producerTime = time.UnixMilli(1699999998000)
							enqueueTime = time.UnixMilli(1699999999005)
						}
						if tc.name == "future-boundary" {
							producerTime = time.UnixMilli(1699999998990)
						}
						if tc.name == "equal-ms" || tc.name == "sub-ms" {
							producerTime = discovery
							enqueueTime = discovery
						}
						if tc.name == "sub-ms" {
							enqueueTime = enqueueTime.Add(time.Nanosecond)
						}
						producerClock := &oneshotStageClock{Clock: clockwork.NewFakeClock(), times: []time.Time{producerTime}}
						shardClock := &oneshotStageClock{Clock: clockwork.NewFakeClock(), times: []time.Time{enqueueTime, pointerTime}}
						opts := []queue.QueueOpt{queue.WithClock(shardClock), queue.WithRunMode(queue.QueueRunMode{Partition: true}), queue.WithAllowKeyQueues(func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID) bool { return false })}
						shard := redis_state.NewQueueShard(consts.DefaultQueueShardName, redis_state.NewQueueClient(c, redis_state.QueueDefaultKey), opts...)
						registry, err := queue.NewSingleShardRegistry(shard)
						require.NoError(t, err)
						producer := queue.NewProducer(registry, queue.WithProducerClock(producerClock))
						q, err := queue.New(t.Context(), "clock-stage-control", registry, append(opts, queue.WithQueueProducer(producer))...)
						require.NoError(t, err)
						s.e.smv2, s.e.queue = svc, q
						var gen []*state.GeneratorOpcode
						require.NoError(t, json.Unmarshal(s.raw, &gen))
						step := responseStepBinding{runCtx: &s.run, gen: *gen[0], edge: s.run.item.Payload.(queue.PayloadEdge)}
						output, err := gen[0].Output()
						require.NoError(t, err)
						ctx, request := step.saveRequest(t.Context(), output)
						g := goResponseEffects{executor: s.e}
						saved := g.saveStep(ctx, request)
						require.NoError(t, saved.err)
						b := g.discoveryBinding(ctx, step, "clock-stage-group", discovery, nil, false)
						if shouldEnqueueDiscovery(saved.hasPending, gen[0].ParallelMode()) {
							require.NoError(t, g.enqueue(b))
						}
						if tc.name == "pending" {
							require.Zero(t, producerClock.reads)
							require.Zero(t, shardClock.reads)
						} else {
							require.Equal(t, 1, producerClock.reads)
							wantReads := 2
							if tc.name == "future-producer" || tc.name == "equal-ms" {
								wantReads = 1
							}
							require.Equal(t, wantReads, shardClock.reads)
							before, err := c.Do(t.Context(), c.B().Hgetall().Key("{queue}:queue:item").Build()).AsStrMap()
							require.NoError(t, err)
							// Dedupe retains the first item despite different stage clocks.
							producerClock.times, producerClock.reads = []time.Time{producerTime.Add(10 * time.Second)}, 0
							shardClock.times, shardClock.reads = []time.Time{enqueueTime.Add(10 * time.Second), pointerTime.Add(10 * time.Second)}, 0
							ctx, request = step.saveRequest(t.Context(), output)
							saved = g.saveStep(ctx, request)
							require.ErrorIs(t, saved.err, state.ErrIdempotentResponse)
							b = g.discoveryBinding(ctx, step, "second-clock-group", discovery.Add(10*time.Second), nil, false)
							require.ErrorIs(t, g.enqueue(b), queue.ErrQueueItemExists)
							after, err := c.Do(t.Context(), c.B().Hgetall().Key("{queue}:queue:item").Build()).AsStrMap()
							require.NoError(t, err)
							require.Equal(t, before, after, "duplicate preserves the original stored item and timestamps")
						}
					}
					persisted, err := svc.LoadMetadata(t.Context(), s.run.md.ID)
					require.NoError(t, err)
					require.Equal(t, 17, persisted.Metrics.MetadataSize)
					raw, err := c.Do(t.Context(), c.B().Hgetall().Key("{queue}:queue:item").Build()).AsStrMap()
					require.NoError(t, err)
					if tc.name == "pending" {
						require.Empty(t, raw)
						return
					}
					require.Len(t, raw, 1)
					var item queue.QueueItem
					for _, value := range raw {
						require.NoError(t, json.Unmarshal([]byte(value), &item))
					}
					require.Equal(t, tc.at, item.AtMS)
					require.Equal(t, tc.wall, item.WallTimeMS)
					require.Equal(t, tc.eat, item.EnqueuedAt)
					for _, key := range []string{"{queue}:queue:sorted:" + s.run.md.ID.FunctionID.String(), "{queue}:idx:run:" + s.run.md.ID.RunID.String()} {
						score, err := c.Do(t.Context(), c.B().Zscore().Key(key).Member(item.ID).Build()).AsInt64()
						require.NoError(t, err)
						require.Equal(t, tc.score, score)
					}
					for key, member := range map[string]string{"{queue}:partition:sorted": s.run.md.ID.FunctionID.String(), "{queue}:accounts:sorted": s.run.md.ID.Tenant.AccountID.String(), "{queue}:accounts:" + s.run.md.ID.Tenant.AccountID.String() + ":partition:sorted": s.run.md.ID.FunctionID.String()} {
						score, err := c.Do(t.Context(), c.B().Zscore().Key(key).Member(member).Build()).AsInt64()
						require.NoError(t, err)
						require.Equal(t, tc.pointer, score)
					}
					// Fresh Go decoding/consumer math must use the actual persisted wall time.
					require.Equal(t, time.Duration(1700000002000-tc.wall)*time.Millisecond, item.Latency(time.UnixMilli(1700000002000)))
					require.Equal(t, time.Duration(max(tc.at-tc.eat, 0))*time.Millisecond, item.ExpectedDelay())
					t.Logf("FUNCTIONAL_CLOCK_STORE lane=%s case=%s at=%d wt=%d eat=%d fifo=%d ready=%d", lane, tc.name, item.AtMS, item.WallTimeMS, item.EnqueuedAt, tc.score, tc.pointer)
				})
			}
		})
	}
}
