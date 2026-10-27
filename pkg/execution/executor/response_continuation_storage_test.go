//go:build linux && response_continuation_spike && cgo

package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	native "github.com/inngest/inngest/internal/responsecontinuationspike"
	"github.com/inngest/inngest/pkg/consts"
	"github.com/inngest/inngest/pkg/enums"
	"github.com/inngest/inngest/pkg/execution/queue"
	"github.com/inngest/inngest/pkg/execution/state"
	"github.com/inngest/inngest/pkg/execution/state/redis_state"
	sv2 "github.com/inngest/inngest/pkg/execution/state/v2"
	"github.com/inngest/inngest/pkg/logger"
	"github.com/inngest/inngest/pkg/tracing"
	"github.com/inngest/inngest/pkg/tracing/meta"
	queuehelper "github.com/inngest/inngest/tests/execution/queue/helper"
	"github.com/jonboulle/clockwork"
	"github.com/oklog/ulid/v2"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
)

// These constructors are test-private. Each witness uses real production state
// and queue adapters, and an independent client/adapter for persisted reads.
func nativeStatePort(t *testing.T, ctx context.Context, client rueidis.Client, stateKey, queueKey string) sv2.RunService {
	t.Helper()
	unsharded := redis_state.NewUnshardedClient(client, stateKey, queueKey)
	sharded := redis_state.NewShardedClient(redis_state.ShardedClientOpts{UnshardedClient: unsharded, FunctionRunStateClient: client, BatchClient: client, StateDefaultKey: stateKey, QueueDefaultKey: queueKey, FnRunIsSharded: redis_state.AlwaysShardOnRun})
	manager, err := redis_state.New(ctx, redis_state.WithShardedClient(sharded))
	require.NoError(t, err)
	return redis_state.MustRunServiceV2(manager)
}
func nativeQueuePort(t *testing.T, ctx context.Context, client rueidis.Client, queueKey string, clock clockwork.Clock, keyQueues bool) (queue.Queue, string) {
	t.Helper()
	opts := []queue.QueueOpt{queue.WithClock(clock), queue.WithRunMode(queue.QueueRunMode{Partition: true, ShadowPartition: keyQueues}), queue.WithAllowKeyQueues(func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID) bool { return keyQueues })}
	qc := redis_state.NewQueueClient(client, queueKey)
	shard := redis_state.NewQueueShard(consts.DefaultQueueShardName, qc, opts...)
	registry, err := queue.NewSingleShardRegistry(shard)
	require.NoError(t, err)
	q, err := queue.New(ctx, "response-continuation-witness", registry, opts...)
	require.NoError(t, err)
	return q, qc.KeyGenerator().QueueItem()
}

type nativeCommittedQueue struct {
	queue.Queue
	mu    sync.Mutex
	calls int
	after error
}

func (q *nativeCommittedQueue) Enqueue(ctx context.Context, item queue.Item, at time.Time, opts queue.EnqueueOpts) error {
	q.mu.Lock()
	q.calls++
	q.mu.Unlock()
	if err := q.Queue.Enqueue(ctx, item, at, opts); err != nil {
		return err
	}
	return q.after
}

// This does not claim distributed exactly-once delivery or lost-ack recovery.
// Save/Enqueue commit then return the original error, and this SAME session
// observes it and never invokes the effect again. Fresh adapters read the store.
func TestResponseContinuationIntegratedValkey(t *testing.T) {
	ctx := logger.WithStdlib(context.Background(), logger.VoidLogger())
	valkey, err := queuehelper.StartValkey(t)
	require.NoError(t, err)
	t.Log("Valkey image valkey/valkey:8.0.3; container platform linux/amd64; native execution linux/amd64")
	client, err := queuehelper.NewValkeyClient(valkey.Addr, valkey.Username, valkey.Password, false)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	reader, err := queuehelper.NewValkeyClient(valkey.Addr, valkey.Username, valkey.Password, false)
	require.NoError(t, err)
	t.Cleanup(reader.Close)
	for _, keyQueues := range []bool{false, true} {
		for _, failure := range []string{"none", "save-after-commit", "enqueue-after-commit"} {
			t.Run(fmt.Sprintf("keys=%t/%s", keyQueues, failure), func(t *testing.T) {
				prefix := "native-response-" + uuid.NewString()
				stateKey, queueKey := prefix+":state", prefix+":queue"
				ops := []*state.GeneratorOpcode{{Op: enums.OpcodeStepRun, ID: "saved", Data: json.RawMessage(`{"value":7}`)}}
				if failure == "none" {
					ops = append(ops, &state.GeneratorOpcode{Op: enums.OpcodeStepPlanned, ID: "planned", Name: "next"})
				}
				e, i, _, recordingQueue, events := newStagedBatch(t, ops...)
				e.tracerProvider = tracing.NewOtelTracerProvider(&continuationExporter{q: recordingQueue, events: events}, 0)
				i.md.ID.RunID = ulid.Make()
				i.md.Config.Idempotency = prefix
				i.md.Config.RequestVersion = -1
				service := nativeStatePort(t, ctx, client, stateKey, queueKey)
				independent := nativeStatePort(t, ctx, reader, stateKey, queueKey)
				_, err := service.Create(ctx, sv2.CreateState{Metadata: i.md, Events: []json.RawMessage{json.RawMessage(`{"name":"native.test","data":{}}`)}})
				require.NoError(t, err)
				if failure != "none" {
					require.NoError(t, service.SavePending(ctx, i.md.ID, []string{"saved"}))
				}
				e.smv2 = service
				realQueue, itemsKey := nativeQueuePort(t, ctx, client, queueKey, e.clock, keyQueues)
				q := &nativeCommittedQueue{Queue: realQueue}
				e.queue = q
				original := errors.New("real committed effect returned an error")
				saveCalls := 0
				if failure == "save-after-commit" {
					e.smv2 = &nativeSavePort{RunService: service, save: func(ctx context.Context, id sv2.ID, step string, data []byte) (bool, error) {
						saveCalls++
						pending, err := service.SaveStep(ctx, id, step, data)
						if err != nil {
							return pending, err
						}
						return pending, original
					}}
				}
				if failure == "enqueue-after-commit" {
					q.after = original
				}
				d, err := newContinuationDriver(ctx, e, i, i.resp)
				require.NoError(t, err)
				result := d.run()
				require.NoError(t, d.drain())
				if failure == "none" {
					require.NoError(t, result)
				} else {
					require.Error(t, result)
					require.Contains(t, result.Error(), original.Error())
					require.Same(t, original, d.originalError(3), "retain the original observed Go error, including an always-retry disposition")
					if failure == "save-after-commit" {
						require.Same(t, original, result)
						require.Equal(t, 1, saveCalls)
						require.Empty(t, events.snapshot())
					} else {
						require.True(t, queue.IsAlwaysRetryable(result))
					}
				}
				outputs, err := independent.LoadSteps(ctx, i.md.ID)
				require.NoError(t, err)
				require.JSONEq(t, `{"data":{"value":7}}`, string(outputs["saved"]))
				require.Len(t, outputs, 1)
				pending, err := independent.LoadPending(ctx, i.md.ID)
				require.NoError(t, err)
				if failure == "none" {
					require.Equal(t, []string{"planned"}, pending)
				} else {
					require.Empty(t, pending)
				}
				md, err := independent.LoadMetadata(ctx, i.md.ID)
				require.NoError(t, err)
				require.Equal(t, 2, md.Config.RequestVersion)
				stored, err := reader.Do(ctx, reader.B().Hgetall().Key(itemsKey).Build()).AsStrMap()
				require.NoError(t, err)
				if failure == "save-after-commit" {
					require.Empty(t, stored)
					require.Zero(t, q.calls)
				} else {
					require.Len(t, stored, 1)
					require.Equal(t, 1, q.calls)
					for id, bytes := range stored {
						var item queue.QueueItem
						require.NoError(t, json.Unmarshal([]byte(bytes), &item))
						require.NotEmpty(t, item.Data.Metadata[meta.PropagationKey], "actual prepared carrier reaches persisted queue item")
						require.Equal(t, id, item.ID)
						require.Equal(t, i.md.ID.RunID, item.Data.Identifier.RunID)
						payload, ok := item.Data.Payload.(queue.PayloadEdge)
						require.True(t, ok)
						require.Equal(t, "function", payload.Edge.Incoming)
						jobID := i.md.IdempotencyKey() + "-saved"
						if failure == "none" {
							jobID = i.md.IdempotencyKey() + "-planned-plan"
							require.Equal(t, "planned", payload.Edge.IncomingGeneratorStep)
							require.NotNil(t, item.Data.ParallelCoalesceKey)
						} else {
							require.Equal(t, "saved", payload.Edge.Outgoing)
						}
						require.Equal(t, queue.HashID(ctx, jobID), id)
					}
				}
				// Neither restarting the Go driver nor resuming native state can replay
				// the committed port in this SAME session.
				require.ErrorContains(t, d.run(), "already started or closed")
				_, err = d.session.Resume(native.Key{}, "Done")
				require.ErrorContains(t, err, "closed")
				if failure == "save-after-commit" {
					require.Equal(t, 1, saveCalls)
				}
				if failure == "enqueue-after-commit" {
					require.Equal(t, 1, q.calls)
				}
			})
		}
	}
}
