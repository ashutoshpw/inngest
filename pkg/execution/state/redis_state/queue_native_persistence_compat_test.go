package redis_state

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/inngest/inngest/pkg/consts"
	osqueue "github.com/inngest/inngest/pkg/execution/queue"
	"github.com/jonboulle/clockwork"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
)

func compactNativeLiteral(t *testing.T, b json.RawMessage) string {
	t.Helper()
	var out bytes.Buffer
	require.NoError(t, json.Compact(&out, b))
	return out.String()
}
func nativeQueueProducer(t *testing.T, c rueidis.Client, prefix string, keys bool, now int64) (osqueue.Queue, RedisQueueShard) {
	t.Helper()
	opts := []osqueue.QueueOpt{osqueue.WithClock(clockwork.NewFakeClockAt(time.UnixMilli(now))), osqueue.WithRunMode(osqueue.QueueRunMode{Partition: true, ShadowPartition: keys}), osqueue.WithAllowKeyQueues(func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID) bool { return keys })}
	shard := NewQueueShard(consts.DefaultQueueShardName, NewQueueClient(c, prefix), opts...)
	registry, err := osqueue.NewSingleShardRegistry(shard)
	require.NoError(t, err)
	q, err := osqueue.New(t.Context(), "native-step-baseline", registry, opts...)
	require.NoError(t, err)
	return q, shard
}
func nativeQueueKeys(f nativeQueueFixture, prefix string, keys bool) []string {
	out := slices.Clone(f.Keys)
	backlog, shadow, normalize := "-", "", "-"
	if keys {
		shadow = "00000000-0000-0000-0000-000000000001"
		backlog = "fn:" + shadow
		normalize = "partition:" + shadow + ":sorted"
	}
	for j, k := range out {
		k = strings.ReplaceAll(k, "QUEUE", prefix)
		k = strings.ReplaceAll(k, "BACKLOG", backlog)
		k = strings.ReplaceAll(k, "SHADOW", shadow)
		k = strings.ReplaceAll(k, "NORMALIZE", normalize)
		out[j] = k
	}
	return out
}
func nativeQueueArgs(t *testing.T, f nativeQueueFixture, keys bool, item string) []string {
	t.Helper()
	shadow, backlog, backlogID, enable := `{"fv":0}`, `{}`, "", "0"
	if keys {
		shadow = compactNativeLiteral(t, f.Shadow)
		backlog = compactNativeLiteral(t, f.Backlog)
		backlogID = "fn:00000000-0000-0000-0000-000000000001"
		enable = "1"
	}
	return []string{item, f.HashID, strconv.FormatInt(f.ScoreMS, 10), strconv.FormatInt(f.PointerSeconds, 10), strconv.FormatInt(f.NowMS, 10), compactNativeLiteral(t, f.Partition), "00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000003", "01ARZ3NDEKTSV4RRFFQ69G5FAV", enable, shadow, backlog, backlogID, ""}
}
func requireNativeQueueSnapshot(t *testing.T, f nativeQueueFixture, prefix string, keys bool, actual nativeSnapshot) {
	t.Helper()
	fixture := loadNativeEffects(t)
	fixture.Queue = f
	require.Equal(t, expectedNativeQueueSnapshot(t, fixture, prefix, keys, compactNativeLiteral(t, f.Item)), actual, "entire isolated store with absent placeholders")
}

func TestNativeStepQueuePersistenceCompatibility(t *testing.T) {
	f := loadNativeEffects(t)
	qf := f.Queue
	body := nativeScript(t, f.Scripts["queue/enqueue"])
	writer, reader := nativeStoreClients(t)
	ctx := t.Context()
	require.NoError(t, reader.Do(ctx, reader.B().ScriptLoad().Script(body).Build()).Error())
	for input, answer := range qf.HashAnswers {
		require.Equal(t, answer, osqueue.HashID(ctx, input))
	}
	for _, keys := range []bool{false, true} {
		for _, prefix := range []string{QueueDefaultKey, "native:queue"} {
			for _, scenario := range []string{"fresh", "identical", "conflict", "commit-lost-reply", "run-id-fallback"} {
				t.Run(fmt.Sprintf("keys=%t/%s/%s", keys, prefix, scenario), func(t *testing.T) {
					qf := qf
					if scenario == "run-id-fallback" {
						original := qf.HashID
						qf.JobID = qf.FallbackJobID
						qf.HashID = qf.HashAnswers[qf.JobID]
						qf.Keys = slices.Clone(qf.Keys)
						qf.Keys[5] = strings.ReplaceAll(qf.Keys[5], original, qf.HashID)
						qf.Item = bytes.ReplaceAll(qf.Item, []byte(original), []byte(qf.HashID))
					}
					require.NoError(t, writer.Do(ctx, writer.B().Flushdb().Build()).Error())
					require.Empty(t, snapshotNativeStore(t, ctx, reader))
					fault := &nativeScriptClient{Client: writer, t: t, manifest: f.Scripts["queue/enqueue"], body: body, keys: nativeQueueKeys(qf, prefix, keys), args: nativeQueueArgs(t, qf, keys, compactNativeLiteral(t, qf.Item)), scalar: true}
					producer, shard := nativeQueueProducer(t, fault, prefix, keys, qf.NowMS)
					var item osqueue.QueueItem
					require.NoError(t, json.Unmarshal(qf.Item, &item))
					item.Data.JobID = &qf.JobID
					fault.afterCommit = func() {
						snap := snapshotNativeStore(t, ctx, reader)
						requireNativeQueueSnapshot(t, qf, prefix, keys, snap)
						t.Logf("independent committed enqueue snapshot=%#v", snap)
					}
					if scenario == "commit-lost-reply" {
						fault.mode = "after"
					}
					err := producer.Enqueue(ctx, item.Data, time.UnixMilli(qf.NowMS), osqueue.EnqueueOpts{})
					if scenario == "commit-lost-reply" {
						require.ErrorIs(t, err, context.Canceled)
					} else {
						require.NoError(t, err)
					}
					require.Equal(t, 1, fault.calls)
					require.Equal(t, [][]int64{{0}}, fault.results)
					post := snapshotNativeStore(t, ctx, reader)
					requireNativeQueueSnapshot(t, qf, prefix, keys, post)
					seenTTL, err := reader.Do(ctx, reader.B().Pttl().Key(fault.keys[5]).Build()).AsInt64()
					require.NoError(t, err)
					require.Equal(t, int64(-2), seenTTL, "enqueue never creates seen tombstone")
					freshProducer, freshShard := nativeQueueProducer(t, reader, prefix, keys, qf.NowMS)
					loaded, err := freshShard.LoadQueueItem(ctx, qf.HashID)
					require.NoError(t, err)
					require.NotNil(t, loaded.Data.JobID)
					require.Equal(t, qf.HashID, *loaded.Data.JobID)
					if scenario != "fresh" {
						if scenario == "conflict" {
							item.Data.Attempt = 2
						}
						err = freshProducer.Enqueue(ctx, item.Data, time.UnixMilli(qf.NowMS), osqueue.EnqueueOpts{})
						require.ErrorIs(t, err, osqueue.ErrQueueItemExists)
						var exists osqueue.QueueItemExistsError
						require.ErrorAs(t, err, &exists)
						require.Equal(t, qf.HashID, exists.JobID)
						require.NotNil(t, exists.RunID)
						require.Equal(t, item.Data.Identifier.RunID, *exists.RunID)
						require.Equal(t, post, snapshotNativeStore(t, ctx, reader), "fresh producer duplicate retains original bytes and all indexes")
					}
					// A genuine dequeue supplies the only tombstone write in this witness.
					// One real second is enough; advancing the fake queue clock cannot expire it.
					if scenario == "fresh" {
						period := time.Second
						loaded.IdempotencyPeriod = &period
						require.NoError(t, shard.Dequeue(ctx, *loaded))
						value, err := reader.Do(ctx, reader.B().Get().Key(fault.keys[5]).Build()).ToString()
						require.NoError(t, err)
						require.Empty(t, value)
						ttl, err := reader.Do(ctx, reader.B().Pttl().Key(fault.keys[5]).Build()).AsInt64()
						require.NoError(t, err)
						require.Greater(t, ttl, int64(0))
						require.LessOrEqual(t, ttl, int64(1000))
						err = freshProducer.Enqueue(ctx, item.Data, time.UnixMilli(qf.NowMS), osqueue.EnqueueOpts{})
						require.ErrorIs(t, err, osqueue.ErrQueueItemExists)
						var exists osqueue.QueueItemExistsError
						require.True(t, errors.As(err, &exists))
						require.Nil(t, exists.RunID)
						require.Eventually(t, func() bool {
							v, e := reader.Do(ctx, reader.B().Pttl().Key(fault.keys[5]).Build()).AsInt64()
							return e == nil && v == -2
						}, 3*time.Second, 25*time.Millisecond)
						require.NoError(t, freshProducer.Enqueue(ctx, item.Data, time.UnixMilli(qf.NowMS), osqueue.EnqueueOpts{}))
					}
					t.Logf("raw enqueue=%s snapshot=%#v", post[fault.keys[0]].Value, post)
				})
			}
		}
	}
}
