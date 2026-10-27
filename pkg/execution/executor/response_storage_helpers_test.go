package executor

import (
	"context"
	"encoding/json"
	"maps"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/inngest/inngest/pkg/consts"
	"github.com/inngest/inngest/pkg/execution/queue"
	"github.com/inngest/inngest/pkg/execution/state/redis_state"
	sv2 "github.com/inngest/inngest/pkg/execution/state/v2"
	"github.com/jonboulle/clockwork"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
)

// These constructors are test-private. Each witness uses real production state
// and queue adapters, and an independent client/adapter for persisted reads.
func nativeStatePort(t *testing.T, ctx context.Context, client rueidis.Client, stateKey, queueKey string) sv2.RunService {
	t.Helper()
	return nativeStatePortMode(t, ctx, client, stateKey, queueKey, true)
}
func nativeStatePortMode(t *testing.T, ctx context.Context, client rueidis.Client, stateKey, queueKey string, shardedMode bool) sv2.RunService {
	t.Helper()
	fn := redis_state.NeverShardOnRun
	if shardedMode {
		fn = redis_state.AlwaysShardOnRun
	}
	unsharded := redis_state.NewUnshardedClient(client, stateKey, queueKey)
	sharded := redis_state.NewShardedClient(redis_state.ShardedClientOpts{UnshardedClient: unsharded, FunctionRunStateClient: client, BatchClient: client, StateDefaultKey: stateKey, QueueDefaultKey: queueKey, FnRunIsSharded: fn})
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

type responseStoredValue struct {
	Type  string
	Value any
	PTTL  int64
}
type responseSnapshot map[string]responseStoredValue

// Store truth comes only from this separate cache-disabled connection. Scan all
// keys, including placeholders, so unexpected writes cannot escape comparison.
func snapshotResponseStore(t *testing.T, ctx context.Context, r rueidis.Client) responseSnapshot {
	t.Helper()
	keys := []string{}
	var cursor uint64
	for {
		v, err := r.Do(ctx, r.B().Scan().Cursor(cursor).Count(1000).Build()).AsScanEntry()
		require.NoError(t, err)
		keys = append(keys, v.Elements...)
		cursor = v.Cursor
		if cursor == 0 {
			break
		}
	}
	slices.Sort(keys)
	keys = slices.Compact(keys)
	out := responseSnapshot{}
	for _, key := range keys {
		typ, err := r.Do(ctx, r.B().Type().Key(key).Build()).ToString()
		require.NoError(t, err)
		ttl, err := r.Do(ctx, r.B().Pttl().Key(key).Build()).AsInt64()
		require.NoError(t, err)
		var value any
		switch typ {
		case "string":
			v, e := r.Do(ctx, r.B().Get().Key(key).Build()).ToString()
			require.NoError(t, e)
			value = v
		case "hash":
			v, e := r.Do(ctx, r.B().Hgetall().Key(key).Build()).AsStrMap()
			require.NoError(t, e)
			value = v
		case "list":
			v, e := r.Do(ctx, r.B().Lrange().Key(key).Start(0).Stop(-1).Build()).AsStrSlice()
			require.NoError(t, e)
			value = v
		case "set":
			v, e := r.Do(ctx, r.B().Smembers().Key(key).Build()).AsStrSlice()
			require.NoError(t, e)
			slices.Sort(v)
			value = v
		case "zset":
			v, e := r.Do(ctx, r.B().Zrange().Key(key).Min("0").Max("-1").Withscores().Build()).AsZScores()
			require.NoError(t, e)
			value = v
		default:
			t.Fatalf("unexpected type %s for %s", typ, key)
		}
		out[key] = responseStoredValue{typ, value, ttl}
	}
	return out
}

func requireResponseSnapshot(t *testing.T, expected, actual responseSnapshot) {
	t.Helper()
	t.Logf("raw snapshot=%#v", actual)
	adjusted := responseSnapshot{}
	for key, value := range actual {
		if strings.Contains(key, ":key:run-key") {
			require.Greater(t, value.PTTL, int64(0))
			require.LessOrEqual(t, value.PTTL, expected[key].PTTL)
			require.InDelta(t, expected[key].PTTL, value.PTTL, 5000)
			value.PTTL = expected[key].PTTL
		}
		adjusted[key] = value
	}
	require.Equal(t, expected, adjusted)
}

type responsePersistenceFixture struct {
	Tracing struct {
		TraceID    string `json:"traceID"`
		RootParent string `json:"rootParent"`
	} `json:"tracing"`
	ObservationSequence []string                   `json:"observationSequence"`
	InitialMetadata     map[string]string          `json:"initialMetadata"`
	InitialEvents       string                     `json:"initialEvents"`
	SaveKeys            []string                   `json:"saveKeys"`
	QueueSnapshots      map[string]json.RawMessage `json:"queueSnapshots"`
	Queue               struct {
		FallbackJobID string            `json:"fallbackJobID"`
		Item          json.RawMessage   `json:"item"`
		HashID        string            `json:"hashID"`
		HashAnswers   map[string]string `json:"hashAnswers"`
	} `json:"queue"`
}

func loadResponsePersistenceFixture(t *testing.T) responsePersistenceFixture {
	t.Helper()
	b, err := os.ReadFile("../state/redis_state/testdata/compatibility/native-step-effects-v1.json")
	require.NoError(t, err)
	var f responsePersistenceFixture
	require.NoError(t, json.Unmarshal(b, &f))
	return f
}

func expectedResponseQueueSnapshot(t *testing.T, f responsePersistenceFixture, prefix string, keys bool, item string, job string) responseSnapshot {
	t.Helper()
	mode := "legacy"
	if keys {
		mode = "key"
	}
	var raw map[string]struct {
		Type  string
		Value json.RawMessage
		PTTL  int64
	}
	require.NoError(t, json.Unmarshal(f.QueueSnapshots[mode], &raw))
	out := responseSnapshot{}
	for key, v := range raw {
		var value any
		switch v.Type {
		case "hash":
			var hash map[string]string
			require.NoError(t, json.Unmarshal(v.Value, &hash))
			replaced := map[string]string{}
			for field, entry := range hash {
				if field == "JOB" {
					field = job
				}
				if entry == "ITEM" {
					entry = item
				}
				replaced[field] = entry
			}
			value = replaced
		case "zset":
			var zs []rueidis.ZScore
			require.NoError(t, json.Unmarshal(v.Value, &zs))
			for j := range zs {
				if zs[j].Member == "JOB" {
					zs[j].Member = job
				}
			}
			value = zs
		default:
			t.Fatalf("unsupported expected queue value type %s", v.Type)
		}
		out[strings.ReplaceAll(key, "QUEUE", prefix)] = responseStoredValue{v.Type, value, v.PTTL}
	}
	return out
}

// Merge independently declared initial-edge and discovery snapshots. This only
// unions fixture entries; it does not compute queue keys, scores or recipes.
func mergeResponseSnapshots(t *testing.T, a, b responseSnapshot) responseSnapshot {
	t.Helper()
	out := maps.Clone(a)
	for key, value := range b {
		if previous, ok := out[key]; ok {
			require.Equal(t, previous.Type, value.Type)
			switch value.Type {
			case "hash":
				hash := maps.Clone(previous.Value.(map[string]string))
				for k, v := range value.Value.(map[string]string) {
					if old, exists := hash[k]; exists {
						require.Equal(t, old, v)
					}
					hash[k] = v
				}
				value.Value = hash
			case "zset":
				members := map[string]float64{}
				for _, entry := range previous.Value.([]rueidis.ZScore) {
					members[entry.Member] = entry.Score
				}
				for _, entry := range value.Value.([]rueidis.ZScore) {
					if old, exists := members[entry.Member]; exists {
						require.Equal(t, old, entry.Score)
					}
					members[entry.Member] = entry.Score
				}
				entries := []rueidis.ZScore{}
				for m, score := range members {
					entries = append(entries, rueidis.ZScore{Member: m, Score: score})
				}
				slices.SortFunc(entries, func(a, b rueidis.ZScore) int {
					if a.Score < b.Score {
						return -1
					}
					if a.Score > b.Score {
						return 1
					}
					return strings.Compare(a.Member, b.Member)
				})
				value.Value = entries
			default:
				require.Equal(t, previous, value)
			}
		}
		out[key] = value
	}
	return out
}
