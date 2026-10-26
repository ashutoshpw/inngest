package redis_state

import (
	"context"
	_ "embed"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	osqueue "github.com/inngest/inngest/pkg/execution/queue"
	"github.com/jonboulle/clockwork"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
)

//go:embed testdata/compatibility/queue-item-v1.json
var queueItemCompatibilityFixture []byte

type queueItemCompatibilityCase struct {
	Name        string          `json:"name"`
	PayloadType string          `json:"payloadType"`
	EnqueueAtMS int64           `json:"enqueueAtMs"`
	Input       json.RawMessage `json:"input"`
	Persisted   json.RawMessage `json:"persisted"`
}

type queueItemPersistedShape struct {
	ID          string    `json:"id"`
	AtMS        int64     `json:"at"`
	WallTimeMS  int64     `json:"wt"`
	FunctionID  uuid.UUID `json:"wfID"`
	WorkspaceID uuid.UUID `json:"wsID"`
	Generation  int       `json:"genID"`
	EnqueuedAt  int64     `json:"eat"`
	Data        struct {
		GroupID             string          `json:"groupID"`
		WorkspaceID         uuid.UUID       `json:"wsID"`
		Kind                string          `json:"kind"`
		Identifier          json.RawMessage `json:"identifier"`
		Attempt             int             `json:"atts"`
		MaxAttempts         *int            `json:"maxAtts"`
		Payload             json.RawMessage `json:"payload"`
		Metadata            json.RawMessage `json:"metadata"`
		QueueName           *string         `json:"qn"`
		PriorityFactor      *int64          `json:"pf"`
		ParallelCoalesceKey *string         `json:"pck"`
	} `json:"data"`
}

func TestQueueItemPersistenceCompatibility(t *testing.T) {
	var fixture struct {
		FormatVersion int                          `json:"formatVersion"`
		ClockNowMS    int64                        `json:"clockNowMs"`
		Cases         []queueItemCompatibilityCase `json:"cases"`
	}
	require.NoError(t, json.Unmarshal(queueItemCompatibilityFixture, &fixture))
	require.Equal(t, 1, fixture.FormatVersion)
	require.NotEmpty(t, fixture.Cases)

	r, rc := initRedis(t)
	defer rc.Close()

	clock := clockwork.NewFakeClockAt(time.UnixMilli(fixture.ClockNowMS))
	_, shard := newQueue(t, rc,
		osqueue.WithClock(clock),
		osqueue.WithAllowKeyQueues(func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID) bool {
			return false
		}),
	)
	ctx := t.Context()

	for _, tc := range fixture.Cases {
		t.Run(tc.Name, func(t *testing.T) {
			var input osqueue.QueueItem
			require.NoError(t, json.Unmarshal(tc.Input, &input))

			enqueued, err := shard.EnqueueItem(ctx, input, time.UnixMilli(tc.EnqueueAtMS), osqueue.EnqueueOpts{
				PassthroughJobId: true,
			})
			require.NoError(t, err)
			require.Equal(t, input.ID, enqueued.ID)

			storedValue := r.HGet(shard.Client().kg.QueueItem(), enqueued.ID)
			require.NotEmpty(t, storedValue)
			require.JSONEq(t, string(tc.Persisted), storedValue)

			var expected queueItemPersistedShape
			require.NoError(t, json.Unmarshal(tc.Persisted, &expected))

			loaded, err := shard.LoadQueueItem(ctx, enqueued.ID)
			require.NoError(t, err)
			require.Equal(t, expected.ID, loaded.ID)
			require.Equal(t, expected.AtMS, loaded.AtMS)
			require.Equal(t, expected.WallTimeMS, loaded.WallTimeMS)
			require.Equal(t, expected.FunctionID, loaded.FunctionID)
			require.Equal(t, expected.WorkspaceID, loaded.WorkspaceID)
			require.Equal(t, expected.Generation, loaded.GenerationID)
			require.Equal(t, expected.EnqueuedAt, loaded.EnqueuedAt)
			require.Equal(t, expected.Data.GroupID, loaded.Data.GroupID)
			require.Equal(t, expected.Data.WorkspaceID, loaded.Data.WorkspaceID)
			require.Equal(t, expected.Data.Kind, loaded.Data.Kind)
			require.Equal(t, expected.Data.Attempt, loaded.Data.Attempt)
			require.Equal(t, expected.Data.MaxAttempts, loaded.Data.MaxAttempts)
			require.Equal(t, expected.Data.QueueName, loaded.Data.QueueName)
			require.Equal(t, expected.Data.PriorityFactor, loaded.Data.PriorityFactor)
			require.Equal(t, expected.Data.ParallelCoalesceKey, loaded.Data.ParallelCoalesceKey)
			require.NotNil(t, loaded.Data.JobID)
			require.Equal(t, loaded.ID, *loaded.Data.JobID)

			identifierJSON, err := json.Marshal(loaded.Data.Identifier)
			require.NoError(t, err)
			require.JSONEq(t, string(expected.Data.Identifier), string(identifierJSON))

			if len(expected.Data.Metadata) == 0 {
				require.Nil(t, loaded.Data.Metadata)
			} else {
				metadataJSON, err := json.Marshal(loaded.Data.Metadata)
				require.NoError(t, err)
				require.JSONEq(t, string(expected.Data.Metadata), string(metadataJSON))
			}

			payloadJSON, err := json.Marshal(loaded.Data.Payload)
			require.NoError(t, err)
			require.JSONEq(t, string(expected.Data.Payload), string(payloadJSON))
			switch tc.PayloadType {
			case "edge":
				_, ok := loaded.Data.Payload.(osqueue.PayloadEdge)
				require.True(t, ok, "edge payload must be rehydrated to PayloadEdge")
			case "function-pause":
				_, ok := loaded.Data.Payload.(osqueue.PayloadPauseFunction)
				require.True(t, ok, "function pause payload must be rehydrated to PayloadPauseFunction")
			default:
				t.Fatalf("fixture has unsupported payload type %q", tc.PayloadType)
			}
		})
	}
}

var queueItemPersistenceBenchmarkSink *osqueue.QueueItem

func BenchmarkQueueItemPersistenceCompatibility(b *testing.B) {
	var fixture struct {
		FormatVersion int                          `json:"formatVersion"`
		ClockNowMS    int64                        `json:"clockNowMs"`
		Cases         []queueItemCompatibilityCase `json:"cases"`
	}
	require.NoError(b, json.Unmarshal(queueItemCompatibilityFixture, &fixture))
	require.Equal(b, 1, fixture.FormatVersion)

	var input osqueue.QueueItem
	require.NoError(b, json.Unmarshal(fixture.Cases[0].Input, &input))

	b.Run("json_marshal_typed_unmarshal", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(fixture.Cases[0].Input)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			serialized, err := json.Marshal(input)
			if err != nil {
				b.Fatal(err)
			}
			var loaded osqueue.QueueItem
			if err := json.Unmarshal(serialized, &loaded); err != nil {
				b.Fatal(err)
			}
			queueItemPersistenceBenchmarkSink = &loaded
		}
	})

	b.Run("miniredis_enqueue_load", func(b *testing.B) {
		redis := miniredis.RunT(b)
		rc, err := rueidis.NewClient(rueidis.ClientOption{
			InitAddress:  []string{redis.Addr()},
			DisableCache: true,
		})
		require.NoError(b, err)
		defer rc.Close()
		clock := clockwork.NewFakeClockAt(time.UnixMilli(fixture.ClockNowMS))
		_, shard := newQueue(b, rc,
			osqueue.WithClock(clock),
			osqueue.WithAllowKeyQueues(func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID) bool {
				return false
			}),
		)
		ctx := context.Background()
		at := time.UnixMilli(fixture.ClockNowMS)
		b.ReportAllocs()
		b.SetBytes(int64(len(fixture.Cases[0].Persisted)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			item := input
			item.ID = "queue-compat-bench-" + strconv.Itoa(i)
			enqueued, err := shard.EnqueueItem(ctx, item, at, osqueue.EnqueueOpts{PassthroughJobId: true})
			if err != nil {
				b.Fatal(err)
			}
			loaded, err := shard.LoadQueueItem(ctx, enqueued.ID)
			if err != nil {
				b.Fatal(err)
			}
			queueItemPersistenceBenchmarkSink = loaded
		}
	})
}
