package redis_state

import (
	_ "embed"
	"encoding/json"
	"testing"
	"time"

	"github.com/inngest/inngest/pkg/enums"
	"github.com/inngest/inngest/pkg/execution/state"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
)

//go:embed testdata/compatibility/run-state-v1.json
var runStateCompatibilityFixture []byte

type runStateCompatibilityInput struct {
	Identifier     state.Identifier     `json:"identifier"`
	Events         []map[string]any     `json:"events"`
	Debugger       bool                 `json:"debugger"`
	RunType        *string              `json:"runType"`
	Context        map[string]any       `json:"context"`
	SpanID         string               `json:"spanID"`
	RequestVersion int                  `json:"requestVersion"`
	ScheduledAtMS  int64                `json:"scheduledAtMs"`
	Steps          []state.MemoizedStep `json:"steps"`
	StepInputs     []state.MemoizedStep `json:"stepInputs"`
}

func TestRunStatePersistenceCompatibility(t *testing.T) {
	var fixture struct {
		FormatVersion int                        `json:"formatVersion"`
		Input         runStateCompatibilityInput `json:"input"`
		ExpectedRedis struct {
			Events       json.RawMessage            `json:"events"`
			Metadata     map[string]json.RawMessage `json:"metadata"`
			Actions      map[string]json.RawMessage `json:"actions"`
			Stack        []string                   `json:"stack"`
			ActionInputs map[string]json.RawMessage `json:"actionInputs"`
		} `json:"expectedRedis"`
		ExpectedLoaded struct {
			Actions map[string]json.RawMessage `json:"actions"`
		} `json:"expectedLoaded"`
	}
	require.NoError(t, json.Unmarshal(runStateCompatibilityFixture, &fixture))
	require.Equal(t, 1, fixture.FormatVersion)

	r, rc := initRedis(t)
	defer rc.Close()
	ctx := t.Context()

	unsharded := NewUnshardedClient(rc, StateDefaultKey, QueueDefaultKey)
	sharded := NewShardedClient(ShardedClientOpts{
		UnshardedClient:        unsharded,
		FunctionRunStateClient: rc,
		BatchClient:            rc,
		StateDefaultKey:        StateDefaultKey,
		QueueDefaultKey:        QueueDefaultKey,
		FnRunIsSharded:         AlwaysShardOnRun,
	})
	manager := shardedMgr{s: sharded}
	requestVersion := fixture.Input.RequestVersion
	input := state.Input{
		Identifier:     fixture.Input.Identifier,
		EventBatchData: fixture.Input.Events,
		Debugger:       fixture.Input.Debugger,
		RunType:        fixture.Input.RunType,
		Context:        fixture.Input.Context,
		SpanID:         fixture.Input.SpanID,
		RequestVersion: &requestVersion,
		ScheduledAt:    time.UnixMilli(fixture.Input.ScheduledAtMS),
		Steps:          fixture.Input.Steps,
		StepInputs:     fixture.Input.StepInputs,
	}
	_, err := manager.New(ctx, input)
	require.NoError(t, err)

	fnRunState := sharded.FunctionRunState()
	_, isSharded := fnRunState.Client(ctx, input.Identifier.AccountID, input.Identifier.RunID)
	require.True(t, isSharded)
	keys := fnRunState.KeyGenerator()
	eventsKey := keys.Events(ctx, isSharded, input.Identifier.WorkflowID, input.Identifier.RunID)
	metadataKey := keys.RunMetadata(ctx, isSharded, input.Identifier.RunID)
	actionsKey := keys.Actions(ctx, isSharded, input.Identifier.WorkflowID, input.Identifier.RunID)
	stackKey := keys.Stack(ctx, isSharded, input.Identifier.RunID)
	stepInputsKey := keys.ActionInputs(ctx, isSharded, input.Identifier)

	events, err := r.Get(eventsKey)
	require.NoError(t, err)
	require.JSONEq(t, string(fixture.ExpectedRedis.Events), events)

	metadata, err := redisHash(t, rc, metadataKey)
	require.NoError(t, err)
	for field, expected := range fixture.ExpectedRedis.Metadata {
		actual, ok := metadata[field]
		require.Truef(t, ok, "run metadata missing %q", field)
		switch field {
		case "id", "ctx":
			require.JSONEq(t, string(expected), actual)
		default:
			var expectedString string
			require.NoError(t, json.Unmarshal(expected, &expectedString))
			require.Equal(t, expectedString, actual)
		}
	}

	assertJSONHash(t, rc, actionsKey, fixture.ExpectedRedis.Actions)
	assertJSONHash(t, rc, stepInputsKey, fixture.ExpectedRedis.ActionInputs)
	stack, err := rc.Do(ctx, rc.B().Lrange().Key(stackKey).Start(0).Stop(-1).Build()).AsStrSlice()
	require.NoError(t, err)
	require.Equal(t, fixture.ExpectedRedis.Stack, stack)

	loaded, err := manager.Load(ctx, input.Identifier.AccountID, input.Identifier.RunID)
	require.NoError(t, err)
	require.Equal(t, input.Identifier, loaded.Identifier())
	require.JSONEq(t, string(fixture.ExpectedRedis.Events), string(mustMarshalJSON(t, loaded.Events())))
	require.Equal(t, fixture.ExpectedRedis.Stack, loaded.Stack())

	metadataValue := loaded.Metadata()
	require.Equal(t, enums.RunStatusScheduled, metadataValue.Status)
	require.Equal(t, 1, metadataValue.Version)
	require.Equal(t, fixture.Input.RequestVersion, metadataValue.RequestVersion)
	require.True(t, metadataValue.Debugger)
	require.NotNil(t, metadataValue.RunType)
	require.Equal(t, *fixture.Input.RunType, *metadataValue.RunType)
	require.Equal(t, fixture.Input.SpanID, metadataValue.SpanID)
	require.JSONEq(t, string(fixture.ExpectedRedis.Metadata["ctx"]), string(mustMarshalJSON(t, metadataValue.Context)))

	loadedActions := loaded.Actions()
	require.Len(t, loadedActions, len(fixture.ExpectedLoaded.Actions))
	for stepID, expected := range fixture.ExpectedLoaded.Actions {
		actual, ok := loadedActions[stepID]
		require.Truef(t, ok, "loaded state missing action %q", stepID)
		var actualJSON []byte
		if raw, ok := actual.([]byte); ok {
			actualJSON = raw
		} else {
			actualJSON = mustMarshalJSON(t, actual)
		}
		require.JSONEq(t, string(expected), string(actualJSON))
	}
}

func redisHash(t *testing.T, rc rueidis.Client, key string) (map[string]string, error) {
	t.Helper()
	return rc.Do(t.Context(), rc.B().Hgetall().Key(key).Build()).AsStrMap()
}

func assertJSONHash(t *testing.T, rc rueidis.Client, key string, expected map[string]json.RawMessage) {
	t.Helper()
	actual, err := redisHash(t, rc, key)
	require.NoError(t, err)
	require.Len(t, actual, len(expected))
	for field, want := range expected {
		got, ok := actual[field]
		require.Truef(t, ok, "Redis hash %q missing %q", key, field)
		require.JSONEq(t, string(want), got)
	}
}

func mustMarshalJSON(t *testing.T, value any) []byte {
	t.Helper()
	byt, err := json.Marshal(value)
	require.NoError(t, err)
	return byt
}
