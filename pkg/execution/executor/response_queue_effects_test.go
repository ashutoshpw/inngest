package executor

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/inngest/inngest/pkg/enums"
	"github.com/inngest/inngest/pkg/execution"
	"github.com/inngest/inngest/pkg/execution/queue"
	"github.com/inngest/inngest/pkg/execution/state"
	sv2 "github.com/inngest/inngest/pkg/execution/state/v2"
	"github.com/inngest/inngest/pkg/tracing/metadata"
	"github.com/stretchr/testify/require"
)

type checkpointScheduledSnapshot struct {
	metadata sv2.Metadata
	item     queue.Item
	name     string
	groupID  string
}

type checkpointSnapshotListener struct {
	execution.NoopLifecyceListener
	release  <-chan struct{}
	observed chan checkpointScheduledSnapshot
}

func (l *checkpointSnapshotListener) OnStepScheduled(ctx context.Context, md sv2.Metadata, item queue.Item, name *string) {
	<-l.release
	l.observed <- checkpointScheduledSnapshot{metadata: md, item: item, name: *name, groupID: state.GroupIDFromContext(ctx)}
}

func TestHandleGeneratorSnapshotsScheduledArguments(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e, i, _, _, _ := newStagedBatch(t)
		max := 10
		i.item.MaxAttempts = &max
		i.md.Metrics.MetadataSize = 7
		i.md.Metrics.MetadataSizeLoaded = 7
		e.tracerProvider = newRecordingTracerProvider()
		release := make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		defer unblock()
		observed := make(chan checkpointScheduledSnapshot, 2)
		e.lifecycles = []execution.LifecycleListener{&checkpointSnapshotListener{release: release, observed: observed}}

		// Checkpoint feeds sequential opcodes through the same RunContext. Keep
		// deliveries outstanding until both calls return; neither may wait for a
		// listener, and the later span emits real metadata into that shared run.
		first := state.GeneratorOpcode{Op: enums.OpcodeStepError, ID: "first", Name: "first", Error: &state.UserError{Name: "E", Message: "x"}}
		require.Same(t, ErrHandledStepError, e.HandleGenerator(state.WithGroupID(context.Background(), "first-group"), i, first))
		second := state.GeneratorOpcode{
			Op: enums.OpcodeStepError, ID: "second", Name: "second", Error: &state.UserError{Name: "E", Message: "x"},
			Metadata: []metadata.ScopedUpdate{stepMetadataUpdate("userland.custom", metadata.Values{"k": json.RawMessage(`"v"`)})},
		}
		require.Same(t, ErrHandledStepError, e.HandleGenerator(state.WithGroupID(context.Background(), "second-group"), i, second))
		require.Equal(t, 11, i.md.Metrics.MetadataSize, "second handler emitted four bytes of metadata")
		unblock()
		synctest.Wait()
		require.Len(t, observed, 2)
		got := make(map[string]checkpointScheduledSnapshot, 2)
		for range 2 {
			snapshot := <-observed
			got[snapshot.name] = snapshot
		}
		require.Len(t, got, 2)
		require.Equal(t, 7, got["first"].metadata.Metrics.MetadataSize, "first callback retains metadata at its own launch")
		require.Equal(t, 11, got["second"].metadata.Metrics.MetadataSize)
		require.Equal(t, 2, got["first"].item.Attempt)
		require.Equal(t, 3, got["second"].item.Attempt)
		require.Equal(t, "first-group", got["first"].groupID)
		require.Equal(t, "second-group", got["second"].groupID)
	})
}
