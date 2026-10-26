package driver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/gowebpki/jcs"
	"github.com/inngest/inngest/pkg/enums"
	"github.com/inngest/inngest/pkg/execution/queue"
	sv2 "github.com/inngest/inngest/pkg/execution/state/v2"
	"github.com/inngest/inngest/pkg/inngest"
	"github.com/oklog/ulid/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const compatibilityDir = "testdata/sdk-request-v1"

type compatibilityFixture struct {
	Name  string `json:"name"`
	Input struct {
		Events         []string          `json:"events"`
		Steps          map[string]string `json:"steps"`
		Stack          []string          `json:"stack"`
		StateSize      int               `json:"state_size"`
		RequestVersion int               `json:"request_version"`
		StackIndex     int               `json:"stack_index"`
		Attempt        int               `json:"attempt"`
		ForceStepPlan  bool              `json:"force_step_plan"`
		RequestID      string            `json:"request_id"`
		JobID          string            `json:"job_id"`
		GenerationID   int               `json:"generation_id"`
		Defers         map[string]struct {
			Status   string          `json:"schedule_status"`
			FnSlug   string          `json:"fn_slug"`
			HashedID string          `json:"hashed_id"`
			Meta     json.RawMessage `json:"meta"`
		} `json:"defers"`
		DefersError string `json:"defers_error"`
		StepsError  string `json:"steps_error"`
	} `json:"input"`
	Recipes map[string]struct {
		ASCII string `json:"ascii"`
		Count int    `json:"count"`
	} `json:"recipes"`
	ExpectedFile  string `json:"expected_file"`
	ExpectedError string `json:"expected_error"`
}

type compatibilityCorpus struct {
	Common struct {
		FunctionID   string `json:"function_id"`
		RunID        string `json:"run_id"`
		Env          string `json:"env"`
		StepID       string `json:"step_id"`
		MaxAttempts  int    `json:"max_attempts"`
		QueueItemRef string `json:"queue_item_ref"`
	} `json:"common"`
	Cases []compatibilityFixture `json:"cases"`
}

func loadCompatibilityCorpus(tb testing.TB) compatibilityCorpus {
	tb.Helper()
	raw, err := os.ReadFile(filepath.Join(compatibilityDir, "cases.json"))
	if err != nil {
		tb.Fatal(err)
	}
	var corpus compatibilityCorpus
	if err := json.Unmarshal(raw, &corpus); err != nil {
		tb.Fatal(err)
	}
	return corpus
}

func (f compatibilityFixture) expand(raw string) []byte {
	for token, recipe := range f.Recipes {
		raw = strings.ReplaceAll(raw, token, strings.Repeat(recipe.ASCII, recipe.Count))
	}
	return []byte(raw)
}

func (f compatibilityFixture) expected(tb testing.TB) []byte {
	tb.Helper()
	raw, err := os.ReadFile(filepath.Join(compatibilityDir, f.ExpectedFile))
	if err != nil {
		tb.Fatal(err)
	}
	return f.expand(string(raw))
}

// Only the real loader boundary is substituted. An unexpected loader method
// fails through the embedded nil interface instead of silently returning data.
type compatibilityLoader struct {
	sv2.StateLoader
	events      []json.RawMessage
	steps       map[string]json.RawMessage
	defers      map[string]sv2.DeferMeta
	defersError error
	stepsError  error
}

func (s *compatibilityLoader) LoadEvents(context.Context, sv2.ID) ([]json.RawMessage, error) {
	return s.events, nil
}

func (s *compatibilityLoader) LoadSteps(context.Context, sv2.ID) (map[string]json.RawMessage, error) {
	return s.steps, s.stepsError
}

func (s *compatibilityLoader) LoadDefersMeta(context.Context, sv2.ID) (map[string]sv2.DeferMeta, error) {
	return s.defers, s.defersError
}

func (c compatibilityCorpus) prepare(tb testing.TB, f compatibilityFixture) (context.Context, *compatibilityLoader, sv2.Metadata) {
	tb.Helper()
	sl := &compatibilityLoader{
		steps:  make(map[string]json.RawMessage, len(f.Input.Steps)),
		defers: make(map[string]sv2.DeferMeta, len(f.Input.Defers)),
	}
	for _, event := range f.Input.Events {
		sl.events = append(sl.events, f.expand(event))
	}
	for id, raw := range f.Input.Steps {
		sl.steps[id] = f.expand(raw)
	}
	for id, deferMeta := range f.Input.Defers {
		scheduleStatus, err := enums.DeferStatusString(deferMeta.Status)
		if err != nil {
			tb.Fatal(err)
		}
		sl.defers[id] = sv2.DeferMeta{
			ScheduleStatus: scheduleStatus,
			FnSlug:         deferMeta.FnSlug,
			HashedID:       deferMeta.HashedID,
			Meta:           deferMeta.Meta,
		}
	}
	if f.Input.DefersError != "" {
		code := codes.Unavailable
		if f.Input.DefersError == "unimplemented" {
			code = codes.Unimplemented
		}
		sl.defersError = status.Error(code, "fixture defers failure")
	}
	if f.Input.StepsError != "" {
		sl.stepsError = fmt.Errorf("%s", f.Input.StepsError)
	}
	md := sv2.Metadata{
		ID: sv2.ID{
			FunctionID: uuid.MustParse(c.Common.FunctionID),
			RunID:      ulid.MustParse(c.Common.RunID),
		},
		Config: sv2.Config{
			RequestVersion: f.Input.RequestVersion,
			ForceStepPlan:  f.Input.ForceStepPlan,
		},
		Stack:   f.Input.Stack,
		Metrics: sv2.RunMetrics{StateSize: f.Input.StateSize},
	}
	ctx := WithRequestIDs(context.Background(), f.Input.RequestID, f.Input.JobID)
	ctx = queue.WithGenerationID(ctx, f.Input.GenerationID)
	return ctx, sl, md
}

func (c compatibilityCorpus) marshal(ctx context.Context, sl *compatibilityLoader, md sv2.Metadata, f compatibilityFixture) ([]byte, error) {
	return MarshalV1(ctx, sl, md, inngest.Step{ID: c.Common.StepID}, f.Input.StackIndex,
		c.Common.Env, f.Input.Attempt, c.Common.MaxAttempts, c.Common.QueueItemRef)
}

func TestMarshalV1Compatibility(t *testing.T) {
	corpus := loadCompatibilityCorpus(t)
	for _, f := range corpus.Cases {
		t.Run(f.Name, func(t *testing.T) {
			ctx, sl, md := corpus.prepare(t, f)
			got, err := corpus.marshal(ctx, sl, md, f)
			if f.ExpectedError != "" {
				if err == nil || err.Error() != f.ExpectedError || got != nil {
					t.Fatalf("got %d bytes, error %v; want nil bytes, error %q", len(got), err, f.ExpectedError)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			want := f.expected(t)
			if !bytes.Equal(want, got) {
				// Keep boundary-case failures readable even for multi-MB wires.
				offset := 0
				for offset < len(want) && offset < len(got) && want[offset] == got[offset] {
					offset++
				}
				t.Fatalf("wire mismatch at byte %d: got %d bytes, want %d bytes", offset, len(got), len(want))
			}
		})
	}
}

func BenchmarkMarshalV1Compatibility(b *testing.B) {
	corpus := loadCompatibilityCorpus(b)
	for _, f := range corpus.benchmarkFixtures() {
		b.Run(f.Name, func(b *testing.B) {
			ctx, source, md := corpus.prepare(b, f)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// MarshalV1 consumes stack entries from LoadSteps' map. Recreate
				// the map each time; immutable raw payload bytes can be shared.
				// This includes unnecessary fixture reset overhead for
				// state_size_above, which never loads steps (see corpus README).
				sl := *source
				sl.steps = make(map[string]json.RawMessage, len(source.steps))
				for id, raw := range source.steps {
					sl.steps[id] = raw
				}
				if _, err := corpus.marshal(ctx, &sl, md, f); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkJCSCompatibility(b *testing.B) {
	corpus := loadCompatibilityCorpus(b)
	for _, f := range corpus.benchmarkFixtures() {
		b.Run(f.Name, func(b *testing.B) {
			// Measure JCS separately on pinned canonical SDK wire bytes. This
			// includes parsing and serialization, but no loader or fixture I/O.
			wire := f.expected(b)
			b.ReportAllocs()
			b.SetBytes(int64(len(wire)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := jcs.Transform(wire); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func (c compatibilityCorpus) benchmarkFixtures() []compatibilityFixture {
	var fixtures []compatibilityFixture
	for _, f := range c.Cases {
		switch f.Name {
		case "replay", "state_size_above", "body_size_above", "event_batch_fallback":
			fixtures = append(fixtures, f)
		}
	}
	return fixtures
}
