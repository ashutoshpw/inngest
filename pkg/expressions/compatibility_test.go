package expressions

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

type celCompatibilityFixture struct {
	Format     string                 `json:"format"`
	Version    int                    `json:"version"`
	Provenance string                 `json:"provenance"`
	Cases      []celCompatibilityCase `json:"cases"`
}

type celCompatibilityCase struct {
	ID         string         `json:"id"`
	Expression string         `json:"expression"`
	Input      map[string]any `json:"input"`
	Expected   struct {
		Result     *bool  `json:"result"`
		ErrorStage string `json:"error_stage"`
	} `json:"expected"`
}

func loadCELCompatibilityFixture(t testing.TB) celCompatibilityFixture {
	t.Helper()

	contents, err := os.ReadFile("testdata/compatibility/cel-v1.json")
	require.NoError(t, err)

	var fixture celCompatibilityFixture
	require.NoError(t, json.Unmarshal(contents, &fixture))
	require.Equal(t, "inngest.cel.compatibility", fixture.Format)
	require.Equal(t, 1, fixture.Version)
	require.NotEmpty(t, fixture.Provenance)
	require.NotEmpty(t, fixture.Cases)
	return fixture
}

func TestCELCompatibilityFixture(t *testing.T) {
	ctx := context.Background()
	fixture := loadCELCompatibilityFixture(t)

	for _, testCase := range fixture.Cases {
		t.Run(testCase.ID, func(t *testing.T) {
			evaluator, err := NewBooleanEvaluator(ctx, testCase.Expression)
			switch testCase.Expected.ErrorStage {
			case "compile":
				require.Error(t, err)
				return
			case "evaluation":
				require.NoError(t, err)
				_, err = evaluator.Evaluate(ctx, NewData(testCase.Input))
				require.Error(t, err)
				return
			case "":
				require.NoError(t, err)
				require.NotNil(t, testCase.Expected.Result)
			default:
				t.Fatalf("unsupported expected error stage %q", testCase.Expected.ErrorStage)
			}

			actual, err := evaluator.Evaluate(ctx, NewData(testCase.Input))
			require.NoError(t, err)
			require.Equal(t, *testCase.Expected.Result, actual)
		})
	}
}

var celCompatibilitySourceRun atomic.Uint64

func BenchmarkCELCompatibilityCorpus(b *testing.B) {
	fixture := loadCELCompatibilityFixture(b)
	var workload *celCompatibilityCase
	for i := range fixture.Cases {
		if fixture.Cases[i].ID == "mixed-numeric-custom-nested-workload" {
			workload = &fixture.Cases[i]
			break
		}
	}
	if workload == nil {
		b.Fatal("compatibility fixture is missing the benchmark workload")
	}

	ctx := context.Background()
	data := NewData(workload.Input)

	b.Run("evaluator_program_construction_cached_parse", func(b *testing.B) {
		// Vary numeric literals so NewBooleanEvaluator misses the evaluator
		// cache. Source uniquification is included, but the caching compiler
		// lifts those literals and reuses its normalized CEL AST.
		sourceRunID := celCompatibilitySourceRun.Add(1)
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			source := fmt.Sprintf("(%s) && %d == %d", workload.Expression, sourceRunID, i)
			if _, err := NewBooleanEvaluator(ctx, source); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("cached_program_evaluate", func(b *testing.B) {
		evaluator, err := NewBooleanEvaluator(ctx, workload.Expression)
		if err != nil {
			b.Fatal(err)
		}

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			matched, err := evaluator.Evaluate(ctx, data)
			if err != nil {
				b.Fatal(err)
			}
			if !matched {
				b.Fatal("benchmark workload did not match")
			}
		}
	})

	b.Run("cache_lookup_and_evaluate", func(b *testing.B) {
		if _, err := NewBooleanEvaluator(ctx, workload.Expression); err != nil {
			b.Fatal(err)
		}

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			evaluator, err := NewBooleanEvaluator(ctx, workload.Expression)
			if err != nil {
				b.Fatal(err)
			}
			matched, err := evaluator.Evaluate(ctx, data)
			if err != nil {
				b.Fatal(err)
			}
			if !matched {
				b.Fatal("benchmark workload did not match")
			}
		}
	})
}
