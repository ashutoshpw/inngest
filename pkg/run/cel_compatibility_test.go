package run

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	sq "github.com/doug-martin/goqu/v9"
	_ "github.com/doug-martin/goqu/v9/dialect/postgres"
	_ "github.com/doug-martin/goqu/v9/dialect/sqlite3"
	"github.com/inngest/inngest/pkg/event"
	"github.com/stretchr/testify/require"
)

type celSQLCompatibilityFixture struct {
	Format     string                `json:"format"`
	Version    int                   `json:"version"`
	Provenance string                `json:"provenance"`
	Cases      []celSQLCompatibility `json:"cases"`
}

type celSQLCompatibility struct {
	ID            string                   `json:"id"`
	Expression    string                   `json:"expression"`
	Event         json.RawMessage          `json:"event"`
	ExpectedMatch bool                     `json:"expected_match"`
	SQL           map[string]celSQLDialect `json:"sql"`
}

type celSQLDialect struct {
	Supported bool            `json:"supported"`
	Filter    string          `json:"filter"`
	Args      json.RawMessage `json:"args"`
}

func loadCELSQLCompatibilityFixture(t testing.TB) celSQLCompatibilityFixture {
	t.Helper()

	contents, err := os.ReadFile("testdata/compatibility/cel-sql-v1.json")
	require.NoError(t, err)

	var fixture celSQLCompatibilityFixture
	require.NoError(t, json.Unmarshal(contents, &fixture))
	require.Equal(t, "inngest.cel-sql.compatibility", fixture.Format)
	require.Equal(t, 1, fixture.Version)
	require.NotEmpty(t, fixture.Provenance)
	require.NotEmpty(t, fixture.Cases)
	return fixture
}

func TestCELSQLCompatibilityFixture(t *testing.T) {
	ctx := context.Background()
	fixture := loadCELSQLCompatibilityFixture(t)
	dialects := []struct {
		name      string
		goquName  string
		converter ExprSQLConverter
	}{
		{name: "sqlite", goquName: "sqlite3", converter: SpanEventSQLiteConverter},
		{name: "postgres", goquName: "postgres", converter: SpanEventPostgresConverter},
	}

	for _, testCase := range fixture.Cases {
		t.Run(testCase.ID, func(t *testing.T) {
			var evt event.Event
			require.NoError(t, json.Unmarshal(testCase.Event, &evt))

			matcher, err := NewExpressionHandler(ctx, WithExpressionHandlerExpressions([]string{testCase.Expression}))
			require.NoError(t, err)
			matched, err := matcher.MatchEventExpressions(ctx, evt)
			require.NoError(t, err)
			require.Equal(t, testCase.ExpectedMatch, matched)

			for _, dialect := range dialects {
				dialect := dialect
				t.Run(dialect.name, func(t *testing.T) {
					expected, ok := testCase.SQL[dialect.name]
					require.True(t, ok, "fixture must define each SQL dialect")

					handler, err := NewExpressionHandler(
						ctx,
						WithExpressionHandlerExpressions([]string{testCase.Expression}),
						WithExpressionSQLConverter(dialect.converter),
					)
					require.NoError(t, err)

					filters, err := handler.ToSQLFilters(ctx)
					if !expected.Supported {
						require.Error(t, err)
						return
					}
					require.NoError(t, err)

					query, args, err := sq.Dialect(dialect.goquName).
						From("events").
						Select(sq.L("1")).
						Where(filters...).
						Prepared(true).
						ToSQL()
					require.NoError(t, err)

					whereIndex := strings.Index(query, " WHERE ")
					require.NotEqual(t, -1, whereIndex, "prepared query must contain converted filters")
					require.Equal(t, expected.Filter, query[whereIndex+1:])

					actualArgs, err := json.Marshal(args)
					require.NoError(t, err)
					require.JSONEq(t, string(expected.Args), string(actualArgs))
				})
			}
		})
	}
}
