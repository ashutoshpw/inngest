package httpdriver

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/inngest/inngest/pkg/enums"
	"github.com/stretchr/testify/require"
)

const generatorParserSourceCommit = "3624c37d5285805d2a95efd0715e8913d5f81ae8"

type generatorParserFixture struct {
	SchemaVersion int                       `json:"schemaVersion"`
	Contract      string                    `json:"contract"`
	Source        generatorParserProvenance `json:"source"`
	Cases         []generatorParserCase     `json:"cases"`
}

type generatorParserProvenance struct {
	Repository string   `json:"repository"`
	Commit     string   `json:"commit"`
	Files      []string `json:"files"`
}

type generatorParserCase struct {
	Name     string                  `json:"name"`
	Input    string                  `json:"input"`
	NoRetry  bool                    `json:"noRetry"`
	Expected generatorParserExpected `json:"expected"`
}

type generatorParserExpected struct {
	Ops           []generatorParserOpcode `json:"ops"`
	Error         string                  `json:"error"`
	ErrorContains string                  `json:"errorContains"`
}

type generatorParserOpcode struct {
	Op    enums.Opcode              `json:"op"`
	ID    string                    `json:"id"`
	Data  json.RawMessage           `json:"data"`
	Error *generatorParserUserError `json:"error"`
}

type generatorParserUserError struct {
	Name    string `json:"name"`
	Message string `json:"message"`
	NoRetry bool   `json:"noRetry"`
}

func loadGeneratorParserFixture(t *testing.T) generatorParserFixture {
	t.Helper()

	data, err := os.ReadFile("testdata/compatibility/generator-parser-v1.json")
	require.NoError(t, err)

	var fixture generatorParserFixture
	require.NoError(t, json.Unmarshal(data, &fixture))
	require.Equal(t, 1, fixture.SchemaVersion)
	require.Equal(t, "generator-parser-v1", fixture.Contract)
	require.Equal(t, generatorParserSourceCommit, fixture.Source.Commit)
	return fixture
}

func TestGeneratorParserCompatibilityFixture(t *testing.T) {
	fixture := loadGeneratorParserFixture(t)

	for _, tc := range fixture.Cases {
		t.Run(tc.Name, func(t *testing.T) {
			ops, err := ParseGenerator(context.Background(), []byte(tc.Input), tc.NoRetry)
			if tc.Expected.Error != "" || tc.Expected.ErrorContains != "" {
				require.Error(t, err)
				if tc.Expected.Error != "" {
					require.EqualError(t, err, tc.Expected.Error)
				}
				if tc.Expected.ErrorContains != "" {
					require.ErrorContains(t, err, tc.Expected.ErrorContains)
				}
				return
			}

			require.NoError(t, err)
			require.Len(t, ops, len(tc.Expected.Ops))
			for i, expected := range tc.Expected.Ops {
				actual := ops[i]
				require.Equal(t, expected.Op, actual.Op, "opcode %d", i)
				require.Equal(t, expected.ID, actual.ID, "opcode %d", i)
				if expected.Data == nil {
					require.Nil(t, actual.Data, "opcode %d data must be absent", i)
				} else {
					require.JSONEq(t, string(expected.Data), string(actual.Data), "opcode %d data", i)
				}
				if expected.Error == nil {
					require.Nil(t, actual.Error, "opcode %d error must be absent", i)
					continue
				}
				require.NotNil(t, actual.Error, "opcode %d error", i)
				require.Equal(t, expected.Error.Name, actual.Error.Name)
				require.Equal(t, expected.Error.Message, actual.Error.Message)
				require.Equal(t, expected.Error.NoRetry, actual.Error.NoRetry)
			}
		})
	}
}
