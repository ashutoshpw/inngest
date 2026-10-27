//go:build linux && response_continuation_spike && cgo

package responsecontinuationspike

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// This is the Cgo ownership/calling-convention boundary. The Rust ABI suite
// separately owns key matching, bounds and post-consumption native poisoning.
func TestFFISessionCopiesOwnsReleasesAndPoisons(t *testing.T) {
	input := []byte(`{"generation":900000,"ops":[{"tag":2,"id":"retained","name":"original","display_name":null,"data":[110,117,108,108],"options":[],"error":null,"parallel_mode":"None","has_ai":false}],"config":{"request_version":2,"force_step_plan":false,"started_at":0,"has_ai":false},"response_version":2,"response_no_retry":false,"response_user_no_retry":false,"retry_at":null,"run_id":"run","idempotency_key":"key","edge":{"incoming":"fn","outgoing":"prev","incoming_generator_step":"","incoming_generator_step_name":""},"attempt":0,"max_attempts":3,"context_mode":"None","context_group":"incoming","context_coalesce_key":null,"queue":{"workspace_id":"env","identifier":[],"priority_factor":null,"custom_concurrency_keys":[],"step_semaphores":[]},"handled_step_error":1,"function_overflow":2}`)
	original := bytes.Clone(input)
	session, first, err := Start(input)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, session.Close()) })
	clear(input) // Rust must own input after the call returns.
	require.Len(t, first.Ready, 1)
	savedKey := first.Ready[0].Key
	active, err := session.Resume(savedKey, map[string]any{"Clock": 0})
	require.NoError(t, err)
	require.Len(t, active.Ready, 1)
	require.Contains(t, string(active.Ready[0].Effect), "ActivateBranch")
	prepared, err := session.Resume(active.Ready[0].Key, "Done")
	require.NoError(t, err)
	require.Contains(t, string(prepared.Ready[0].Effect), `"name":"original"`)
	require.Equal(t, `{"Clock":"Handling"}`, string(first.Ready[0].Effect), "previous outputs are copied before Rust releases their allocation")
	// A stale result poisons this Go session; it cannot silently retry the key.
	stale := prepared.Ready[0].Key
	stale.Generation++
	_, err = session.Resume(stale, map[string]any{"Prepared": map[string]any{"Ok": []int{}}})
	require.ErrorContains(t, err, "poisoned")
	_, again := session.Resume(prepared.Ready[0].Key, map[string]any{"Prepared": map[string]any{"Ok": []int{}}})
	require.Same(t, err, again)
	require.NoError(t, session.Close())
	require.NoError(t, session.Close())
	_, err = session.Resume(savedKey, map[string]any{"Clock": 0})
	require.ErrorContains(t, err, "closed")
	// Exceed the ABI's simultaneous allocation bounds through sequential Go
	// sessions. Missing Cgo output_release/session_close would exhaust the registry.
	var value map[string]any
	require.NoError(t, json.Unmarshal(original, &value))
	for n := 0; n < 66; n++ {
		value["generation"] = 900001 + n
		encoded, err := json.Marshal(value)
		require.NoError(t, err)
		s, p, err := Start(encoded)
		require.NoError(t, err)
		require.Len(t, p.Ready, 1)
		require.NoError(t, s.Close())
	}
}
