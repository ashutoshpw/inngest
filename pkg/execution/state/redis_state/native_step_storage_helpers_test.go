package redis_state

import (
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	sv2 "github.com/inngest/inngest/pkg/execution/state/v2"
	queuehelper "github.com/inngest/inngest/tests/execution/queue/helper"
	"github.com/oklog/ulid/v2"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
)

type nativeScriptManifest struct {
	Path          string            `json:"path"`
	RawSHA256     string            `json:"rawSHA256"`
	Includes      map[string]string `json:"includesSHA256"`
	ExpandedBytes int               `json:"expandedBytes"`
	SHA1          string            `json:"sha1"`
	SHA256        string            `json:"sha256"`
}
type nativeQueueFixture struct {
	FallbackJobID  string            `json:"fallbackJobID"`
	HashAnswers    map[string]string `json:"hashAnswers"`
	JobID          string            `json:"jobID"`
	HashID         string            `json:"hashID"`
	Keys           []string          `json:"keys"`
	Item           json.RawMessage   `json:"item"`
	Partition      json.RawMessage   `json:"partition"`
	Backlog        json.RawMessage   `json:"backlog"`
	Shadow         json.RawMessage   `json:"shadow"`
	ScoreMS        int64             `json:"scoreMS"`
	PointerSeconds int64             `json:"pointerSeconds"`
	NowMS          int64             `json:"nowMS"`
}
type nativeSaveCase struct {
	Name              string    `json:"name"`
	Pending           []string  `json:"pending"`
	Error             string    `json:"error"`
	HasPending        bool      `json:"hasPending"`
	Attempts          int       `json:"attempts"`
	Results           [][]int64 `json:"results"`
	Remaining         []string  `json:"remaining"`
	StateSizeDelta    int       `json:"stateSizeDelta"`
	MetadataSizeDelta int       `json:"metadataSizeDelta"`
	StepCount         int       `json:"stepCount"`
	Stack             []string  `json:"stack"`
}
type nativeEffectsFixture struct {
	AdapterCases              []nativeSaveCase                `json:"adapterCases"`
	QueueSnapshots            map[string]json.RawMessage      `json:"queueSnapshots"`
	InitialMetadata           map[string]string               `json:"initialMetadata"`
	InitialEvents             string                          `json:"initialEvents"`
	InitialStateSizeWithInput int                             `json:"initialStateSizeWithInput"`
	Queue                     nativeQueueFixture              `json:"queue"`
	FormatVersion             int                             `json:"formatVersion"`
	Scripts                   map[string]nativeScriptManifest `json:"scripts"`
	SaveKeys                  []string                        `json:"saveKeys"`
	Output                    string                          `json:"output"`
	OutputBytes               int                             `json:"outputBytes"`
	Conflict                  string                          `json:"conflict"`
	Input                     string                          `json:"input"`
	InputBytes                int                             `json:"inputBytes"`
	MetadataDelta             int                             `json:"metadataDelta"`
}

func loadNativeEffects(t *testing.T) nativeEffectsFixture {
	t.Helper()
	b, err := os.ReadFile("testdata/compatibility/native-step-effects-v1.json")
	require.NoError(t, err)
	var f nativeEffectsFixture
	require.NoError(t, json.Unmarshal(b, &f))
	require.Equal(t, 1, f.FormatVersion)
	return f
}

// Direct Go expansion follows the existing single-pass include contract. This
// asserts a checked-in manifest; unlike TestLuaScriptSnapshots it never writes.
func nativeScript(t *testing.T, m nativeScriptManifest) string {
	t.Helper()
	raw, err := os.ReadFile(m.Path)
	require.NoError(t, err)
	require.Equal(t, m.RawSHA256, fmt.Sprintf("%x", sha256.Sum256(raw)))
	expanded := string(raw)
	seen := map[string]string{}
	for _, match := range include.FindAllStringSubmatch(string(raw), -1) {
		b, err := os.ReadFile("lua/includes/" + match[1])
		require.NoError(t, err)
		seen[match[1]] = fmt.Sprintf("%x", sha256.Sum256(b))
		expanded = strings.ReplaceAll(expanded, match[0], string(b))
	}
	require.Equal(t, m.Includes, seen)
	require.Len(t, expanded, m.ExpandedBytes)
	require.Equal(t, m.SHA1, fmt.Sprintf("%x", sha1.Sum([]byte(expanded))))
	require.Equal(t, m.SHA256, fmt.Sprintf("%x", sha256.Sum256([]byte(expanded))))
	return expanded
}

type nativeStoredValue struct {
	Type  string
	Value any
	PTTL  int64
}
type nativeSnapshot map[string]nativeStoredValue

// Store truth comes only from this separate cache-disabled connection. Scan all
// keys, including placeholders, so unexpected writes cannot escape comparison.
func snapshotNativeStore(t *testing.T, ctx context.Context, r rueidis.Client) nativeSnapshot {
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
	out := nativeSnapshot{}
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
		out[key] = nativeStoredValue{typ, value, ttl}
	}
	return out
}
func nativeStoreClients(t *testing.T) (rueidis.Client, rueidis.Client) {
	t.Helper()
	v, err := queuehelper.StartValkey(t)
	require.NoError(t, err)
	connect := func() rueidis.Client {
		c, e := queuehelper.NewValkeyClient(v.Addr, v.Username, v.Password, false)
		require.NoError(t, e)
		t.Cleanup(c.Close)
		return c
	}
	return connect(), connect()
}
func nativeAdapter(t *testing.T, c rueidis.Client, prefix string, sharded bool, opts ...MgrV2Opt) sv2.RunService {
	t.Helper()
	fn := NeverShardOnRun
	if sharded {
		fn = AlwaysShardOnRun
	}
	u := NewUnshardedClient(c, prefix, QueueDefaultKey)
	s := NewShardedClient(ShardedClientOpts{UnshardedClient: u, FunctionRunStateClient: c, BatchClient: c, StateDefaultKey: prefix, QueueDefaultKey: QueueDefaultKey, FnRunIsSharded: fn})
	m, err := New(t.Context(), WithShardedClient(s))
	require.NoError(t, err)
	return MustRunServiceV2(m, opts...)
}
func nativeMetadata() sv2.Metadata {
	return sv2.Metadata{ID: sv2.ID{RunID: ulid.MustParse("01ARZ3NDEKTSV4RRFFQ69G5FAV"), FunctionID: uuid.MustParse("00000000-0000-0000-0000-000000000001"), Tenant: sv2.Tenant{AccountID: uuid.MustParse("00000000-0000-0000-0000-000000000003"), EnvID: uuid.MustParse("00000000-0000-0000-0000-000000000002"), AppID: uuid.MustParse("00000000-0000-0000-0000-000000000004")}}, Config: *sv2.InitConfig(&sv2.Config{ScheduledAt: time.UnixMilli(1793016000000), RequestVersion: 2, Idempotency: "run-key", EventIDs: []ulid.ULID{ulid.MustParse("01ARZ3NDEKTSV4RRFFQ69G5FAW")}, SpanID: "0123456789abcdef"})}
}
func nativeSaveKeys(f nativeEffectsFixture, prefix string, sharded bool) []string {
	if sharded {
		prefix += ":01ARZ3NDEKTSV4RRFFQ69G5FAV"
	}
	keys := make([]string, len(f.SaveKeys))
	for j, k := range f.SaveKeys {
		keys[j] = strings.ReplaceAll(k, "STATE", prefix)
	}
	return keys
}

// Only this exact script and fixed KEYS/ARGV may be faulted. All other commands
// forward unchanged. The error result comes from a real canceled-context command;
// the caller context remains live, allowing the real adapter's retry to execute.
type nativeScriptClient struct {
	rueidis.Client
	t           *testing.T
	manifest    nativeScriptManifest
	body        string
	keys, args  []string
	mode        string
	scalar      bool
	calls       int
	commands    [][]string
	results     [][]int64
	afterCommit func()
}

func (c *nativeScriptClient) Do(ctx context.Context, cmd rueidis.Completed) rueidis.RedisResult {
	tokens := slices.Clone(cmd.Commands())
	if len(tokens) < 3 || (tokens[0] != "EVALSHA" && tokens[0] != "EVAL") {
		return c.Client.Do(ctx, cmd)
	}
	if tokens[1] != c.manifest.SHA1 && tokens[1] != c.body {
		return c.Client.Do(ctx, cmd)
	}
	require.Equal(c.t, strconv.Itoa(len(c.keys)), tokens[2])
	require.Equal(c.t, c.keys, tokens[3:3+len(c.keys)])
	require.Equal(c.t, c.args, tokens[3+len(c.keys):])
	c.commands = append(c.commands, tokens)
	c.calls++
	canceled := func() rueidis.RedisResult {
		require.NoError(c.t, ctx.Err())
		other, cancel := context.WithCancel(context.Background())
		cancel()
		r := c.Client.Do(other, c.Client.B().Ping().Build())
		require.ErrorIs(c.t, r.Error(), context.Canceled)
		require.NoError(c.t, ctx.Err())
		return r
	}
	if c.calls == 1 && c.mode == "before" {
		return canceled()
	}
	r := c.Client.Do(ctx, cmd)
	if redisErr, ok := rueidis.IsRedisErr(r.Error()); ok && redisErr.IsNoScript() {
		c.calls--
		return r
	}
	var vals []int64
	var err error
	if c.scalar {
		var v int64
		v, err = r.AsInt64()
		vals = []int64{v}
	} else {
		vals, err = r.AsIntSlice()
	}
	require.NoError(c.t, err)
	c.results = append(c.results, vals)
	if c.calls == 1 && c.mode == "after" {
		require.Contains(c.t, []int64{0, 1}, vals[0], "fault must follow a real new commit")
		require.NotNil(c.t, c.afterCommit)
		c.afterCommit()
		return canceled()
	}
	return r
}

// Only the run-creation idempotency TTL moves with real server time. Preserve
// raw observations; validate its bounded decrease before equality comparison.
func requireNativeSnapshot(t *testing.T, expected, actual nativeSnapshot) {
	t.Helper()
	copyActual := nativeSnapshot{}
	for key, value := range actual {
		if strings.Contains(key, ":key:run-key") {
			require.Greater(t, value.PTTL, int64(0))
			require.LessOrEqual(t, value.PTTL, expected[key].PTTL)
			require.InDelta(t, expected[key].PTTL, value.PTTL, 5000)
			value.PTTL = expected[key].PTTL
		}
		copyActual[key] = value
	}
	require.Equal(t, expected, copyActual)
}

func expectedNativeQueueSnapshot(t *testing.T, f nativeEffectsFixture, prefix string, keys bool, item string) nativeSnapshot {
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
	out := nativeSnapshot{}
	for key, v := range raw {
		var value any
		switch v.Type {
		case "hash":
			var hash map[string]string
			require.NoError(t, json.Unmarshal(v.Value, &hash))
			replaced := map[string]string{}
			for field, entry := range hash {
				if field == "JOB" {
					field = f.Queue.HashID
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
					zs[j].Member = f.Queue.HashID
				}
			}
			value = zs
		default:
			t.Fatalf("unsupported expected queue value type %s", v.Type)
		}
		out[strings.ReplaceAll(key, "QUEUE", prefix)] = nativeStoredValue{v.Type, value, v.PTTL}
	}
	return out
}
