# SDK response recovery baseline

**Phase:** 1, partial Go reference increment (stack layer 4)

**Base:** `240c75432dcf6e895399287a0414cbfd0ab43ef2`,
`t3/queue-persistence-compat-fixtures`, [parent PR #3](https://github.com/ashutoshpw/inngest/pull/3).
The synthetic corpus was authored against layer 2 `8ebc428587cc11220e86d6fbbd5ab7ffc3bf5fef`;
its provenance remains pinned there after integration with layer 3.

**Delivery:** [PR #4](https://github.com/ashutoshpw/inngest/pull/4), head branch
`t3/recovery-replay-witness`, base branch `t3/queue-persistence-compat-fixtures`.
Opening implementation commit: `0394e148e016f78647ccab4520396c821eef5f5d`.
Open, non-draft; no GitHub reviews or check runs at opening. Linked to parent
thread `c37dccc0-74a6-4b56-bc19-0ffcf321cc19`; merge pending. The delivery handoff
reports the current head after this metadata commit.

## Accepted boundary

The [fixture README](../../../../../tests/execution/executor/testdata/sdk-response-replay-v1/README.md)
contains the literal HTTP/request/state corpus, authoring gate, exact validation,
prior race/mutation evidence, and detailed limits. The
[test](../../../../../tests/execution/executor/recovery_replay_test.go)
serves a synthetic SDK response over actual HTTP. Runtime A saves its output
before discovery enqueue fails; the real queue worker durably retries the
original job with attempt 0 and releases its lease. After A drains and closes,
runtime B opens fresh adapters, loader, executor, driver, queue, and clients.
Its normal scanner discovers that persisted job and sends the saved output in
the HTTP replay. Fresh readers check the checkpoint, one future sleep, original
retry removal, and one non-deduped synthetic external effect.

Independent Luna-max review found no blockers. Both key-queue modes were wired
and passed three race repetitions each; three production mutations failed the
intended replay, checkpoint-ordering, and always-retry assertions. Production
sources were restored. Those accepted experiments need no repetition for this
documentation/stack integration.

## Integration validation

Go 1.26.4 on Linux amd64; Go temporary files live under `/home/ashutosh/.cache`.
After advancing to layer 3, the targeted recovery test passed once in each
`EXPERIMENTAL_KEY_QUEUES_ENABLE=false/true` mode (2.285 s / 2.185 s), and the new
queue/run-state compatibility tests passed (0.027 s). Exact commands and results are recorded in the fixture
README. Runtime tracked files remain unchanged. Formatting, JSON, links, and staged
whitespace checks passed. The plan-status script still flags the pre-existing
invalid status in `docs/plans/run-ai-debugging.md`; that file is unchanged.

## Limits and remaining work

This covers drained owner replacement against surviving stores. It does not
cover OS process crash/restart, recovery with an active lease, Valkey restart
or disk durability, concurrent owners, SDK conformance/version policy, general
exactly-once execution, or broader queue/lifecycle parity. The future sleep is
inspected rather than completed. Broader SDK-response replay, Phase 1 fault and
performance baselines, and Rust comparisons remain open. Linux amd64/arm64 is
first; macOS/Windows and cloud parity retain their existing gates.
