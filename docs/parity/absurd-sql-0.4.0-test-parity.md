# Absurd SQL 0.4.0 test parity

## Purpose

This document tracks high-signal test parity for the Rust SDK against the upstream Absurd `0.4.0` Python, TypeScript, and Go SDK suites.

Coverage parity is not the goal. The goal is confidence in public Rust SDK behavior, durable workflow semantics, and intentional Rust deviations documented in `docs/parity/absurd-sql-0.4.0.md`.

## Target

- Absurd SQL tag: `0.4.0`
- Absurd SQL commit: `05282a40c8dddc378acdc6933adc4c221583808a`
- Rust crate version: `0.1.0`
- Rust test suites:
  - unit tests in `src/**`
  - ignored database integration suite in `tests/integration.rs`

## Scope

Included:

- public Rust SDK behavior
- database-backed durable semantics surfaced through the SDK
- failure, cancellation, retry, worker, event, checkpoint, and task-result invariants
- Rust-specific diagnostics and intentional API decisions

Excluded:

- duplicate sync/async coverage from Python and TypeScript
- language-specific decorator, contextvar, or global-context behavior
- Go driver matrix tests
- raw claim/execute APIs that Rust intentionally keeps internal
- `absurdctl`, cron, SQL migration, partition maintenance, and storage-engine tests owned by upstream Absurd
- exact traceback-string parity, which Rust intentionally does not implement

## Test parity matrix

| Area | Rust status | Existing Rust evidence | High-signal action |
|---|---|---|---|
| Absurd SQL `0.4.0` schema target | Covered | CI installs upstream SQL; DB integration suite runs against it | None |
| Connection defaults | Partial | `Client::from_env*` exists; no direct Rust test for env precedence | Add unit tests for explicit URL, `ABSURD_DATABASE_URL`, `DATABASE_URL`, `PGDATABASE`, and localhost fallback |
| Queue create, list, drop | Covered | `queue_create_list_drop_round_trip` | None |
| Partitioned queue creation | Covered | `partitioned_queue_create_round_trip` | Optional partitioned idempotency smoke; not required now |
| Queue policy get/set | Covered | `queue_policy_round_trip`; queue policy wire-key unit tests | None |
| Typed task registration and spawn | Covered | `basic_typed_task_round_trip` | None |
| Spawn options | Partial | `spawn_options_use_sql_wire_keys`; header and idempotency integration tests | Add max-attempt override/default persistence; reject unregistered spawn without queue; reject registered queue mismatch |
| Spawn headers | Covered | `spawn_headers_are_available_to_task_context`; hook tests | None |
| Idempotent spawn | Covered | `idempotent_spawn_creates_one_task_and_executes_once` | Optional different-key and queue-scope cases; not required now |
| Retry without explicit strategy | Covered | `failed_task_retries_immediately_until_success` | None |
| Fixed retry strategy | Covered | `fixed_retry_strategy_delays_next_attempt` | None |
| Exponential retry strategy | Missing | wire shape exists through `RetryStrategy`, but no DB behavior test | Add DB integration test using `absurd.fake_now` to validate increasing delay and max cap |
| Explicit none retry strategy | Missing | `RetryStrategy::none()` exists; no DB behavior test | Add DB integration test that `kind: none` is persisted and requeues immediately when attempts remain |
| Cancellation by max delay | Missing | wire serialization through `CancellationPolicy`; no DB behavior test | Add DB integration test where a delayed first claim cancels the task |
| Cancellation by max duration | Missing | wire serialization through `CancellationPolicy`; no DB behavior test | Add DB integration test where retry after elapsed duration cancels the task |
| Default task cancellation | Missing | `task_builder_sets_default_cancellation` only checks builder state | Add DB integration test proving task-level default cancellation is applied when spawn options omit cancellation |
| Worker batch execution | Partial | `basic_typed_task_round_trip`; event batch tests | Add mixed success/failure batch test proving failure of one task does not prevent later claimed tasks from running |
| Continuous worker lifecycle | Partial | `worker_close_waits_for_running_tasks` | Add concurrency-limit test for `start_worker` |
| Worker error callback | Covered | `worker_on_error_reports_claim_failures`; `worker_on_error_reports_execution_failures`; unit callback tests | None |
| Durable `TaskContext::step` checkpoint | Covered | `step_checkpoint_is_reused_after_retry`; `failed_step_is_not_checkpointed_and_reexecutes` | Add multistep retry test where only unfinished steps re-execute |
| Decomposed steps | Partial | `decomposed_step_handle_reuses_completed_state_after_retry`; `failed_decomposed_step_is_not_checkpointed_and_reexecutes` | Add repeated `begin_step` / `complete_step` name numbering test |
| Sleep / scheduled resume | Covered | `sleep_until_suspends_then_resumes_from_checkpoint` | None |
| Event await/emit | Partial | `pre_emitted_event_is_available_to_late_waiter`; `emitted_event_wakes_all_waiters`; `event_timeout_can_be_caught_without_recreating_wait` | Add first-write-wins `emit_event` test |
| Task result fetch/await | Partial | terminal fetch/await, failed/cancelled/missing, timeout, same-queue rejection, completed cross-queue await | Add cross-queue pending child wait; add checkpoint-survives-parent-retry-after-child-cleanup test |
| Manual retry task | Covered | default, max-attempt override, spawn-new, missing-task, and non-failed-task tests | None |
| Manual cancel task | Partial | pending cancel and terminal race tests | Add running cancel, sleeping cancel, completed/failed no-op, and missing-task error tests |
| Cleanup tasks/events | Missing | No Rust integration test found; parity doc currently claims coverage | Add `Client::cleanup` / `cleanup_with_limit` integration test for terminal tasks and emitted events |
| Hooks | Covered | before-spawn inject/preserve, wrap execution, and context access tests | None |
| Failure payload shape | Covered | user error, invalid headers, task-result failure payload assertions | None |
| Panic handling | Covered | `panic_failure_payload_records_diagnostic_shape` | None |
| Raw task claim API | Not applicable | intentionally internal in Rust | Do not add public parity tests |
| Sync client API | Not applicable | Rust SDK is intentionally async-only | Do not add |
| Global/decorator task context | Not applicable | Rust uses explicit `TaskContext` | Do not add |
| Logger injection | Not applicable | Rust uses `tracing` and `WorkerOptions::on_error` | Do not add |
| Failure traceback strings | Not applicable | Rust asserts diagnostic payload with `traceback: null` | Do not chase upstream traceback-string tests |

## Recommended implementation backlog

### Required high-signal additions

1. `cleanup_removes_terminal_tasks_and_events_by_ttl`
2. `cancellation_policy_cancels_by_max_delay`
3. `cancellation_policy_cancels_by_max_duration`
4. `default_task_cancellation_is_applied`
5. `exponential_retry_strategy_delays_attempts`
6. `retry_strategy_none_requeues_immediately`
7. `spawn_resolution_applies_defaults_overrides_and_rejects_queue_mismatches`
8. `work_batch_handles_mixed_success_and_failure`
9. `worker_respects_concurrency_limit`
10. `event_emit_is_first_write_wins`
11. `context_await_task_result_waits_for_pending_child_in_other_queue`
12. `context_await_task_result_checkpoint_survives_parent_retry_after_child_cleanup`
13. `manual_cancel_running_sleeping_terminal_and_missing_cases`
14. `multi_step_only_reexecutes_uncompleted_steps`
15. `repeated_decomposed_step_names_are_numbered`
16. `connection_defaults_resolve_env_precedence`

### Test design notes

- Prefer one focused integration test per durable invariant.
- Use `SET absurd.fake_now` where possible instead of real sleeps for retry and cancellation timing.
- Keep optional permutation tests out unless they catch a distinct class of regression.
- Keep language-specific upstream tests out of the Rust suite unless Rust exposes an equivalent public surface.

## Validation commands

```sh
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
ABSURD_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/absurd_test cargo test-db
```
