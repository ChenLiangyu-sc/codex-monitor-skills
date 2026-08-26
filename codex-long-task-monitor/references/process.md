# Synchronous Process Backend

Use this attached backend only for a predictably short local or remote command expected to finish within about two minutes and whose terminal exit code is observable. For longer work, use an execution supervisor or backend authority outside the monitoring agent; never extend this recipe into main-agent polling.

## New command

Have the main agent resolve and review the exact argument vector, working directory, environment requirements, output paths, and overwrite behavior first. Confirm that the user's request authorizes execution.

The monitor subagent may then run that exact command and wait through both tool layers. It must:

- execute without adding shell pipelines, retries, cleanup, cancellation, or fallback arguments;
- preserve the requested working directory and environment boundary;
- wait on the existing execution session rather than starting repeated `ps` checks;
- call inner `tools.write_stdin` for an `exec_io_session_id` until an explicit `exit_code`;
- call outer `functions.wait` only when `functions.exec` returns an `outer_cell_id`;
- never end the agent turn or claim background monitoring continues while that cell is active;
- report the child exit code and elapsed time once;
- stop after the first terminal result.

Use this `functions.exec` JavaScript shape. The isolate has no Node APIs, filesystem API, or `console`:

```js
let result = await tools.exec_command({
  cmd: exactCommand,
  workdir: exactWorkingDirectory,
  yield_time_ms: 30000,
  max_output_tokens: 2000,
});

let execIoSessionId = result.session_id;
const hasTerminalExit = (value) =>
  Object.prototype.hasOwnProperty.call(value ?? {}, "exit_code") &&
  value.exit_code !== null &&
  value.exit_code !== undefined;

while (!hasTerminalExit(result)) {
  if (!execIoSessionId) throw new Error("lost exec I/O observability");
  result = await tools.write_stdin({
    session_id: execIoSessionId,
    chars: "",
    yield_time_ms: 30000,
    max_output_tokens: 2000,
  });
  if (result.session_id) execIoSessionId = result.session_id;
}

text(JSON.stringify({exit_code: result.exit_code}));
```

If the outer `functions.exec` call yields, repeatedly call `functions.wait({cell_id: outer_cell_id})` until it finishes. Never pass `exec_io_session_id` to `functions.wait`. Empty `output` is not terminal, and `exit_code=0` must not be tested by truthiness.

This backend is `monitor-survival-dependent`. Do not describe it as zero-token unattended monitoring. For a long task with a deterministic external supervisor and atomic terminal artifact, use the artifact backend so watcher interruption does not lose the child exit status.

Treat exit code zero as command completion, not output acceptance. The main agent must inspect the expected artifacts and project-specific success marker.

For a note-generation batch, monitor the single batch command/session rather than spawning one monitor per video. After it exits, the main agent must verify the expected item count, per-item statuses, failure manifest, output paths, and note-quality hard gates.

## Existing process

Prefer an owned tool-session handle, task-manager ID, or atomic status record. A PID alone is weak evidence because it can disappear without a recorded exit code or be reused. If only a PID is available, report process disappearance as `lost_observability`, not success.

Treat a monitor agent that returns before its owned session exits as `lost_observability`. Do not replace, attach to, signal, retry, or clean up an orphan without separate authority. First report the exact handle and reconcile any terminal record and business side effects.

Do not attach debuggers, read unrelated process environments, or send signals unless the main agent separately authorizes that action.

## Timeout

Treat timeout as an escalation event. Do not terminate the process automatically. Report the handle and last observable state so the main agent can decide whether to continue waiting, diagnose, or request cancellation authority.
