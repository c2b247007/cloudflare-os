// Server-side classification and retry for Durable Object resets.
//
// When a call into a DO fails because the object was reset (storage timeout, overload abort,
// code update) or the connection to it was lost, workerd tags the rejection with structured
// flags (workerd jsg/util.c++): `retryable` ⇔ kj DISCONNECTED, `overloaded` ⇔ kj OVERLOADED
// (mutually exclusive with `retryable`), and `durableObjectReset` orthogonally whenever the
// target object's incarnation died — the production storage-timeout reset arrives as
// `{remote, overloaded, durableObjectReset}`. The flags are attached natively where the call
// was made, so reading them here needs no message matching and survives no serialization
// boundary. (`enhanced_error_serialization` only matters for flags crossing to a browser.)
//
// Note vitest-pool-workers aborts reject FLAGLESS (pinned by the "user-DO reset flags"
// integration test), so these paths are unit-tested with synthetic errors and integration
// tests assert recovery behaviorally.

/** True for rejections caused by a DO reset or a lost connection to the object — cases where
 * the object restarts on the next request and a retry of an IDEMPOTENT call through a fresh
 * stub is expected to succeed. `overloaded` without `durableObjectReset` is deliberately
 * excluded: the object is alive but shedding load, and a retry adds to the overload — that
 * live-object case is what the DO error-handling docs' "never retry .overloaded" guidance is
 * about. An overloaded RESET is retried despite the docs' blanket wording: the production
 * storage-timeout reset (the error this module exists for) arrives as
 * `{remote, overloaded, durableObjectReset}`, the queue that was overloaded died with the
 * incarnation, and one jittered attempt against the restarted object is not a retry loop. */
export function isDoResetError(e: unknown): boolean {
  if (typeof e !== "object" || e === null) return false;
  const flags = e as { durableObjectReset?: unknown; retryable?: unknown };
  return flags.durableObjectReset === true || flags.retryable === true;
}

export interface DoRetryInfo {
  /** The attempt that just failed (1 = the initial call). */
  attempt: number;
  error: unknown;
  delayMs: number;
}

/** Runs `fn`, retrying ONCE if it rejects with a DO-reset error, after a short jittered delay
 * (the object restarts on its next request, one same-colo hop away — this is why recovery
 * lives in the Worker and not in the browser, 1.5s of internet away).
 *
 * `fn` MUST be idempotent and MUST mint a fresh stub per invocation: a stub is bound to one
 * incarnation of the object and is permanently broken once that incarnation resets (see the
 * DO error-handling docs). `onRetry` fires before the delay. A second failure propagates
 * unchanged — the client's socket-level recovery is the backstop, not a retry loop. */
export async function retryOnDoReset<T>(
    fn: () => Promise<T>,
    onRetry?: (info: DoRetryInfo) => void,
    delayRangeMs: readonly [number, number] = [150, 400]): Promise<T> {
  try {
    return await fn();
  } catch (e) {
    if (!isDoResetError(e)) throw e;
    const delayMs = delayRangeMs[0] + Math.random() * (delayRangeMs[1] - delayRangeMs[0]);
    onRetry?.({ attempt: 1, error: e, delayMs });
    await scheduler.wait(delayMs);
    return await fn();
  }
}
