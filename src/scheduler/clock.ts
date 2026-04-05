/**
 * Time, as an injected dependency.
 *
 * Every deadline in this system — attempt timeouts, lease expiry, retry backoff, the
 * workflow deadline — is a comparison against "now". If "now" is `Date.now()` then the
 * only way to test any of it is to sleep, and a test that sleeps for a lease to expire
 * is deciding correctness by racing the scheduler against the host's load average. So
 * nothing in the scheduler reads the wall clock directly; it reads a `Clock`, and the
 * tests hand it one whose time only moves when they say so.
 */

/** Opaque timer identity. Opaque because the fake and the real clock number differently. */
export type TimerHandle = number;

export interface Clock {
  now(): number;

  /**
   * Schedules `fn` for `ms` from now. Returns a handle that `clearTimeout` accepts.
   * Deliberately not `Promise`-based: a cancelled timer must leave nothing pending,
   * and an un-awaited promise cannot be cancelled.
   */
  setTimeout(fn: () => void, ms: number): TimerHandle;

  clearTimeout(handle: TimerHandle): void;
}

/** The production clock. */
export const systemClock: Clock = {
  now: () => Date.now(),
  setTimeout: (fn, ms) => globalThis.setTimeout(fn, ms) as unknown as TimerHandle,
  clearTimeout: (handle) => globalThis.clearTimeout(handle as unknown as NodeJS.Timeout),
};

interface FakeTimer {
  readonly handle: TimerHandle;
  readonly deadline: number;
  readonly fn: () => void;
}

/**
 * A clock whose time advances only on request.
 *
 * Timers fire in deadline order, and `now()` is set to each timer's deadline *before*
 * that timer runs. That detail matters: a lease-expiry callback that observes a `now()`
 * later than its own deadline would compute a different backoff than production does,
 * and the test would be asserting against an artefact of the fake.
 */
export class FakeClock implements Clock {
  private current: number;
  private nextHandle = 1;
  private timers: FakeTimer[] = [];

  constructor(start = 0) {
    this.current = start;
  }

  now(): number {
    return this.current;
  }

  setTimeout(fn: () => void, ms: number): TimerHandle {
    const handle = this.nextHandle++;
    // A non-positive delay still queues rather than running inline, because callers
    // rely on `setTimeout` returning before the callback observes any state it sets.
    this.timers.push({ handle, deadline: this.current + Math.max(0, ms), fn });
    return handle;
  }

  clearTimeout(handle: TimerHandle): void {
    this.timers = this.timers.filter((t) => t.handle !== handle);
  }

  /** Timers not yet fired. Tests assert on this to catch leaked timers. */
  get pending(): number {
    return this.timers.length;
  }

  /**
   * Moves time forward by `ms`, firing every timer that falls due.
   *
   * Async because the callbacks it fires are scheduler callbacks that append to the
   * event store, and the work they queue has to settle before the next timer sees the
   * log. A synchronous `advance` would fire a lease-expiry timer and return before the
   * expiry was recorded, which is exactly the race the fake clock exists to remove.
   */
  async advance(ms: number): Promise<void> {
    const target = this.current + Math.max(0, ms);
    for (;;) {
      const due = this.timers
        .filter((t) => t.deadline <= target)
        .sort((a, b) => a.deadline - b.deadline || a.handle - b.handle)[0];
      if (!due) break;
      this.timers = this.timers.filter((t) => t.handle !== due.handle);
      this.current = Math.max(this.current, due.deadline);
      due.fn();
      await drain();
    }
    this.current = target;
    await drain();
  }

  /** Moves time forward without firing anything, for testing expiry detection on a tick. */
  jump(ms: number): void {
    this.current += Math.max(0, ms);
  }
}

/**
 * Lets already-queued promise callbacks run.
 *
 * `setImmediate` rather than `Promise.resolve()` because the scheduler awaits store
 * operations, and those resolve on the microtask queue in chains several deep; a single
 * microtask turn would return while the chain was still unwinding. This is a yield to
 * the event loop, not a sleep — it does not depend on elapsed wall-clock time.
 */
function drain(): Promise<void> {
  return new Promise<void>((resolve) => setImmediate(resolve));
}
