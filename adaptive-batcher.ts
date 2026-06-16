export enum BatcherErrorCode {
  DISPOSED = 'BATCHER_DISPOSED',
  QUEUE_OVERFLOW = 'QUEUE_OVERFLOW',
  FLUSH_FAILED = 'FLUSH_FAILED',
  MISMATCHED_RESULTS = 'MISMATCHED_RESULTS',
  CANCELED = 'CANCELED',
  TIMEOUT = 'TIMEOUT',
  DROPPED = 'DROPPED',
}

export class BatcherError extends Error {
  readonly code: BatcherErrorCode;
  readonly retryable: boolean;
  readonly data?: unknown;

  constructor(
    code: BatcherErrorCode,
    message: string,
    options: { retryable?: boolean; data?: unknown; cause?: unknown } = {}
  ) {
    super(message);
    this.name = 'BatcherError';
    this.code = code;
    this.retryable = options.retryable ?? true;
    this.data = options.data;
    if (options.cause) (this as unknown as { cause: unknown }).cause = options.cause;
  }
}

export type OverflowStrategy = 'reject' | 'drop' | 'block';
export type DisposeStrategy = 'drain' | 'reject' | 'kill';

export interface SubmitOptions {
  timeoutMs?: number;
  signal?: AbortSignal;
}

export interface FlushEvent<T, R = unknown> {
  readonly batchSize: number;
  readonly queuedWaitMs: number;
  readonly flushDurationMs: number;
  readonly success: boolean;
  readonly error?: unknown;
  readonly results?: readonly R[];
  readonly items: readonly T[];
  readonly timestamp: number;
}

export interface RollingStats {
  readonly avgBatchSize: number;
  readonly avgQueuedWaitMs: number;
  readonly avgFlushDurationMs: number;
  readonly p50QueuedWaitMs: number;
  readonly p95QueuedWaitMs: number;
  readonly p50FlushDurationMs: number;
  readonly p95FlushDurationMs: number;
  readonly failureRate: number;
  readonly throughputPerSecond: number;
  readonly itemsProcessed: number;
  readonly batchesProcessed: number;
  readonly batchesFailed: number;
  readonly stale: boolean;
}

export interface HealthStatus {
  readonly status: 'healthy' | 'degraded' | 'unhealthy' | 'stale';
  readonly currentQueueSize: number;
  readonly inflightBatches: number;
  readonly capacityUsedPercent: number;
  readonly ewmaFailureRate: number;
  readonly ewmaFlushDurationMs: number;
  readonly mode: 'low-latency' | 'balanced' | 'high-throughput' | 'idle';
  readonly lastEventAgeMs: number | null;
  readonly rollingStats: RollingStats;
}

export interface BatcherOptions<T, R> {
  minBatchSize?: number;
  maxBatchSize?: number;
  initialBatchSize?: number;
  minWaitMs?: number;
  maxWaitMs?: number;
  initialWaitMs?: number;
  rateWindowMs?: number;
  maxInflightBatches?: number;
  maxQueueSize?: number;
  overflowStrategy?: OverflowStrategy;
  statsWindowMs?: number;
  adaptToFlushDuration?: boolean;
  adaptToFailureRate?: boolean;
  flush: (batch: T[]) => Promise<R[]> | R[];
  onFlush?: (event: FlushEvent<T, R>) => void | Promise<void>;
  onError?: (error: BatcherError, batch: T[]) => void | Promise<void>;
  fallback?: (item: T) => R | Promise<R>;
}

interface PendingItem<T, R> {
  item: T;
  resolve: (result: R) => void;
  reject: (reason: unknown) => void;
  timestamp: number;
  canceled: boolean;
  timeoutTimer?: ReturnType<typeof setTimeout>;
  abortListener?: () => void;
}

interface RateTracker {
  timestamps: number[];
  windowMs: number;
}

interface StatsWindow {
  events: Array<{
    batchSize: number;
    queuedWaitMs: number;
    flushDurationMs: number;
    success: boolean;
    timestamp: number;
  }>;
  windowMs: number;
  itemsProcessed: number;
  batchesProcessed: number;
  batchesFailed: number;
  lastEventTime: number | null;
}

export class AdaptiveBatcher<T, R> {
  private minBatchSize: number;
  private maxBatchSize: number;
  private targetBatchSize: number;
  private minWaitMs: number;
  private maxWaitMs: number;
  private waitWindowMs: number;
  private maxInflightBatches: number;
  private maxQueueSize: number;
  private overflowStrategy: OverflowStrategy;
  private adaptToFlushDuration: boolean;
  private adaptToFailureRate: boolean;

  private activeBuffer: PendingItem<T, R>[] = [];
  private flushLock = false;
  private flushTimer: ReturnType<typeof setTimeout> | null = null;
  private inflightCount = 0;
  private inflightPromises: Set<Promise<void>> = new Set();

  private rateTracker: RateTracker;
  private ewmaRate = 0;
  private ewmaFlushDuration = 0;
  private ewmaFailureRate = 0;
  private readonly alpha = 0.3;

  private statsWindow: StatsWindow;

  private flush: (batch: T[]) => Promise<R[]> | R[];
  private onFlush?: (event: FlushEvent<T, R>) => void | Promise<void>;
  private onError?: (error: BatcherError, batch: T[]) => void | Promise<void>;
  private fallback?: (item: T) => R | Promise<R>;

  private disposed = false;
  private disposing = false;

  constructor(options: BatcherOptions<T, R>) {
    this.minBatchSize = options.minBatchSize ?? 1;
    this.maxBatchSize = options.maxBatchSize ?? 500;
    this.targetBatchSize = options.initialBatchSize ?? Math.max(10, this.minBatchSize);
    this.minWaitMs = options.minWaitMs ?? 2;
    this.maxWaitMs = options.maxWaitMs ?? 200;
    this.waitWindowMs = options.initialWaitMs ?? 50;
    this.maxInflightBatches = options.maxInflightBatches ?? 4;
    this.maxQueueSize = options.maxQueueSize ?? 10_000;
    this.overflowStrategy = options.overflowStrategy ?? 'reject';
    this.adaptToFlushDuration = options.adaptToFlushDuration ?? true;
    this.adaptToFailureRate = options.adaptToFailureRate ?? true;
    this.rateTracker = {
      timestamps: [],
      windowMs: options.rateWindowMs ?? 1000,
    };
    this.statsWindow = {
      events: [],
      windowMs: options.statsWindowMs ?? 5000,
      itemsProcessed: 0,
      batchesProcessed: 0,
      batchesFailed: 0,
      lastEventTime: null,
    };
    this.flush = options.flush;
    this.onFlush = options.onFlush;
    this.onError = options.onError;
    this.fallback = options.fallback;
  }

  submit(item: T, options: SubmitOptions = {}): Promise<R> {
    if (this.disposed || this.disposing) {
      return Promise.reject(
        new BatcherError(BatcherErrorCode.DISPOSED, 'Batcher has been disposed', {
          retryable: false,
        })
      );
    }

    if (options.signal?.aborted) {
      return Promise.reject(
        new BatcherError(BatcherErrorCode.CANCELED, 'Submit canceled by signal', {
          retryable: false,
        })
      );
    }

    const totalQueued = this.activeBuffer.length + this.inflightCount * this.targetBatchSize;
    if (totalQueued >= this.maxQueueSize) {
      return this.handleOverflow(item, options);
    }

    return new Promise<R>((resolve, reject) => {
      const now = Date.now();
      const pendingItem: PendingItem<T, R> = {
        item,
        resolve,
        reject,
        timestamp: now,
        canceled: false,
      };

      if (options.timeoutMs != null && options.timeoutMs > 0) {
        pendingItem.timeoutTimer = setTimeout(() => {
          this.cancelPending(pendingItem, BatcherErrorCode.TIMEOUT, `Submit timed out after ${options.timeoutMs}ms`);
        }, options.timeoutMs);
      }

      if (options.signal) {
        pendingItem.abortListener = () => {
          this.cancelPending(
            pendingItem,
            BatcherErrorCode.CANCELED,
            'Submit canceled by signal'
          );
        };
        options.signal.addEventListener('abort', pendingItem.abortListener);
      }

      this.activeBuffer.push(pendingItem);
      this.recordRequest(now);
      this.tickle();
    });
  }

  private cancelPending(
    item: PendingItem<T, R>,
    code: BatcherErrorCode,
    message: string
  ): void {
    if (item.canceled) return;
    item.canceled = true;

    if (item.timeoutTimer) {
      clearTimeout(item.timeoutTimer);
      item.timeoutTimer = undefined;
    }
    if (item.abortListener) {
      try {
        item.abortListener = undefined;
      } catch {}
    }

    const idx = this.activeBuffer.indexOf(item);
    if (idx !== -1) {
      this.activeBuffer.splice(idx, 1);
    }

    item.reject(new BatcherError(code, message, { retryable: code === BatcherErrorCode.TIMEOUT }));
  }

  private async handleOverflow(item: T, options: SubmitOptions): Promise<R> {
    switch (this.overflowStrategy) {
      case 'drop': {
        if (this.fallback) {
          try {
            return await this.fallback(item);
          } catch (e) {
            throw new BatcherError(
              BatcherErrorCode.DROPPED,
              'Item dropped due to queue overflow, fallback failed',
              { retryable: true, cause: e }
            );
          }
        }
        throw new BatcherError(
          BatcherErrorCode.DROPPED,
          'Item dropped due to queue overflow (no fallback configured)',
          { retryable: true, data: { item } }
        );
      }
      case 'block': {
        const checkInterval = this.minWaitMs;
        const timeoutAt = options.timeoutMs != null ? Date.now() + options.timeoutMs : null;
        const signal = options.signal;

        const poll = (): Promise<R> => {
          if (this.disposed || this.disposing) {
            return Promise.reject(
              new BatcherError(
                BatcherErrorCode.DISPOSED,
                'Batcher disposed while waiting for queue capacity',
                { retryable: false }
              )
            );
          }
          if (signal?.aborted) {
            return Promise.reject(
              new BatcherError(BatcherErrorCode.CANCELED, 'Submit canceled while waiting for capacity', {
                retryable: false,
              })
            );
          }
          if (timeoutAt != null && Date.now() >= timeoutAt) {
            return Promise.reject(
              new BatcherError(BatcherErrorCode.TIMEOUT, 'Timed out waiting for queue capacity', {
                retryable: true,
              })
            );
          }
          const totalQueued = this.activeBuffer.length + this.inflightCount * this.targetBatchSize;
          if (totalQueued < this.maxQueueSize) {
            return this.submit(item, options);
          }
          return new Promise<R>((res, rej) =>
            setTimeout(() => poll().then(res).catch(rej), checkInterval)
          );
        };
        return poll();
      }
      case 'reject':
      default:
        throw new BatcherError(
          BatcherErrorCode.QUEUE_OVERFLOW,
          `Queue overflow: ${this.maxQueueSize} items max`,
          {
            retryable: true,
            data: { item, queueSize: this.activeBuffer.length, inflightCount: this.inflightCount },
          }
        );
    }
  }

  private recordRequest(now: number): void {
    const { timestamps, windowMs } = this.rateTracker;
    timestamps.push(now);
    while (timestamps.length > 0 && now - timestamps[0] > windowMs) {
      timestamps.shift();
    }
    const instantRate = timestamps.length / (windowMs / 1000);
    this.ewmaRate = this.alpha * instantRate + (1 - this.alpha) * this.ewmaRate;
    this.adaptParameters();
  }

  private recordFlush(
    batchSize: number,
    queuedWaitMs: number,
    flushDurationMs: number,
    success: boolean,
    now: number
  ): void {
    if (this.adaptToFlushDuration) {
      this.ewmaFlushDuration =
        this.alpha * flushDurationMs + (1 - this.alpha) * this.ewmaFlushDuration;
    }
    if (this.adaptToFailureRate) {
      this.ewmaFailureRate =
        this.alpha * (success ? 0 : 1) + (1 - this.alpha) * this.ewmaFailureRate;
    }
    const sw = this.statsWindow;
    sw.events.push({ batchSize, queuedWaitMs, flushDurationMs, success, timestamp: now });
    sw.itemsProcessed += batchSize;
    sw.batchesProcessed += 1;
    if (!success) sw.batchesFailed += 1;
    sw.lastEventTime = now;
    while (sw.events.length > 0 && now - sw.events[0].timestamp > sw.windowMs) {
      const evicted = sw.events.shift()!;
      sw.itemsProcessed -= evicted.batchSize;
      sw.batchesProcessed -= 1;
      if (!evicted.success) sw.batchesFailed -= 1;
    }
    this.adaptParameters();
  }

  private isStatsStale(): boolean {
    if (this.statsWindow.lastEventTime == null) return true;
    const age = Date.now() - this.statsWindow.lastEventTime;
    return age > this.statsWindow.windowMs * 2;
  }

  private adaptParameters(): void {
    const rate = this.ewmaRate;
    let sizeMultiplier = 1;
    let waitMultiplier = 1;

    if (this.adaptToFlushDuration && this.ewmaFlushDuration > 0) {
      const targetFlushMs = 200;
      if (this.ewmaFlushDuration > targetFlushMs * 2) {
        sizeMultiplier *= 0.6;
        waitMultiplier *= 0.7;
      } else if (this.ewmaFlushDuration > targetFlushMs) {
        sizeMultiplier *= 0.85;
        waitMultiplier *= 0.9;
      } else if (this.ewmaFlushDuration < targetFlushMs * 0.3) {
        sizeMultiplier *= 1.15;
      }
    }

    if (this.adaptToFailureRate) {
      if (this.ewmaFailureRate > 0.5) {
        sizeMultiplier *= 0.3;
        waitMultiplier *= 0.4;
      } else if (this.ewmaFailureRate > 0.2) {
        sizeMultiplier *= 0.6;
        waitMultiplier *= 0.7;
      } else if (this.ewmaFailureRate > 0.05) {
        sizeMultiplier *= 0.85;
        waitMultiplier *= 0.9;
      }
    }

    sizeMultiplier = Math.max(0.2, Math.min(1.5, sizeMultiplier));
    waitMultiplier = Math.max(0.3, Math.min(1.2, waitMultiplier));

    let baseSize: number;
    let baseWait: number;

    if (rate < 10) {
      baseSize = Math.max(
        this.minBatchSize,
        Math.round(this.minBatchSize + rate * 0.5)
      );
      baseWait = this.lerp(this.minWaitMs, this.maxWaitMs * 0.2, rate / 10);
    } else if (rate < 200) {
      const t = (rate - 10) / 190;
      baseSize = Math.round(
        this.lerp(this.minBatchSize + 5, this.maxBatchSize * 0.6, t)
      );
      baseWait = this.lerp(this.maxWaitMs * 0.2, this.maxWaitMs * 0.7, t);
    } else {
      const t = Math.min(1, (rate - 200) / 800);
      baseSize = Math.round(
        this.lerp(this.maxBatchSize * 0.6, this.maxBatchSize, t)
      );
      baseWait = this.lerp(this.maxWaitMs * 0.7, this.maxWaitMs, t);
    }

    this.targetBatchSize = this.clamp(
      Math.round(baseSize * sizeMultiplier),
      this.minBatchSize,
      this.maxBatchSize
    );
    this.waitWindowMs = this.clamp(
      baseWait * waitMultiplier,
      this.minWaitMs,
      this.maxWaitMs
    );
  }

  private tickle(): void {
    if (this.flushLock) return;

    const filtered = this.activeBuffer.filter((it) => !it.canceled);
    if (filtered.length !== this.activeBuffer.length) {
      this.activeBuffer = filtered;
    }

    if (this.activeBuffer.length === 0) {
      this.clearTimer();
      return;
    }

    if (this.activeBuffer.length >= this.targetBatchSize) {
      this.clearTimer();
      this.doFlush();
      return;
    }

    const oldest = this.activeBuffer[0].timestamp;
    const elapsed = Date.now() - oldest;
    if (elapsed >= this.waitWindowMs && this.activeBuffer.length >= this.minBatchSize) {
      this.clearTimer();
      this.doFlush();
      return;
    }

    if (elapsed >= this.maxWaitMs) {
      this.clearTimer();
      this.doFlush();
      return;
    }

    if (this.flushTimer === null) {
      const remaining = Math.max(0, this.waitWindowMs - elapsed);
      const safeTimeout = Math.min(
        remaining,
        this.maxWaitMs - elapsed,
        this.minWaitMs * 10
      );
      this.flushTimer = setTimeout(() => {
        this.flushTimer = null;
        this.tickle();
      }, Math.max(1, safeTimeout));
    }
  }

  private safeInvokeObserver<R>(
    fn: (() => R | Promise<R>) | undefined,
    label: string
  ): void {
    if (!fn) return;
    try {
      const result = fn();
      if (result && typeof (result as Promise<R>).then === 'function') {
        (result as Promise<R>).catch((e) => {
          console.warn(`[AdaptiveBatcher] ${label} observer async error:`, e);
        });
      }
    } catch (e) {
      console.warn(`[AdaptiveBatcher] ${label} observer sync error:`, e);
    }
  }

  private async doFlush(): Promise<void> {
    if (this.flushLock) return;
    if (this.activeBuffer.length === 0) return;
    if (this.inflightCount >= this.maxInflightBatches) return;

    this.flushLock = true;
    const batch = this.activeBuffer.filter((it) => !it.canceled);
    this.activeBuffer = [];
    this.flushLock = false;

    if (batch.length === 0) {
      this.tickle();
      return;
    }

    this.inflightCount++;

    const items = batch.map((b) => b.item);
    const queuedAt = batch[0].timestamp;
    const queuedWaitMs = Date.now() - queuedAt;
    const flushStartedAt = Date.now();

    for (const it of batch) {
      if (it.timeoutTimer) {
        clearTimeout(it.timeoutTimer);
        it.timeoutTimer = undefined;
      }
      if (it.abortListener) {
        try {
          it.abortListener = undefined;
        } catch {}
      }
    }

    const inflightPromise = (async () => {
      let success = false;
      let error: unknown;
      let results: R[] | undefined;

      try {
        results = await this.flush(items);
        if (results.length !== batch.length) {
          error = new BatcherError(
            BatcherErrorCode.MISMATCHED_RESULTS,
            `Flush returned ${results.length} results for ${batch.length} items`,
            { retryable: true }
          );
          batch.forEach(({ reject }) => reject(error));
          this.safeInvokeObserver(
            () => this.onError?.(error as BatcherError, items),
            'onError'
          );
        } else {
          success = true;
          batch.forEach(({ resolve }, i) => resolve(results![i]));
        }
      } catch (caught) {
        error = caught;
        const wrapped = new BatcherError(
          BatcherErrorCode.FLUSH_FAILED,
          `Flush failed: ${(caught as Error).message}`,
          { cause: caught, retryable: true }
        );
        batch.forEach(({ reject }) => reject(wrapped));
        this.safeInvokeObserver(
          () => this.onError?.(wrapped, items),
          'onError'
        );
      } finally {
        const now = Date.now();
        const flushDurationMs = now - flushStartedAt;
        this.recordFlush(batch.length, queuedWaitMs, flushDurationMs, success, now);

        this.safeInvokeObserver(
          () =>
            this.onFlush?.({
              batchSize: batch.length,
              queuedWaitMs,
              flushDurationMs,
              success,
              error,
              results,
              items,
              timestamp: now,
            }),
          'onFlush'
        );

        this.inflightCount--;
        this.inflightPromises.delete(inflightPromise);
        this.tickle();
      }
    })();

    this.inflightPromises.add(inflightPromise);
  }

  async forceFlush(): Promise<void> {
    while (this.activeBuffer.length > 0) {
      if (this.inflightCount >= this.maxInflightBatches) {
        await Promise.race(Array.from(this.inflightPromises));
        continue;
      }
      const prevInflight = this.inflightCount;
      this.doFlush();
      if (this.inflightCount === prevInflight && this.activeBuffer.length > 0) {
        await Promise.race(Array.from(this.inflightPromises));
      } else {
        await new Promise((r) => setTimeout(r, 0));
      }
    }
    await this.drainInflight();
  }

  async dispose(strategy: DisposeStrategy = 'drain'): Promise<void> {
    if (this.disposed || this.disposing) return;
    this.disposing = true;
    this.clearTimer();

    if (strategy === 'kill') {
      const err = new BatcherError(
        BatcherErrorCode.DISPOSED,
        'Batcher killed: all pending requests rejected',
        { retryable: true }
      );
      for (const item of this.activeBuffer) {
        if (item.timeoutTimer) clearTimeout(item.timeoutTimer);
        if (item.abortListener) {
          try {
            item.abortListener = undefined;
          } catch {}
        }
        if (!item.canceled) item.reject(err);
      }
      this.activeBuffer = [];
      this.disposed = true;
      await this.drainInflight();
      return;
    }

    if (strategy === 'reject') {
      const err = new BatcherError(
        BatcherErrorCode.DISPOSED,
        'Batcher disposed: queued requests rejected, inflight continuing',
        { retryable: true }
      );
      for (const item of this.activeBuffer) {
        if (item.timeoutTimer) clearTimeout(item.timeoutTimer);
        if (item.abortListener) {
          try {
            item.abortListener = undefined;
          } catch {}
        }
        if (!item.canceled) item.reject(err);
      }
      this.activeBuffer = [];
      this.disposed = true;
      await this.drainInflight();
      return;
    }

    while (this.activeBuffer.length > 0) {
      if (this.inflightCount >= this.maxInflightBatches) {
        await Promise.race(Array.from(this.inflightPromises));
        continue;
      }
      const prevInflight = this.inflightCount;
      this.doFlush();
      if (this.inflightCount === prevInflight && this.activeBuffer.length > 0) {
        await Promise.race(Array.from(this.inflightPromises));
      } else {
        await new Promise((r) => setTimeout(r, 0));
      }
    }

    if (this.activeBuffer.length > 0) {
      const err = new BatcherError(
        BatcherErrorCode.DISPOSED,
        'Batcher disposed before batch could be flushed',
        { retryable: true }
      );
      for (const item of this.activeBuffer) {
        if (item.timeoutTimer) clearTimeout(item.timeoutTimer);
        if (item.abortListener) {
          try {
            item.abortListener = undefined;
          } catch {}
        }
        item.reject(err);
      }
      this.activeBuffer = [];
    }

    this.disposed = true;
    await this.drainInflight();
  }

  private async drainInflight(): Promise<void> {
    if (this.inflightPromises.size === 0) return;
    try {
      await Promise.all(Array.from(this.inflightPromises));
    } catch {
    }
    if (this.inflightPromises.size > 0) {
      await this.drainInflight();
    }
  }

  get metrics() {
    return {
      targetBatchSize: this.targetBatchSize,
      waitWindowMs: this.waitWindowMs,
      currentQueueSize: this.activeBuffer.length,
      inflightBatches: this.inflightCount,
      ewmaRate: Math.round(this.ewmaRate * 100) / 100,
      ewmaFlushDurationMs: Math.round(this.ewmaFlushDuration * 100) / 100,
      ewmaFailureRate: Math.round(this.ewmaFailureRate * 10000) / 10000,
      capacityUsedPercent: Math.round(
        ((this.activeBuffer.length + this.inflightCount * this.targetBatchSize) /
          this.maxQueueSize) *
          100
      ),
    };
  }

  get rollingStats(): RollingStats {
    const sw = this.statsWindow;
    const stale = this.isStatsStale();
    const count = sw.events.length;

    if (count === 0 || stale) {
      return {
        avgBatchSize: 0,
        avgQueuedWaitMs: 0,
        avgFlushDurationMs: 0,
        p50QueuedWaitMs: 0,
        p95QueuedWaitMs: 0,
        p50FlushDurationMs: 0,
        p95FlushDurationMs: 0,
        failureRate: 0,
        throughputPerSecond: 0,
        itemsProcessed: 0,
        batchesProcessed: 0,
        batchesFailed: 0,
        stale,
      };
    }

    const sortedWait = sw.events.map((e) => e.queuedWaitMs).sort((a, b) => a - b);
    const sortedFlush = sw.events.map((e) => e.flushDurationMs).sort((a, b) => a - b);

    const percentile = (arr: number[], p: number): number => {
      if (arr.length === 0) return 0;
      const idx = Math.ceil((p / 100) * arr.length) - 1;
      return arr[Math.max(0, Math.min(arr.length - 1, idx))];
    };

    const avgBatchSize = sw.events.reduce((s, e) => s + e.batchSize, 0) / count;
    const avgQueuedWaitMs = sw.events.reduce((s, e) => s + e.queuedWaitMs, 0) / count;
    const avgFlushDurationMs = sw.events.reduce((s, e) => s + e.flushDurationMs, 0) / count;
    const failureRate = sw.events.reduce((s, e) => s + (e.success ? 0 : 1), 0) / count;
    const windowSeconds = sw.windowMs / 1000;
    const throughputPerSecond = sw.itemsProcessed / windowSeconds;

    return {
      avgBatchSize: Math.round(avgBatchSize * 100) / 100,
      avgQueuedWaitMs: Math.round(avgQueuedWaitMs * 100) / 100,
      avgFlushDurationMs: Math.round(avgFlushDurationMs * 100) / 100,
      p50QueuedWaitMs: Math.round(percentile(sortedWait, 50) * 100) / 100,
      p95QueuedWaitMs: Math.round(percentile(sortedWait, 95) * 100) / 100,
      p50FlushDurationMs: Math.round(percentile(sortedFlush, 50) * 100) / 100,
      p95FlushDurationMs: Math.round(percentile(sortedFlush, 95) * 100) / 100,
      failureRate: Math.round(failureRate * 10000) / 10000,
      throughputPerSecond: Math.round(throughputPerSecond * 100) / 100,
      itemsProcessed: sw.itemsProcessed,
      batchesProcessed: sw.batchesProcessed,
      batchesFailed: sw.batchesFailed,
      stale,
    };
  }

  get mode(): 'low-latency' | 'balanced' | 'high-throughput' | 'idle' {
    if (this.isStatsStale() || this.statsWindow.batchesProcessed === 0) {
      return 'idle';
    }
    const stats = this.rollingStats;
    if (stats.avgFlushDurationMs > 150 || stats.failureRate > 0.2) {
      return 'low-latency';
    }
    if (this.ewmaRate > 150 && stats.avgBatchSize > this.maxBatchSize * 0.4) {
      return 'high-throughput';
    }
    return 'balanced';
  }

  get health(): HealthStatus {
    const m = this.metrics;
    const stats = this.rollingStats;
    const lastEventAgeMs = this.statsWindow.lastEventTime != null
      ? Date.now() - this.statsWindow.lastEventTime
      : null;

    let status: HealthStatus['status'];
    if (stats.stale) {
      status = 'stale';
    } else if (m.ewmaFailureRate > 0.3 || m.capacityUsedPercent > 90 || m.ewmaFlushDurationMs > 500) {
      status = 'unhealthy';
    } else if (m.ewmaFailureRate > 0.05 || m.capacityUsedPercent > 70 || m.ewmaFlushDurationMs > 200) {
      status = 'degraded';
    } else {
      status = 'healthy';
    }

    return {
      status,
      currentQueueSize: m.currentQueueSize,
      inflightBatches: m.inflightBatches,
      capacityUsedPercent: m.capacityUsedPercent,
      ewmaFailureRate: m.ewmaFailureRate,
      ewmaFlushDurationMs: m.ewmaFlushDurationMs,
      mode: this.mode,
      lastEventAgeMs,
      rollingStats: stats,
    };
  }

  private clearTimer(): void {
    if (this.flushTimer !== null) {
      clearTimeout(this.flushTimer);
      this.flushTimer = null;
    }
  }

  private clamp(v: number, lo: number, hi: number): number {
    return Math.max(lo, Math.min(hi, v));
  }

  private lerp(a: number, b: number, t: number): number {
    return a + (b - a) * Math.max(0, Math.min(1, t));
  }
}
