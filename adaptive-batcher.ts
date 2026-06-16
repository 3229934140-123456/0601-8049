export enum BatcherErrorCode {
  DISPOSED = 'BATCHER_DISPOSED',
  QUEUE_OVERFLOW = 'QUEUE_OVERFLOW',
  FLUSH_FAILED = 'FLUSH_FAILED',
  MISMATCHED_RESULTS = 'MISMATCHED_RESULTS',
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

export interface FlushEvent<T, R = unknown> {
  readonly batchSize: number;
  readonly queuedWaitMs: number;
  readonly flushDurationMs: number;
  readonly success: boolean;
  readonly error?: unknown;
  readonly results?: readonly R[];
  readonly items: readonly T[];
}

export interface RollingStats {
  readonly avgBatchSize: number;
  readonly avgQueuedWaitMs: number;
  readonly avgFlushDurationMs: number;
  readonly failureRate: number;
  readonly throughputPerSecond: number;
  readonly itemsProcessed: number;
  readonly batchesProcessed: number;
  readonly batchesFailed: number;
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
  onFlush?: (event: FlushEvent<T, R>) => void;
  onError?: (error: BatcherError, batch: T[]) => void;
  fallback?: (item: T) => R | Promise<R>;
}

interface PendingItem<T, R> {
  item: T;
  resolve: (result: R) => void;
  reject: (reason: unknown) => void;
  timestamp: number;
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
  private flushDrain: Promise<void> | null = null;
  private resolveFlushDrain: (() => void) | null = null;

  private rateTracker: RateTracker;
  private ewmaRate = 0;
  private ewmaFlushDuration = 0;
  private ewmaFailureRate = 0;
  private readonly alpha = 0.3;

  private statsWindow: StatsWindow;

  private flush: (batch: T[]) => Promise<R[]> | R[];
  private onFlush?: (event: FlushEvent<T, R>) => void;
  private onError?: (error: BatcherError, batch: T[]) => void;
  private fallback?: (item: T) => R | Promise<R>;

  private disposed = false;

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
    };
    this.flush = options.flush;
    this.onFlush = options.onFlush;
    this.onError = options.onError;
    this.fallback = options.fallback;
  }

  submit(item: T): Promise<R> {
    if (this.disposed) {
      return Promise.reject(
        new BatcherError(BatcherErrorCode.DISPOSED, 'Batcher has been disposed', {
          retryable: false,
        })
      );
    }

    const totalQueued = this.activeBuffer.length + this.inflightCount * this.targetBatchSize;
    if (totalQueued >= this.maxQueueSize) {
      return this.handleOverflow(item);
    }

    return new Promise<R>((resolve, reject) => {
      const now = Date.now();
      this.activeBuffer.push({
        item,
        resolve,
        reject,
        timestamp: now,
      });
      this.recordRequest(now);
      this.tickle();
    });
  }

  private async handleOverflow(item: T): Promise<R> {
    switch (this.overflowStrategy) {
      case 'drop':
        if (this.fallback) {
          return this.fallback(item);
        }
        return new Promise<R>(() => {});
      case 'block': {
        const poll = (): Promise<R> => {
          if (this.disposed) {
            return Promise.reject(
              new BatcherError(BatcherErrorCode.DISPOSED, 'Batcher disposed while waiting for queue capacity', {
                retryable: false,
              })
            );
          }
          const totalQueued = this.activeBuffer.length + this.inflightCount * this.targetBatchSize;
          if (totalQueued < this.maxQueueSize) {
            return this.submit(item);
          }
          return new Promise<R>((res) => setTimeout(() => poll().then(res), this.minWaitMs));
        };
        return poll();
      }
      case 'reject':
      default:
        return Promise.reject(
          new BatcherError(
            BatcherErrorCode.QUEUE_OVERFLOW,
            `Queue overflow: ${this.maxQueueSize} items max`,
            {
              retryable: true,
              data: { item, queueSize: this.activeBuffer.length, inflightCount: this.inflightCount },
            }
          )
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
    while (sw.events.length > 0 && now - sw.events[0].timestamp > sw.windowMs) {
      const evicted = sw.events.shift()!;
      sw.itemsProcessed -= evicted.batchSize;
      sw.batchesProcessed -= 1;
      if (!evicted.success) sw.batchesFailed -= 1;
    }
    this.adaptParameters();
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

    if (this.activeBuffer.length === 0) {
      this.clearTimer();
      if (this.inflightCount === 0 && this.resolveFlushDrain) {
        this.resolveFlushDrain();
        this.flushDrain = null;
        this.resolveFlushDrain = null;
      }
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

  private async doFlush(): Promise<void> {
    if (this.flushLock) return;
    if (this.activeBuffer.length === 0) return;
    if (this.inflightCount >= this.maxInflightBatches) return;

    this.flushLock = true;
    const batch = this.activeBuffer;
    this.activeBuffer = [];
    this.flushLock = false;
    this.inflightCount++;

    const items = batch.map((b) => b.item);
    const queuedAt = batch[0].timestamp;
    const queuedWaitMs = Date.now() - queuedAt;
    const flushStartedAt = Date.now();

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
          this.onError?.(error as BatcherError, items);
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
        this.onError?.(wrapped, items);
      } finally {
        const now = Date.now();
        const flushDurationMs = now - flushStartedAt;
        this.recordFlush(batch.length, queuedWaitMs, flushDurationMs, success, now);

        this.onFlush?.({
          batchSize: batch.length,
          queuedWaitMs,
          flushDurationMs,
          success,
          error,
          results,
          items,
        });

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

  async dispose(): Promise<void> {
    this.disposed = true;
    this.clearTimer();

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
        item.reject(err);
      }
      this.activeBuffer = [];
    }

    await this.drainInflight();
  }

  private async drainInflight(): Promise<void> {
    if (this.inflightPromises.size === 0) return;
    if (this.inflightPromises.size > 0) {
      await Promise.all(Array.from(this.inflightPromises));
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
    const count = sw.events.length;
    if (count === 0) {
      return {
        avgBatchSize: 0,
        avgQueuedWaitMs: 0,
        avgFlushDurationMs: 0,
        failureRate: 0,
        throughputPerSecond: 0,
        itemsProcessed: 0,
        batchesProcessed: 0,
        batchesFailed: 0,
      };
    }
    const avgBatchSize =
      sw.events.reduce((s, e) => s + e.batchSize, 0) / count;
    const avgQueuedWaitMs =
      sw.events.reduce((s, e) => s + e.queuedWaitMs, 0) / count;
    const avgFlushDurationMs =
      sw.events.reduce((s, e) => s + e.flushDurationMs, 0) / count;
    const failureRate =
      sw.events.reduce((s, e) => s + (e.success ? 0 : 1), 0) / count;
    const windowSeconds = sw.windowMs / 1000;
    const throughputPerSecond = sw.itemsProcessed / windowSeconds;

    return {
      avgBatchSize: Math.round(avgBatchSize * 100) / 100,
      avgQueuedWaitMs: Math.round(avgQueuedWaitMs * 100) / 100,
      avgFlushDurationMs: Math.round(avgFlushDurationMs * 100) / 100,
      failureRate: Math.round(failureRate * 10000) / 10000,
      throughputPerSecond: Math.round(throughputPerSecond * 100) / 100,
      itemsProcessed: sw.itemsProcessed,
      batchesProcessed: sw.batchesProcessed,
      batchesFailed: sw.batchesFailed,
    };
  }

  get mode(): 'low-latency' | 'balanced' | 'high-throughput' {
    const stats = this.rollingStats;
    if (stats.avgFlushDurationMs > 150 || stats.failureRate > 0.2) {
      return 'low-latency';
    }
    if (this.ewmaRate > 150 && stats.avgBatchSize > this.maxBatchSize * 0.4) {
      return 'high-throughput';
    }
    return 'balanced';
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
