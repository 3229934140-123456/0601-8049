export interface BatcherOptions<T, R> {
  minBatchSize?: number;
  maxBatchSize?: number;
  initialBatchSize?: number;
  minWaitMs?: number;
  maxWaitMs?: number;
  initialWaitMs?: number;
  rateWindowMs?: number;
  maxInflightBatches?: number;
  flush: (batch: T[]) => Promise<R[]> | R[];
  onError?: (error: unknown, batch: T[]) => void;
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

export class AdaptiveBatcher<T, R> {
  private minBatchSize: number;
  private maxBatchSize: number;
  private targetBatchSize: number;
  private minWaitMs: number;
  private maxWaitMs: number;
  private waitWindowMs: number;
  private maxInflightBatches: number;

  private activeBuffer: PendingItem<T, R>[] = [];
  private flushLock = false;
  private flushTimer: ReturnType<typeof setTimeout> | null = null;
  private inflightCount = 0;

  private rateTracker: RateTracker;
  private ewmaRate = 0;
  private readonly alpha = 0.3;

  private flush: (batch: T[]) => Promise<R[]> | R[];
  private onError?: (error: unknown, batch: T[]) => void;

  private disposed = false;

  constructor(options: BatcherOptions<T, R>) {
    this.minBatchSize = options.minBatchSize ?? 1;
    this.maxBatchSize = options.maxBatchSize ?? 500;
    this.targetBatchSize = options.initialBatchSize ?? Math.max(10, this.minBatchSize);
    this.minWaitMs = options.minWaitMs ?? 2;
    this.maxWaitMs = options.maxWaitMs ?? 200;
    this.waitWindowMs = options.initialWaitMs ?? 50;
    this.maxInflightBatches = options.maxInflightBatches ?? 4;
    this.rateTracker = {
      timestamps: [],
      windowMs: options.rateWindowMs ?? 1000,
    };
    this.flush = options.flush;
    this.onError = options.onError;
  }

  submit(item: T): Promise<R> {
    if (this.disposed) {
      return Promise.reject(new Error('Batcher has been disposed'));
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

  private adaptParameters(): void {
    const rate = this.ewmaRate;
    if (rate < 10) {
      this.targetBatchSize = this.clamp(
        Math.round(this.minBatchSize + rate * 0.5),
        this.minBatchSize,
        this.maxBatchSize
      );
      this.waitWindowMs = this.lerp(this.minWaitMs, this.maxWaitMs * 0.2, rate / 10);
    } else if (rate < 200) {
      const t = (rate - 10) / 190;
      this.targetBatchSize = this.clamp(
        Math.round(this.lerp(this.minBatchSize + 5, this.maxBatchSize * 0.6, t)),
        this.minBatchSize,
        this.maxBatchSize
      );
      this.waitWindowMs = this.lerp(this.maxWaitMs * 0.2, this.maxWaitMs * 0.7, t);
    } else {
      const t = Math.min(1, (rate - 200) / 800);
      this.targetBatchSize = this.clamp(
        Math.round(this.lerp(this.maxBatchSize * 0.6, this.maxBatchSize, t)),
        this.minBatchSize,
        this.maxBatchSize
      );
      this.waitWindowMs = this.lerp(this.maxWaitMs * 0.7, this.maxWaitMs, t);
    }
  }

  private tickle(): void {
    if (this.flushLock) return;

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
      const safeTimeout = Math.min(remaining, this.maxWaitMs - elapsed, this.minWaitMs * 10);
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

    try {
      const results = await this.flush(items);
      if (results.length !== batch.length) {
        const err = new Error(
          `Flush returned ${results.length} results for ${batch.length} items`
        );
        batch.forEach(({ reject }) => reject(err));
        this.onError?.(err, items);
      } else {
        batch.forEach(({ resolve }, i) => resolve(results[i]));
      }
    } catch (error) {
      batch.forEach(({ reject }) => reject(error));
      this.onError?.(error, items);
    } finally {
      this.inflightCount--;
      this.tickle();
    }
  }

  async forceFlush(): Promise<void> {
    while (this.activeBuffer.length > 0) {
      await this.doFlush();
    }
  }

  async dispose(): Promise<void> {
    this.disposed = true;
    this.clearTimer();
    await this.forceFlush();
  }

  get metrics() {
    return {
      targetBatchSize: this.targetBatchSize,
      waitWindowMs: this.waitWindowMs,
      currentQueueSize: this.activeBuffer.length,
      inflightBatches: this.inflightCount,
      ewmaRate: Math.round(this.ewmaRate * 100) / 100,
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
