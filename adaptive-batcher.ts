// ============================================================
// Adaptive Batcher v5 — Production-ready
// 支持: 自适应攒批、双缓冲无阻塞、背压、部分失败重试、
//       取消/超时(含flushing中)、按key分区、健康快照、
//       请求追踪、按key熔断限流
// ============================================================

export enum BatcherErrorCode {
  DISPOSED = 'BATCHER_DISPOSED',
  QUEUE_OVERFLOW = 'QUEUE_OVERFLOW',
  FLUSH_FAILED = 'FLUSH_FAILED',
  MISMATCHED_RESULTS = 'MISMATCHED_RESULTS',
  CANCELED = 'CANCELED',
  TIMEOUT = 'TIMEOUT',
  DROPPED = 'DROPPED',
  PERMANENT_FAILURE = 'PERMANENT_FAILURE',
  RETRY_EXHAUSTED = 'RETRY_EXHAUSTED',
  CIRCUIT_OPEN = 'CIRCUIT_OPEN',
}

export class BatcherError extends Error {
  readonly code: BatcherErrorCode;
  readonly retryable: boolean;
  readonly data?: unknown;
  readonly trace?: SubmitTrace;

  constructor(
    code: BatcherErrorCode,
    message: string,
    options: { retryable?: boolean; data?: unknown; cause?: unknown; trace?: SubmitTrace } = {}
  ) {
    super(message);
    this.name = 'BatcherError';
    this.code = code;
    this.retryable = options.retryable ?? true;
    this.data = options.data;
    this.trace = options.trace;
    if (options.cause) (this as unknown as { cause: unknown }).cause = options.cause;
  }
}

export class RetryableFailure extends Error {
  readonly retryable = true as const;
  constructor(message: string, public readonly cause?: unknown) {
    super(message);
    this.name = 'RetryableFailure';
  }
}

export class PermanentFailure extends Error {
  readonly permanent = true as const;
  constructor(message: string, public readonly cause?: unknown) {
    super(message);
    this.name = 'PermanentFailure';
  }
}

export type OverflowStrategy = 'reject' | 'drop' | 'block';
export type DisposeStrategy = 'drain' | 'reject' | 'kill';

export interface SubmitTrace {
  readonly requestId?: string;
  readonly tags?: readonly string[];
  readonly [key: string]: unknown;
}

export interface SubmitOptions {
  timeoutMs?: number;
  signal?: AbortSignal;
  key?: string;
  trace?: SubmitTrace;
}

export interface FlushEvent<T, R = unknown> {
  readonly batchSize: number;
  readonly queuedWaitMs: number;
  readonly flushDurationMs: number;
  readonly success: boolean;
  readonly error?: unknown;
  readonly results?: readonly R[];
  readonly items: readonly T[];
  readonly traces: readonly (SubmitTrace | undefined)[];
  readonly timestamp: number;
  readonly attempt: number;
  readonly key?: string;
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
  readonly lastError: string | null;
  readonly overflowCount: number;
  readonly circuitState: CircuitBreakerState;
}

export type CircuitBreakerState = 'normal' | 'degraded' | 'circuit-open' | 'half-open';

export interface CircuitBreakerOptions {
  enabled?: boolean;
  failureThreshold?: number;
  slowDurationThresholdMs?: number;
  degradationRatio?: number;
  circuitOpenAfterDegradedMs?: number;
  halfOpenAfterMs?: number;
  halfOpenBatchSize?: number;
  recoveryStepRatio?: number;
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
  maxRetries?: number;
  retryDelayMs?: number;
  retryBackoffMultiplier?: number;
  circuitBreaker?: CircuitBreakerOptions;
  flush: (batch: T[], attempt: number) => Promise<R[]> | R[];
  onFlush?: (event: FlushEvent<T, R>) => void | Promise<void>;
  onError?: (error: BatcherError, batch: T[]) => void | Promise<void>;
  onCircuitStateChange?: (state: CircuitBreakerState, prevState: CircuitBreakerState) => void | Promise<void>;
  fallback?: (item: T) => R | Promise<R>;
}

interface PendingItem<T, R> {
  item: T;
  resolve: (result: R) => void;
  reject: (reason: unknown) => void;
  timestamp: number;
  canceled: boolean;
  trace?: SubmitTrace;
  timeoutTimer?: ReturnType<typeof setTimeout>;
  abortListener?: () => void;
  flushingTimer?: ReturnType<typeof setTimeout>;
  deadline?: number;
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
  lastError: string | null;
  overflowCount: number;
}

// ============================================================
// Single-key AdaptiveBatcher
// ============================================================
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
  private maxRetries: number;
  private retryDelayMs: number;
  private retryBackoffMultiplier: number;

  private activeBuffer: PendingItem<T, R>[] = [];
  private flushLock = false;
  private flushTimer: ReturnType<typeof setTimeout> | null = null;
  private inflightCount = 0;
  private inflightPromises: Set<Promise<void>> = new Set();
  private inflightItems = new Set<PendingItem<T, R>>();

  private rateTracker: RateTracker;
  private ewmaRate = 0;
  private ewmaFlushDuration = 0;
  private ewmaFailureRate = 0;
  private readonly alpha = 0.3;

  private statsWindow: StatsWindow;

  private flush: (batch: T[], attempt: number) => Promise<R[]> | R[];
  private onFlush?: (event: FlushEvent<T, R>) => void | Promise<void>;
  private onError?: (error: BatcherError, batch: T[]) => void | Promise<void>;
  private onCircuitStateChange?: (state: CircuitBreakerState, prevState: CircuitBreakerState) => void | Promise<void>;
  private fallback?: (item: T) => R | Promise<R>;

  private disposed = false;
  private disposing = false;
  private killSwitch = false;
  private killError?: BatcherError;
  private batcherKey?: string;

  // ---- 熔断限流 ----
  private circuitBreakerEnabled: boolean;
  private circuitBreakerState: CircuitBreakerState = 'normal';
  private degradedAt: number | null = null;
  private circuitOpenedAt: number | null = null;
  private halfOpenAttempts = 0;
  private consecutiveSuccesses = 0;
  private baseMaxInflightBatches: number;
  private baseTargetBatchSize: number;
  private circuitBreakerConfig: Required<CircuitBreakerOptions>;

  constructor(options: BatcherOptions<T, R> & { key?: string } = {} as BatcherOptions<T, R>) {
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
    this.maxRetries = options.maxRetries ?? 0;
    this.retryDelayMs = options.retryDelayMs ?? 10;
    this.retryBackoffMultiplier = options.retryBackoffMultiplier ?? 2;
    this.batcherKey = (options as { key?: string }).key;

    // 熔断配置初始化
    const cb = options.circuitBreaker ?? {};
    this.circuitBreakerEnabled = cb.enabled ?? true;
    this.circuitBreakerConfig = {
      enabled: cb.enabled ?? true,
      failureThreshold: cb.failureThreshold ?? 0.3,
      slowDurationThresholdMs: cb.slowDurationThresholdMs ?? 500,
      degradationRatio: cb.degradationRatio ?? 0.5,
      circuitOpenAfterDegradedMs: cb.circuitOpenAfterDegradedMs ?? 30_000,
      halfOpenAfterMs: cb.halfOpenAfterMs ?? 10_000,
      halfOpenBatchSize: cb.halfOpenBatchSize ?? 1,
      recoveryStepRatio: cb.recoveryStepRatio ?? 0.2,
    };
    this.baseMaxInflightBatches = this.maxInflightBatches;
    this.baseTargetBatchSize = this.targetBatchSize;

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
      lastError: null,
      overflowCount: 0,
    };
    this.flush = options.flush;
    this.onFlush = options.onFlush;
    this.onError = options.onError;
    this.onCircuitStateChange = options.onCircuitStateChange;
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
          trace: options.trace,
        })
      );
    }

    // 熔断检查
    if (this.circuitBreakerEnabled) {
      const now = Date.now();

      // degraded 状态下检查是否需要转换到 circuit-open
      if (this.circuitBreakerState === 'degraded' && this.degradedAt != null) {
        if (now - this.degradedAt >= this.circuitBreakerConfig.circuitOpenAfterDegradedMs) {
          this.transitionCircuitState('circuit-open');
        }
      }

      // circuit-open 状态检查
      if (this.circuitBreakerState === 'circuit-open') {
        if (now - (this.circuitOpenedAt ?? 0) >= this.circuitBreakerConfig.halfOpenAfterMs) {
          this.transitionCircuitState('half-open');
        } else {
          return Promise.reject(
            new BatcherError(
              BatcherErrorCode.CIRCUIT_OPEN,
              `Circuit breaker is open for key ${this.batcherKey ?? 'default'}, try again later`,
              { retryable: true, trace: options.trace }
            )
          );
        }
      }
    }

    const totalQueued = this.activeBuffer.length + this.inflightCount * this.targetBatchSize;
    if (totalQueued >= this.maxQueueSize) {
      this.statsWindow.overflowCount++;
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
        trace: options.trace,
      };

      if (options.timeoutMs != null && options.timeoutMs > 0) {
        pendingItem.deadline = now + options.timeoutMs;
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
    if (item.flushingTimer) {
      clearTimeout(item.flushingTimer);
      item.flushingTimer = undefined;
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

    const err = new BatcherError(code, message, { retryable: code === BatcherErrorCode.TIMEOUT, trace: item.trace });
    item.reject(err);

    // 记录到统计，反映这次失败
    if (code === BatcherErrorCode.TIMEOUT || code === BatcherErrorCode.CANCELED) {
      const now = Date.now();
      const queuedWaitMs = now - item.timestamp;
      this.recordFlush(1, queuedWaitMs, 0, false, err, now);
    }
  }

  private handleOverflow(item: T, options: SubmitOptions): Promise<R> {
    const trace = options.trace;
    switch (this.overflowStrategy) {
      case 'drop': {
        if (this.fallback) {
          try {
            const result = this.fallback(item);
            return Promise.resolve(result);
          } catch (e) {
            return Promise.reject(
              new BatcherError(
                BatcherErrorCode.DROPPED,
                'Item dropped due to queue overflow, fallback failed',
                { retryable: true, cause: e, trace }
              )
            );
          }
        }
        return Promise.reject(
          new BatcherError(
            BatcherErrorCode.DROPPED,
            'Item dropped due to queue overflow (no fallback configured)',
            { retryable: true, data: { item }, trace }
          )
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
                { retryable: false, trace }
              )
            );
          }
          if (signal?.aborted) {
            return Promise.reject(
              new BatcherError(BatcherErrorCode.CANCELED, 'Submit canceled while waiting for capacity', {
                retryable: false,
                trace,
              })
            );
          }
          if (timeoutAt != null && Date.now() >= timeoutAt) {
            return Promise.reject(
              new BatcherError(BatcherErrorCode.TIMEOUT, 'Timed out waiting for queue capacity', {
                retryable: true,
                trace,
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
        return Promise.reject(
          new BatcherError(
            BatcherErrorCode.QUEUE_OVERFLOW,
            `Queue overflow: ${this.maxQueueSize} items max`,
            {
              retryable: true,
              data: { item, queueSize: this.activeBuffer.length, inflightCount: this.inflightCount },
              trace,
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
    error: unknown,
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
    if (error instanceof Error) {
      sw.lastError = error.message;
    } else if (typeof error === 'string') {
      sw.lastError = error;
    }
    while (sw.events.length > 0 && now - sw.events[0].timestamp > sw.windowMs) {
      const evicted = sw.events.shift()!;
      sw.itemsProcessed -= evicted.batchSize;
      sw.batchesProcessed -= 1;
      if (!evicted.success) sw.batchesFailed -= 1;
    }
    this.adaptParameters();
    this.evaluateCircuitBreaker(success, flushDurationMs, now);
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

  // ---- 熔断限流 ----
  private transitionCircuitState(newState: CircuitBreakerState): void {
    if (!this.circuitBreakerEnabled) return;
    const prevState = this.circuitBreakerState;
    if (newState === prevState) return;

    this.circuitBreakerState = newState;

    if (newState === 'degraded') {
      this.degradedAt = Date.now();
      this.circuitOpenedAt = null;
      this.applyDegradation();
    } else if (newState === 'circuit-open') {
      this.circuitOpenedAt = Date.now();
      this.degradedAt = null;
    } else if (newState === 'half-open') {
      this.halfOpenAttempts = 0;
      this.consecutiveSuccesses = 0;
      this.circuitOpenedAt = null;
      this.degradedAt = null;
      // half-open 状态下用小批次探测
      this.maxInflightBatches = 1;
      this.targetBatchSize = Math.max(this.minBatchSize, this.circuitBreakerConfig.halfOpenBatchSize);
    } else if (newState === 'normal') {
      this.degradedAt = null;
      this.circuitOpenedAt = null;
      this.consecutiveSuccesses = 0;
      this.maxInflightBatches = this.baseMaxInflightBatches;
      this.targetBatchSize = this.baseTargetBatchSize;
    }

    this.safeInvokeObserver(
      () => this.onCircuitStateChange?.(newState, prevState),
      'onCircuitStateChange'
    );
  }

  private applyDegradation(): void {
    const ratio = this.circuitBreakerConfig.degradationRatio;
    this.maxInflightBatches = Math.max(1, Math.floor(this.baseMaxInflightBatches * ratio));
    this.targetBatchSize = Math.max(
      this.minBatchSize,
      Math.floor(this.baseTargetBatchSize * ratio)
    );
  }

  private applyRecoveryStep(): void {
    const ratio = this.circuitBreakerConfig.recoveryStepRatio;
    const targetInflight = Math.min(
      this.baseMaxInflightBatches,
      Math.ceil(this.maxInflightBatches * (1 + ratio))
    );
    const targetBatch = Math.min(
      this.baseTargetBatchSize,
      Math.ceil(this.targetBatchSize * (1 + ratio))
    );
    this.maxInflightBatches = targetInflight;
    this.targetBatchSize = targetBatch;
  }

  private evaluateCircuitBreaker(success: boolean, flushDurationMs: number, now: number): void {
    if (!this.circuitBreakerEnabled) return;

    const state = this.circuitBreakerState;
    const cfg = this.circuitBreakerConfig;
    const isSlow = flushDurationMs > cfg.slowDurationThresholdMs;
    const isFailing = !success || this.ewmaFailureRate > cfg.failureThreshold;

    if (state === 'normal') {
      if (isFailing || isSlow) {
        this.consecutiveSuccesses = 0;
        this.transitionCircuitState('degraded');
      } else {
        this.consecutiveSuccesses++;
      }
    } else if (state === 'degraded') {
      if (isFailing || isSlow) {
        this.consecutiveSuccesses = 0;
        // degraded 持续时间超过阈值，进入 circuit-open
        if (this.degradedAt != null && now - this.degradedAt >= cfg.circuitOpenAfterDegradedMs) {
          this.transitionCircuitState('circuit-open');
        }
      } else {
        this.consecutiveSuccesses++;
        // 连续成功多次后逐步恢复
        if (this.consecutiveSuccesses >= 3) {
          this.applyRecoveryStep();
          // 如果已恢复到接近基准值，回到 normal
          if (
            this.maxInflightBatches >= this.baseMaxInflightBatches * 0.9 &&
            this.targetBatchSize >= this.baseTargetBatchSize * 0.9
          ) {
            this.transitionCircuitState('normal');
          }
        }
      }
    } else if (state === 'half-open') {
      this.halfOpenAttempts++;
      if (success && !isSlow) {
        this.consecutiveSuccesses++;
        // 探测成功，逐步恢复
        this.applyRecoveryStep();
        if (this.consecutiveSuccesses >= 2) {
          // 连续成功，回到 normal
          this.transitionCircuitState('normal');
        }
      } else {
        // 探测失败，回到 circuit-open
        this.transitionCircuitState('circuit-open');
      }
    }
  }

  get circuitState(): CircuitBreakerState {
    return this.circuitBreakerState;
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

  private safeInvokeObserver<X>(
    fn: (() => X | Promise<X>) | undefined,
    label: string
  ): void {
    if (!fn) return;
    try {
      const result = fn();
      if (result && typeof (result as Promise<X>).then === 'function') {
        (result as Promise<X>).catch((e) => {
          console.warn(`[AdaptiveBatcher] ${label} observer async error:`, e);
        });
      }
    } catch (e) {
      console.warn(`[AdaptiveBatcher] ${label} observer sync error:`, e);
    }
  }

  // ---- 核心 flush：支持部分失败 + 重试 + flushing 中可取消 ----
  private doFlush(): void {
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

      if (it.deadline != null && !it.canceled) {
        const remaining = it.deadline - Date.now();
        if (remaining <= 0) {
          this.rejectItem(it, BatcherErrorCode.TIMEOUT, `Submit timed out (deadline passed before flush)`);
          continue;
        }
        it.flushingTimer = setTimeout(() => {
          this.rejectItem(it, BatcherErrorCode.TIMEOUT, `Submit timed out during flush after ${it.deadline! - it.timestamp}ms`);
        }, remaining);
      }
    }

    const aliveBatch = batch.filter((it) => !it.canceled);
    if (aliveBatch.length === 0) {
      this.inflightCount--;
      this.tickle();
      return;
    }

    for (const it of aliveBatch) {
      this.inflightItems.add(it);
    }

    const inflightPromise = this.runFlushWithRetry(aliveBatch, queuedWaitMs, flushStartedAt)
      .finally(() => {
        for (const it of aliveBatch) {
          this.inflightItems.delete(it);
        }
        this.inflightPromises.delete(inflightPromise);
        this.inflightCount--;
        this.tickle();
      });
    this.inflightPromises.add(inflightPromise);
  }

  private rejectItem(item: PendingItem<T, R>, code: BatcherErrorCode, message: string): void {
    if (item.canceled) return;
    item.canceled = true;
    if (item.flushingTimer) {
      clearTimeout(item.flushingTimer);
      item.flushingTimer = undefined;
    }
    if (item.timeoutTimer) {
      clearTimeout(item.timeoutTimer);
      item.timeoutTimer = undefined;
    }
    const err = new BatcherError(code, message, {
      retryable: code === BatcherErrorCode.TIMEOUT,
      trace: item.trace,
    });
    item.reject(err);

    // flushing 过程中被取消/超时，也要记录到统计
    if (code === BatcherErrorCode.TIMEOUT || code === BatcherErrorCode.CANCELED) {
      const now = Date.now();
      const queuedWaitMs = now - item.timestamp;
      this.recordFlush(1, queuedWaitMs, 0, false, err, now);
    }
  }

  private async runFlushWithRetry(
    batch: PendingItem<T, R>[],
    queuedWaitMs: number,
    flushStartedAt: number
  ): Promise<void> {
    let attempt = 0;
    let remainingItems = batch;
    let lastError: unknown;
    let totalSucceeded = 0;
    let hasAnyFailure = false;

    const totalAttempts = this.maxRetries + 1;

    while (attempt < totalAttempts && remainingItems.length > 0 && !this.killSwitch) {
      attempt++;
      const items = remainingItems.map((b) => b.item);

      if (attempt > 1) {
        const delay = this.retryDelayMs * Math.pow(this.retryBackoffMultiplier, attempt - 2);
        await this.sleep(delay);
        if (this.killSwitch) break;
      }

      const alive = remainingItems.filter((it) => !it.canceled);
      if (alive.length === 0) {
        remainingItems = [];
        break;
      }
      if (alive.length !== remainingItems.length) {
        remainingItems = alive;
        continue;
      }

      const attemptStart = Date.now();
      const aliveItems = remainingItems.map((b) => b.item);

      try {
        const rawResults = await this.flush(aliveItems, attempt);

        const results = this.normalizeResults(rawResults, aliveItems.length);
        const successCount = results.filter((r) => 'ok' in r && r.ok).length;

        if (successCount === results.length) {
          for (let i = 0; i < remainingItems.length; i++) {
            const item = remainingItems[i];
            const result = results[i];
            if (item.canceled) continue;
            if (item.flushingTimer) {
              clearTimeout(item.flushingTimer);
              item.flushingTimer = undefined;
            }
            if ('ok' in result && result.ok) {
              item.resolve(result.value as R);
              totalSucceeded++;
            }
          }
          lastError = undefined;
          remainingItems = [];
          break;
        }

        const stillPending: PendingItem<T, R>[] = [];

        for (let i = 0; i < results.length; i++) {
          const r = results[i];
          const item = remainingItems[i];
          if (item.canceled) continue;

          if ('ok' in r && r.ok) {
            if (item.flushingTimer) {
              clearTimeout(item.flushingTimer);
              item.flushingTimer = undefined;
            }
            item.resolve(r.value as R);
            totalSucceeded++;
          } else if ('permanent' in r && r.permanent) {
            if (item.flushingTimer) {
              clearTimeout(item.flushingTimer);
              item.flushingTimer = undefined;
            }
            const err = new BatcherError(
              BatcherErrorCode.PERMANENT_FAILURE,
              `Permanent failure: ${r.errorMessage}`,
              { retryable: false, cause: r.error, trace: item.trace }
            );
            item.reject(err);
            hasAnyFailure = true;
          } else if ('retryable' in r && r.retryable) {
            if (attempt >= totalAttempts) {
              if (item.flushingTimer) {
                clearTimeout(item.flushingTimer);
                item.flushingTimer = undefined;
              }
              const err = new BatcherError(
                BatcherErrorCode.RETRY_EXHAUSTED,
                `Retry exhausted after ${attempt} attempts: ${r.errorMessage}`,
                { retryable: true, cause: r.error, trace: item.trace }
              );
              item.reject(err);
              hasAnyFailure = true;
            } else {
              stillPending.push(item);
            }
          }
        }

        remainingItems = stillPending;
        if (stillPending.length > 0) {
          const failResult = results.find((r) => !('ok' in r) && 'error' in r);
          lastError = failResult && 'error' in failResult ? failResult.error : undefined;
        } else {
          const firstFail = results.find((r) => !('ok' in r));
          if (firstFail && 'error' in firstFail) {
            lastError = firstFail.error;
          }
        }
      } catch (caught) {
        lastError = caught;
        if (attempt >= totalAttempts) {
          const baseErr = new BatcherError(
            BatcherErrorCode.FLUSH_FAILED,
            `Flush failed after ${attempt} attempts: ${(caught as Error).message}`,
            { cause: caught, retryable: true }
          );
          for (const item of remainingItems) {
            if (item.canceled) continue;
            if (item.flushingTimer) {
              clearTimeout(item.flushingTimer);
              item.flushingTimer = undefined;
            }
            if (item.trace) {
              const errWithTrace = new BatcherError(baseErr.code, baseErr.message, {
                retryable: baseErr.retryable,
                cause: baseErr.cause,
                trace: item.trace,
              });
              item.reject(errWithTrace);
            } else {
              item.reject(baseErr);
            }
          }
          hasAnyFailure = true;
          this.safeInvokeObserver(
            () => this.onError?.(baseErr, items),
            'onError'
          );
          remainingItems = [];
        }
      }
    }

    if (this.killSwitch && remainingItems.length > 0 && this.killError) {
      for (const item of remainingItems) {
        if (item.canceled) continue;
        if (item.flushingTimer) {
          clearTimeout(item.flushingTimer);
          item.flushingTimer = undefined;
        }
        if (item.trace) {
          const errWithTrace = new BatcherError(this.killError.code, this.killError.message, {
            retryable: this.killError.retryable,
            trace: item.trace,
          });
          item.reject(errWithTrace);
        } else {
          item.reject(this.killError);
        }
      }
      remainingItems = [];
    }

    const now = Date.now();
    const totalDurationMs = now - flushStartedAt;
    const batchSize = batch.length;
    const aliveCount = batch.filter((it) => !it.canceled).length;
    const success = !hasAnyFailure && !this.killSwitch && totalSucceeded === aliveCount;

    this.recordFlush(
      batchSize,
      queuedWaitMs,
      totalDurationMs,
      success,
      lastError,
      now
    );

    this.safeInvokeObserver(
      () =>
        this.onFlush?.({
          batchSize,
          queuedWaitMs,
          flushDurationMs: totalDurationMs,
          success,
          error: lastError,
          items: batch.map((b) => b.item),
          traces: batch.map((b) => b.trace),
          timestamp: now,
          attempt,
          key: this.batcherKey,
        }),
      'onFlush'
    );
  }

  private normalizeResults(
    raw: R[] | unknown[],
    expectedLength: number
  ): Array<
    | { ok: true; value: R }
    | { retryable: true; error: unknown; errorMessage: string }
    | { permanent: true; error: unknown; errorMessage: string }
  > {
    if (!Array.isArray(raw)) {
      return Array(expectedLength).fill(null).map(() => ({
        retryable: true,
        error: new Error('Flush returned non-array result'),
        errorMessage: 'Flush returned non-array result',
      }));
    }

    if (raw.length !== expectedLength) {
      return Array(expectedLength).fill(null).map(() => ({
        retryable: true,
        error: new BatcherError(
          BatcherErrorCode.MISMATCHED_RESULTS,
          `Flush returned ${raw.length} results for ${expectedLength} items`,
          { retryable: true }
        ),
        errorMessage: `Result count mismatch (${raw.length}/${expectedLength})`,
      }));
    }

    return raw.map((r) => {
      if (r instanceof PermanentFailure) {
        return { permanent: true, error: r.cause ?? r, errorMessage: r.message } as const;
      }
      if (r instanceof RetryableFailure) {
        return { retryable: true, error: r.cause ?? r, errorMessage: r.message } as const;
      }
      if (r instanceof Error) {
        return { retryable: true, error: r, errorMessage: r.message } as const;
      }
      return { ok: true, value: r as R } as const;
    });
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((r) => setTimeout(r, ms));
  }

  // ---- Public API ----

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
      this.killSwitch = true;
      this.killError = new BatcherError(
        BatcherErrorCode.DISPOSED,
        'Batcher killed: all pending requests rejected immediately',
        { retryable: true }
      );

      for (const item of this.activeBuffer) {
        if (item.timeoutTimer) clearTimeout(item.timeoutTimer);
        if (item.flushingTimer) clearTimeout(item.flushingTimer);
        if (item.abortListener) {
          try {
            item.abortListener = undefined;
          } catch {}
        }
        if (!item.canceled) {
          if (item.trace) {
            const errWithTrace = new BatcherError(this.killError.code, this.killError.message, {
              retryable: this.killError.retryable,
              trace: item.trace,
            });
            item.reject(errWithTrace);
          } else {
            item.reject(this.killError);
          }
        }
      }
      this.activeBuffer = [];

      for (const item of this.inflightItems) {
        if (!item.canceled) {
          if (item.flushingTimer) clearTimeout(item.flushingTimer);
          if (item.trace) {
            const errWithTrace = new BatcherError(this.killError.code, this.killError.message, {
              retryable: this.killError.retryable,
              trace: item.trace,
            });
            item.reject(errWithTrace);
          } else {
            item.reject(this.killError);
          }
          item.canceled = true;
        }
      }

      this.disposed = true;
      return;
    }

    if (strategy === 'reject') {
      const baseErr = new BatcherError(
        BatcherErrorCode.DISPOSED,
        'Batcher disposed: queued requests rejected, inflight continuing',
        { retryable: true }
      );
      for (const item of this.activeBuffer) {
        if (item.timeoutTimer) clearTimeout(item.timeoutTimer);
        if (item.flushingTimer) clearTimeout(item.flushingTimer);
        if (item.abortListener) {
          try {
            item.abortListener = undefined;
          } catch {}
        }
        if (!item.canceled) {
          if (item.trace) {
            const errWithTrace = new BatcherError(baseErr.code, baseErr.message, {
              retryable: baseErr.retryable,
              trace: item.trace,
            });
            item.reject(errWithTrace);
          } else {
            item.reject(baseErr);
          }
        }
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
        if (item.flushingTimer) clearTimeout(item.flushingTimer);
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
      maxRetries: this.maxRetries,
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
      lastError: this.statsWindow.lastError,
      overflowCount: this.statsWindow.overflowCount,
      circuitState: this.circuitBreakerState,
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

  get isDisposed(): boolean {
    return this.disposed;
  }
}

// ============================================================
// Partitioned Adaptive Batcher — 按 key 分区攒批
// ============================================================
export interface PartitionedBatcherOptions<T, R> {
  defaultKey?: string;
  maxKeys?: number;
  maxTotalQueueSize?: number;
  perKeyMaxQueueSize?: number;
  perKeyMinBatchSize?: number;
  perKeyMaxBatchSize?: number;
  perKeyInitialBatchSize?: number;
  perKeyMinWaitMs?: number;
  perKeyMaxWaitMs?: number;
  perKeyInitialWaitMs?: number;
  perKeyMaxInflightBatches?: number;
  rateWindowMs?: number;
  statsWindowMs?: number;
  overflowStrategy?: OverflowStrategy;
  adaptToFlushDuration?: boolean;
  adaptToFailureRate?: boolean;
  maxRetries?: number;
  retryDelayMs?: number;
  retryBackoffMultiplier?: number;
  circuitBreaker?: CircuitBreakerOptions;
  flush: (key: string, batch: T[], attempt: number) => Promise<R[]> | R[];
  onFlush?: (key: string, event: FlushEvent<T, R>) => void | Promise<void>;
  onError?: (key: string, error: BatcherError, batch: T[]) => void | Promise<void>;
  onCircuitStateChange?: (key: string, state: CircuitBreakerState, prevState: CircuitBreakerState) => void | Promise<void>;
  perKeyFallback?: (key: string, item: T) => R | Promise<R>;
  keyLabel?: (key: string) => string;
}

export interface KeyStats {
  readonly key: string;
  readonly queueSize: number;
  readonly inflightBatches: number;
  readonly capacityUsedPercent: number;
  readonly ewmaRate: number;
  readonly ewmaFlushDurationMs: number;
  readonly ewmaFailureRate: number;
  readonly targetBatchSize: number;
  readonly waitWindowMs: number;
  readonly health: HealthStatus;
  readonly mode: 'low-latency' | 'balanced' | 'high-throughput' | 'idle';
  readonly lastError: string | null;
  readonly overflowCount: number;
  readonly circuitState: CircuitBreakerState;
}

export interface PartitionedSnapshot {
  readonly timestamp: number;
  readonly totalKeys: number;
  readonly totalQueueSize: number;
  readonly totalInflightBatches: number;
  readonly totalCapacityUsedPercent: number;
  readonly globalHealth: 'healthy' | 'degraded' | 'unhealthy' | 'stale';
  readonly topKeysByQueue: KeyStats[];
  readonly topKeysByFailure: KeyStats[];
  readonly perKey: Record<string, KeyStats>;
}

export class PartitionedAdaptiveBatcher<T, R> {
  private batchers = new Map<string, AdaptiveBatcher<T, R>>();
  private defaultKey: string;
  private maxKeys: number;
  private maxTotalQueueSize: number;
  private perKeyMaxQueueSize: number;
  private perKeyOptions: PartitionedBatcherOptions<T, R>;
  private disposed = false;
  private snapshotExportTimer: ReturnType<typeof setInterval> | null = null;
  private snapshotExportCallback: ((snapshot: PartitionedSnapshot) => void | Promise<void>) | null = null;
  private snapshotExportTopN = 10;

  constructor(options: PartitionedBatcherOptions<T, R>) {
    this.defaultKey = options.defaultKey ?? '__default__';
    this.maxKeys = options.maxKeys ?? 1000;
    this.maxTotalQueueSize = options.maxTotalQueueSize ?? 50_000;
    this.perKeyMaxQueueSize = options.perKeyMaxQueueSize ?? 10_000;
    this.perKeyOptions = options;
  }

  private getBatcher(key: string): AdaptiveBatcher<T, R> {
    let batcher = this.batchers.get(key);
    if (!batcher) {
      if (this.batchers.size >= this.maxKeys) {
        throw new BatcherError(
          BatcherErrorCode.QUEUE_OVERFLOW,
          `Too many distinct keys: max ${this.maxKeys} allowed`,
          { retryable: true }
        );
      }
      batcher = this.createBatcher(key);
      this.batchers.set(key, batcher);
    }
    return batcher;
  }

  private createBatcher(key: string): AdaptiveBatcher<T, R> {
    const opts = this.perKeyOptions;
    return new AdaptiveBatcher<T, R>({
      minBatchSize: opts.perKeyMinBatchSize,
      maxBatchSize: opts.perKeyMaxBatchSize,
      initialBatchSize: opts.perKeyInitialBatchSize,
      minWaitMs: opts.perKeyMinWaitMs,
      maxWaitMs: opts.perKeyMaxWaitMs,
      initialWaitMs: opts.perKeyInitialWaitMs,
      rateWindowMs: opts.rateWindowMs,
      maxInflightBatches: opts.perKeyMaxInflightBatches,
      maxQueueSize: this.perKeyMaxQueueSize,
      overflowStrategy: opts.overflowStrategy,
      statsWindowMs: opts.statsWindowMs,
      adaptToFlushDuration: opts.adaptToFlushDuration,
      adaptToFailureRate: opts.adaptToFailureRate,
      maxRetries: opts.maxRetries,
      retryDelayMs: opts.retryDelayMs,
      retryBackoffMultiplier: opts.retryBackoffMultiplier,
      circuitBreaker: opts.circuitBreaker,
      key,
      flush: (batch, attempt) => opts.flush(key, batch, attempt),
      onFlush: opts.onFlush ? (event) => opts.onFlush!(key, event) : undefined,
      onError: opts.onError ? (err, batch) => opts.onError!(key, err, batch) : undefined,
      onCircuitStateChange: opts.onCircuitStateChange
        ? (state, prev) => opts.onCircuitStateChange!(key, state, prev)
        : undefined,
      fallback: opts.perKeyFallback ? (item) => opts.perKeyFallback!(key, item) : undefined,
    } as BatcherOptions<T, R> & { key: string });
  }

  submit(item: T, options: SubmitOptions = {}): Promise<R> {
    if (this.disposed) {
      return Promise.reject(
        new BatcherError(BatcherErrorCode.DISPOSED, 'PartitionedBatcher has been disposed', {
          retryable: false,
        })
      );
    }
    const key = options.key ?? this.defaultKey;

    const totalQueued = this.totalQueueSize;
    if (totalQueued >= this.maxTotalQueueSize) {
      return Promise.reject(
        new BatcherError(
          BatcherErrorCode.QUEUE_OVERFLOW,
          `Total queue overflow: ${this.maxTotalQueueSize} items max`,
          { retryable: true, data: { key, totalQueued, maxTotalQueueSize: this.maxTotalQueueSize } }
        )
      );
    }

    try {
      return this.getBatcher(key).submit(item, options);
    } catch (e) {
      // getBatcher 中 key 上限等同步异常，转为 rejected Promise
      if (e instanceof BatcherError) {
        return Promise.reject(e);
      }
      return Promise.reject(
        new BatcherError(
          BatcherErrorCode.FLUSH_FAILED,
          `Submit failed: ${e instanceof Error ? e.message : String(e)}`,
          { retryable: true, cause: e }
        )
      );
    }
  }

  get totalQueueSize(): number {
    let total = 0;
    for (const batcher of this.batchers.values()) {
      const m = batcher.metrics;
      total += m.currentQueueSize + m.inflightBatches * m.targetBatchSize;
    }
    return total;
  }

  get totalInflightBatches(): number {
    let total = 0;
    for (const batcher of this.batchers.values()) {
      total += batcher.metrics.inflightBatches;
    }
    return total;
  }

  get keyCount(): number {
    return this.batchers.size;
  }

  getKeyStats(key: string): KeyStats | null {
    const batcher = this.batchers.get(key);
    if (!batcher) return null;
    const m = batcher.metrics;
    return {
      key,
      queueSize: m.currentQueueSize,
      inflightBatches: m.inflightBatches,
      capacityUsedPercent: m.capacityUsedPercent,
      ewmaRate: m.ewmaRate,
      ewmaFlushDurationMs: m.ewmaFlushDurationMs,
      ewmaFailureRate: m.ewmaFailureRate,
      targetBatchSize: m.targetBatchSize,
      waitWindowMs: m.waitWindowMs,
      health: batcher.health,
      mode: batcher.mode,
      lastError: batcher.health.lastError,
      overflowCount: batcher.health.overflowCount,
      circuitState: batcher.circuitState,
    };
  }

  getSnapshot(options: { topN?: number } = {}): PartitionedSnapshot {
    const topN = options.topN ?? 10;
    const perKey: Record<string, KeyStats> = {};
    const allKeys: KeyStats[] = [];

    for (const key of this.batchers.keys()) {
      const stats = this.getKeyStats(key);
      if (stats) {
        perKey[key] = stats;
        allKeys.push(stats);
      }
    }

    const topByQueue = [...allKeys].sort((a, b) => b.queueSize - a.queueSize).slice(0, topN);
    const topByFailure = [...allKeys].sort((a, b) => b.ewmaFailureRate - a.ewmaFailureRate).slice(0, topN);

    const totalQueue = allKeys.reduce((s, k) => s + k.queueSize, 0);
    const totalInflight = allKeys.reduce((s, k) => s + k.inflightBatches, 0);
    const totalCap = Math.round((totalQueue / this.maxTotalQueueSize) * 100);

    let globalHealth: PartitionedSnapshot['globalHealth'] = 'healthy';
    if (allKeys.length === 0 || allKeys.every((k) => k.health.status === 'stale')) {
      globalHealth = 'stale';
    } else if (allKeys.some((k) => k.health.status === 'unhealthy')) {
      globalHealth = 'unhealthy';
    } else if (allKeys.some((k) => k.health.status === 'degraded')) {
      globalHealth = 'degraded';
    }

    return {
      timestamp: Date.now(),
      totalKeys: allKeys.length,
      totalQueueSize: totalQueue,
      totalInflightBatches: totalInflight,
      totalCapacityUsedPercent: totalCap,
      globalHealth,
      topKeysByQueue: topByQueue,
      topKeysByFailure: topByFailure,
      perKey,
    };
  }

  startSnapshotExport(
    intervalMs: number,
    callback: (snapshot: PartitionedSnapshot) => void | Promise<void>,
    options: { topN?: number } = {}
  ): void {
    if (this.disposed) return;
    this.stopSnapshotExport();
    this.snapshotExportCallback = callback;
    this.snapshotExportTopN = options.topN ?? 10;
    this.snapshotExportTimer = setInterval(() => {
      if (this.disposed) {
        this.stopSnapshotExport();
        return;
      }
      const snapshot = this.getSnapshot({ topN: this.snapshotExportTopN });
      if (this.snapshotExportCallback) {
        Promise.resolve()
          .then(() => this.snapshotExportCallback!(snapshot))
          .catch(() => {});
      }
    }, intervalMs);
  }

  stopSnapshotExport(): void {
    if (this.snapshotExportTimer !== null) {
      clearInterval(this.snapshotExportTimer);
      this.snapshotExportTimer = null;
    }
    this.snapshotExportCallback = null;
  }

  async forceFlush(key?: string): Promise<void> {
    if (key) {
      const batcher = this.batchers.get(key);
      if (batcher) await batcher.forceFlush();
      return;
    }
    await Promise.all(Array.from(this.batchers.values()).map((b) => b.forceFlush()));
  }

  async dispose(strategy: DisposeStrategy = 'drain'): Promise<void> {
    if (this.disposed) return;
    this.disposed = true;
    this.stopSnapshotExport();
    await Promise.all(Array.from(this.batchers.values()).map((b) => b.dispose(strategy)));
    this.batchers.clear();
  }

  get isDisposed(): boolean {
    return this.disposed;
  }
}
