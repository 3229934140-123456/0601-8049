import {
  AdaptiveBatcher,
  BatcherError,
  BatcherErrorCode,
  FlushEvent,
  DisposeStrategy,
  RollingStats,
  HealthStatus,
} from './adaptive-batcher';

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

interface TestCase {
  name: string;
  run: () => Promise<void>;
}

function expect(actual: unknown, message: string): {
  toBe: (expected: unknown) => void;
  toBeTruthy: () => void;
  toBeFalsy: () => void;
  toBeGreaterThan: (n: number) => void;
  toBeLessThan: (n: number) => void;
} {
  return {
    toBe(expected: unknown) {
      if (actual !== expected) {
        const err = new Error(`${message}: 期望 ${JSON.stringify(expected)}, 实际 ${JSON.stringify(actual)}`);
        err.name = 'AssertionError';
        throw err;
      }
    },
    toBeTruthy() {
      if (!actual) {
        const err = new Error(`${message}: 期望真值, 实际 ${JSON.stringify(actual)}`);
        err.name = 'AssertionError';
        throw err;
      }
    },
    toBeFalsy() {
      if (actual) {
        const err = new Error(`${message}: 期望假值, 实际 ${JSON.stringify(actual)}`);
        err.name = 'AssertionError';
        throw err;
      }
    },
    toBeGreaterThan(n: number) {
      if (typeof actual !== 'number' || !(actual > n)) {
        const err = new Error(`${message}: 期望 > ${n}, 实际 ${JSON.stringify(actual)}`);
        err.name = 'AssertionError';
        throw err;
      }
    },
    toBeLessThan(n: number) {
      if (typeof actual !== 'number' || !(actual < n)) {
        const err = new Error(`${message}: 期望 < ${n}, 实际 ${JSON.stringify(actual)}`);
        err.name = 'AssertionError';
        throw err;
      }
    },
  };
}

function expectError(fn: () => Promise<unknown>): Promise<BatcherError> {
  return fn().then(
    () => {
      const err = new Error('期望抛出错误但实际 resolve');
      err.name = 'AssertionError';
      throw err;
    },
    (e) => {
      if (!(e instanceof BatcherError)) {
        const err = new Error(`期望 BatcherError, 实际 ${e}`);
        err.name = 'AssertionError';
        throw err;
      }
      return e;
    }
  );
}

const testCases: TestCase[] = [
  {
    name: '场景A: 可观测能力 - onFlush事件 + rollingStats滚动统计',
    run: async () => {
      const events: FlushEvent<number, number>[] = [];

      const batcher = new AdaptiveBatcher<number, number>({
        minBatchSize: 2,
        maxBatchSize: 10,
        initialBatchSize: 2,
        minWaitMs: 5,
        maxWaitMs: 30,
        initialWaitMs: 10,
        statsWindowMs: 3000,
        flush: async (batch) => {
          await sleep(20);
          return batch.map((x) => x * 2);
        },
        onFlush: (ev) => events.push(ev),
      });

      const proms: Promise<number>[] = [];
      for (let i = 0; i < 7; i++) {
        proms.push(batcher.submit(i));
        await sleep(2);
      }
      await Promise.all(proms);

      expect(events.length > 0, '应收到 onFlush 事件').toBeTruthy();

      const firstEvent = events[0];
      expect(firstEvent.batchSize > 0, 'batchSize > 0').toBeTruthy();
      expect(firstEvent.flushDurationMs >= 15, 'flushDurationMs >= 15ms').toBeTruthy();
      expect(firstEvent.success, 'success=true').toBe(true);
      expect(firstEvent.items.length > 0, 'items 非空').toBeTruthy();
      expect(firstEvent.results!.length === firstEvent.items.length, 'results 与 items 等长').toBe(true);
      expect(firstEvent.timestamp > 0, 'timestamp 存在').toBeTruthy();

      const stats = batcher.rollingStats;
      expect(stats.itemsProcessed, 'itemsProcessed=7').toBe(7);
      expect(stats.batchesProcessed >= 1, 'batchesProcessed >= 1').toBeTruthy();
      expect(stats.failureRate, 'failureRate=0').toBe(0);
      expect(stats.throughputPerSecond > 0, 'throughput > 0').toBeTruthy();
      expect(stats.stale, 'stale=false').toBe(false);
      expect(stats.p50QueuedWaitMs, 'p50QueuedWaitMs 存在').toBeTruthy();
      expect(stats.p95QueuedWaitMs, 'p95QueuedWaitMs 存在').toBeTruthy();
      expect(stats.p50FlushDurationMs, 'p50FlushDurationMs 存在').toBeTruthy();
      expect(stats.p95FlushDurationMs, 'p95FlushDurationMs 存在').toBeTruthy();

      const mode = batcher.mode;
      expect(['low-latency', 'balanced', 'high-throughput', 'idle'].includes(mode), 'mode 枚举合法').toBe(true);

      const metrics = batcher.metrics;
      expect(metrics.ewmaFlushDurationMs >= 10, 'ewmaFlushDuration >= 10ms').toBeTruthy();

      const health = batcher.health;
      expect(['healthy', 'degraded', 'unhealthy', 'stale'].includes(health.status), 'health.status 合法').toBe(true);
      expect(health.lastEventAgeMs != null, 'lastEventAgeMs 存在').toBeTruthy();

      await batcher.dispose();
    },
  },
  {
    name: '场景B1: 背压策略 - reject模式 + 错误码区分',
    run: async () => {
      const batcher = new AdaptiveBatcher<number, number>({
        minBatchSize: 3,
        maxBatchSize: 3,
        initialBatchSize: 3,
        minWaitMs: 5,
        maxWaitMs: 100,
        maxQueueSize: 6,
        maxInflightBatches: 1,
        overflowStrategy: 'reject',
        flush: async (batch) => {
          await sleep(200);
          return batch.map((x) => x * 10);
        },
      });

      const p1 = batcher.submit(1);
      const p2 = batcher.submit(2);
      const p3 = batcher.submit(3);
      await sleep(10);

      const p4 = batcher.submit(4);
      const p5 = batcher.submit(5);
      const p6 = batcher.submit(6);

      await sleep(5);

      const overflowErr = await expectError(() => batcher.submit(999));
      expect(overflowErr.code, '错误码应为 QUEUE_OVERFLOW').toBe(BatcherErrorCode.QUEUE_OVERFLOW);
      expect(overflowErr.retryable, '溢出错误应为可重试').toBe(true);

      const badBatcher = new AdaptiveBatcher<number, number>({
        minBatchSize: 1,
        maxBatchSize: 1,
        maxQueueSize: 10,
        flush: async () => {
          throw new Error('下游数据库挂了');
        },
      });
      const downstreamErr = await expectError(() => badBatcher.submit(1));
      expect(downstreamErr.code, '错误码应为 FLUSH_FAILED').toBe(BatcherErrorCode.FLUSH_FAILED);
      expect((downstreamErr as unknown as { cause?: Error }).cause?.message, 'cause 包含原始错误').toBe('下游数据库挂了');

      await Promise.all([p1, p2, p3, p4, p5, p6]);
      await batcher.dispose();
      await badBatcher.dispose();
    },
  },
  {
    name: '场景B2: 背压策略 - drop模式 + fallback降级 / 无降级不挂起',
    run: async () => {
      const drops: number[] = [];
      const batcherWithFallback = new AdaptiveBatcher<number, number>({
        minBatchSize: 2,
        maxBatchSize: 2,
        maxQueueSize: 2,
        maxInflightBatches: 1,
        overflowStrategy: 'drop',
        flush: async (batch) => {
          await sleep(100);
          return batch.map((x) => x + 100);
        },
        fallback: (item) => {
          drops.push(item);
          return item + 500;
        },
      });

      const p1 = batcherWithFallback.submit(1);
      const p2 = batcherWithFallback.submit(2);
      await sleep(5);
      const p3 = batcherWithFallback.submit(3);
      const p5 = batcherWithFallback.submit(99);

      expect(await p1, 'p1=101').toBe(101);
      expect(await p5, 'p5=599').toBe(599);
      expect(drops.includes(99), 'drops包含99').toBe(true);
      await batcherWithFallback.dispose();

      const batcherNoFallback = new AdaptiveBatcher<number, number>({
        minBatchSize: 2,
        maxBatchSize: 2,
        maxQueueSize: 2,
        maxInflightBatches: 1,
        overflowStrategy: 'drop',
        flush: async (batch) => {
          await sleep(100);
          return batch.map((x) => x + 100);
        },
      });

      const pSubmit1 = batcherNoFallback.submit(1);
      const pSubmit2 = batcherNoFallback.submit(2);
      await sleep(5);

      const dropPromise = batcherNoFallback.submit(999);
      const dropErr = await expectError(() =>
        Promise.race([
          dropPromise,
          new Promise<never>((_, r) => setTimeout(() => r(new Error('超时挂起') as never), 100)),
        ])
      );
      await pSubmit1;
      await pSubmit2;
      expect(dropErr.code, '无降级时drop应返回DROPPED错误').toBe(BatcherErrorCode.DROPPED);
      expect(dropErr.retryable, 'DROPPED应为可重试').toBe(true);

      await batcherNoFallback.dispose();
    },
  },
  {
    name: '场景C: 自适应调参 - 根据flush耗时和失败率收缩批次',
    run: async () => {
      let slowMode = false;
      let failMode = false;

      const batcher = new AdaptiveBatcher<number, number>({
        minBatchSize: 1,
        maxBatchSize: 50,
        initialBatchSize: 10,
        minWaitMs: 2,
        maxWaitMs: 60,
        adaptToFlushDuration: true,
        adaptToFailureRate: true,
        flush: async (batch) => {
          if (failMode) throw new Error('模拟下游失败');
          await sleep(slowMode ? 500 : 10);
          return batch.map((x) => x);
        },
      });

      console.log('  阶段1: 正常高速模式');
      for (let i = 0; i < 200; i++) {
        void batcher.submit(i);
        if (i % 5 === 0) await sleep(1);
      }
      await sleep(30);
      const m1 = batcher.metrics;
      console.log(`    → 目标批次=${m1.targetBatchSize}, EWMA速率=${m1.ewmaRate}/s`);
      expect(m1.targetBatchSize > 20, '阶段1批次应较大 (>20)').toBeTruthy();
      const size1 = m1.targetBatchSize;

      console.log('  阶段2: 下游变慢(flush=500ms), 批次应收缩');
      slowMode = true;
      await batcher.forceFlush();
      for (let i = 100; i < 130; i++) {
        void batcher.submit(i);
        await sleep(5);
      }
      await sleep(1100);
      const m2 = batcher.metrics;
      console.log(`    → 目标批次=${m2.targetBatchSize}, flush耗时EWMA=${m2.ewmaFlushDurationMs}ms`);
      expect(m2.ewmaFlushDurationMs > 200, 'flush耗时EWMA应>200ms').toBeTruthy();
      expect(m2.targetBatchSize < size1 * 0.9, '下游变慢后批次应收缩至少10%').toBe(true);

      console.log('  阶段3: 下游开始失败, 批次应进一步收缩');
      failMode = true;
      await batcher.forceFlush();
      for (let i = 200; i < 220; i++) {
        batcher.submit(i).catch(() => {});
        await sleep(5);
      }
      await sleep(300);
      const m3 = batcher.metrics;
      console.log(`    → 目标批次=${m3.targetBatchSize}, 失败率EWMA=${m3.ewmaFailureRate}`);
      expect(m3.ewmaFailureRate > 0, '失败率>0').toBeTruthy();
      expect(m3.targetBatchSize < m2.targetBatchSize, '失败后批次应更小').toBe(true);

      console.log('  阶段4: 下游恢复, 批次应回升');
      failMode = false;
      slowMode = false;
      for (let i = 300; i < 360; i++) {
        void batcher.submit(i);
        await sleep(2);
      }
      await sleep(50);
      const m4 = batcher.metrics;
      console.log(`    → 目标批次=${m4.targetBatchSize}`);
      expect(m4.targetBatchSize > m3.targetBatchSize, '恢复后批次应回升').toBe(true);

      await batcher.dispose();
    },
  },
  {
    name: '场景D: 收尾不卡死 - dispose在并发占满时正常结束',
    run: async () => {
      const results: Array<{ id: number; status: string; value?: number; error?: string }> = [];
      const batcher = new AdaptiveBatcher<number, number>({
        minBatchSize: 3,
        maxBatchSize: 3,
        initialBatchSize: 3,
        minWaitMs: 5,
        maxWaitMs: 20,
        maxInflightBatches: 1,
        maxQueueSize: 100,
        flush: async (batch) => {
          await sleep(150);
          return batch.map((x) => x + 1000);
        },
      });

      for (let i = 0; i < 10; i++) {
        const id = i;
        batcher.submit(i)
          .then((v) => results.push({ id, status: 'resolved', value: v }))
          .catch((e) => results.push({ id, status: 'rejected', error: (e as Error).message }));
        if (i === 2) await sleep(10);
      }

      await sleep(20);
      const metricsBefore = batcher.metrics;
      expect(metricsBefore.inflightBatches, '此时inflight=1').toBe(1);
      expect(metricsBefore.currentQueueSize > 0, '队列仍有积压').toBe(true);

      const disposeStart = Date.now();
      const disposePromise = batcher.dispose();
      const timeout = new Promise<never>((_, r) =>
        setTimeout(() => r(new Error('dispose 卡死了!') as never), 2000)
      );
      await Promise.race([disposePromise, timeout]);
      const disposeDuration = Date.now() - disposeStart;

      console.log(`  dispose 耗时: ${disposeDuration}ms`);
      expect(disposeDuration < 2000, 'dispose 不应卡死').toBe(true);
      expect(results.length, '所有10个请求都有归宿').toBe(10);

      await batcher.dispose();
    },
  },
  {
    name: '场景E: 收尾不卡死 - forceFlush在并发占满时正常结束',
    run: async () => {
      const batcher = new AdaptiveBatcher<number, number>({
        minBatchSize: 2,
        maxBatchSize: 2,
        initialBatchSize: 2,
        minWaitMs: 5,
        maxWaitMs: 20,
        maxInflightBatches: 1,
        flush: async (batch) => {
          await sleep(80);
          return batch.map((x) => x * 10);
        },
      });

      const proms: Promise<number>[] = [];
      for (let i = 0; i < 7; i++) {
        proms.push(batcher.submit(i));
      }

      await sleep(10);
      const m = batcher.metrics;
      expect(m.inflightBatches, 'inflight=1').toBe(1);
      expect(m.currentQueueSize > 0, '队列有积压').toBe(true);

      const flushStart = Date.now();
      const flushPromise = batcher.forceFlush();
      const timeout = new Promise<never>((_, r) =>
        setTimeout(() => r(new Error('forceFlush 卡死了!') as never), 2000)
      );
      await Promise.race([flushPromise, timeout]);
      const flushDuration = Date.now() - flushStart;

      console.log(`  forceFlush 耗时: ${flushDuration}ms`);
      expect(flushDuration < 2000, 'forceFlush 不应卡死').toBe(true);

      const m2 = batcher.metrics;
      expect(m2.inflightBatches, 'inflight=0').toBe(0);
      expect(m2.currentQueueSize, 'queue=0').toBe(0);

      await Promise.all(proms);
      await batcher.dispose();
    },
  },
  {
    name: '场景F: 提交取消与超时 - 排队中取消、超时生效',
    run: async () => {
      const batcher = new AdaptiveBatcher<number, number>({
        minBatchSize: 10,
        maxBatchSize: 10,
        initialBatchSize: 10,
        minWaitMs: 5,
        maxWaitMs: 500,
        maxInflightBatches: 1,
        flush: async (batch) => {
          await sleep(30);
          return batch.map((x) => x * 2);
        },
      });

      console.log('  测试1: 排队中通过 AbortSignal 取消');
      const controller = new AbortController();
      const pCancel = batcher.submit(999, { signal: controller.signal });
      await sleep(5);
      controller.abort();
      const errCancel = await expectError(() => pCancel);
      expect(errCancel.code, '错误码应为 CANCELED').toBe(BatcherErrorCode.CANCELED);
      expect(errCancel.retryable, 'CANCELED 不可重试').toBe(false);

      console.log('  测试2: 提交超时 (timeoutMs)');
      const pTimeout = batcher.submit(888, { timeoutMs: 50 });
      const errTimeout = await expectError(() => pTimeout);
      expect(errTimeout.code, '错误码应为 TIMEOUT').toBe(BatcherErrorCode.TIMEOUT);
      expect(errTimeout.retryable, 'TIMEOUT 可重试').toBe(true);

      console.log('  测试3: 已取消的 signal 直接拒绝');
      const controller2 = new AbortController();
      controller2.abort();
      const pPreCancel = batcher.submit(777, { signal: controller2.signal });
      const errPreCancel = await expectError(() => pPreCancel);
      expect(errPreCancel.code, '已取消的signal直接返回CANCELED').toBe(BatcherErrorCode.CANCELED);

      console.log('  测试4: 正常提交不影响');
      const normalProms: Promise<number>[] = [];
      for (let i = 0; i < 10; i++) {
        normalProms.push(batcher.submit(i));
      }
      const normalResults = await Promise.all(normalProms);
      expect(normalResults.length, '正常提交应全部成功').toBe(10);
      expect(normalResults[0], '结果正确').toBe(0);

      await batcher.dispose();
    },
  },
  {
    name: '场景G: dispose策略 - drain/reject/kill 三种策略',
    run: async () => {
      for (const strategy of ['drain', 'reject', 'kill'] as DisposeStrategy[]) {
        console.log(`  测试策略: ${strategy}`);
        const batcher = new AdaptiveBatcher<number, number>({
          minBatchSize: 2,
          maxBatchSize: 2,
          initialBatchSize: 2,
          minWaitMs: 5,
          maxWaitMs: 20,
          maxInflightBatches: 1,
          flush: async (batch) => {
            await sleep(60);
            return batch.map((x) => x + 100);
          },
        });

        const results: Array<{ id: number; status: string; code?: BatcherErrorCode }> = [];
        for (let i = 0; i < 5; i++) {
          const id = i;
          batcher.submit(i)
            .then(() => results.push({ id, status: 'resolved' }))
            .catch((e) => results.push({ id, status: 'rejected', code: (e as BatcherError).code }));
        }

        await sleep(10);
        const before = batcher.metrics;
        console.log(`    dispose前: inflight=${before.inflightBatches}, queue=${before.currentQueueSize}`);

        const start = Date.now();
        await batcher.dispose(strategy);
        const duration = Date.now() - start;
        console.log(`    dispose耗时=${duration}ms, resolved=${results.filter((r) => r.status === 'resolved').length}, rejected=${results.filter((r) => r.status === 'rejected').length}`);

        expect(results.length, '所有5个请求有归宿').toBe(5);
        expect(duration < 1000, `${strategy} 不卡死`).toBe(true);

        if (strategy === 'kill') {
          const rejected = results.filter((r) => r.status === 'rejected');
          expect(rejected.length > 0, 'kill策略应有被reject的请求').toBeTruthy();
          expect(rejected[0].code, 'kill策略应为DISPOSED').toBe(BatcherErrorCode.DISPOSED);
        }
      }
    },
  },
  {
    name: '场景H: 百分位统计 + stale过期机制 + 健康状态',
    run: async () => {
      const batcher = new AdaptiveBatcher<number, number>({
        minBatchSize: 2,
        maxBatchSize: 5,
        initialBatchSize: 2,
        minWaitMs: 2,
        maxWaitMs: 20,
        statsWindowMs: 2000,
        flush: async (batch) => {
          await sleep(10 + batch.length * 5);
          return batch.map((x) => x);
        },
      });

      console.log('  阶段1: 活跃流量,验证p50/p95和健康状态');
      const proms: Promise<number>[] = [];
      for (let i = 0; i < 15; i++) {
        proms.push(batcher.submit(i));
        await sleep(3);
      }
      await Promise.all(proms);
      await sleep(50);

      const stats = batcher.rollingStats;
      console.log(`    p50Wait=${stats.p50QueuedWaitMs}ms, p95Wait=${stats.p95QueuedWaitMs}ms, p50Flush=${stats.p50FlushDurationMs}ms, p95Flush=${stats.p95FlushDurationMs}ms`);
      expect(stats.p50QueuedWaitMs >= 0, 'p50Wait 合法').toBeTruthy();
      expect(stats.p95QueuedWaitMs >= stats.p50QueuedWaitMs, 'p95 >= p50').toBeTruthy();
      expect(stats.p95FlushDurationMs >= stats.p50FlushDurationMs, 'p95Flush >= p50Flush').toBeTruthy();
      expect(stats.stale, '活跃时stale=false').toBe(false);

      const health1 = batcher.health;
      console.log(`    健康状态: ${health1.status}, mode=${health1.mode}`);
      expect(health1.status, '健康状态应为healthy').toBe('healthy');

      console.log('  阶段2: 长时间无流量,统计应过期为stale');
      const originalGetTime = Date.now;
      const now = originalGetTime();
      Date.now = () => now + 6000;

      const statsStale = batcher.rollingStats;
      console.log(`    6秒后: stale=${statsStale.stale}, itemsProcessed=${statsStale.itemsProcessed}`);
      expect(statsStale.stale, '过期后stale=true').toBe(true);
      expect(statsStale.itemsProcessed, '过期后统计清零').toBe(0);

      const healthStale = batcher.health;
      console.log(`    过期健康状态: ${healthStale.status}, mode=${healthStale.mode}`);
      expect(healthStale.status, '过期状态应为stale').toBe('stale');
      expect(healthStale.mode, '过期mode应为idle').toBe('idle');

      Date.now = originalGetTime;

      await batcher.dispose();
    },
  },
  {
    name: '场景I: 背压边界 - block策略关闭时快速返回',
    run: async () => {
      const batcher = new AdaptiveBatcher<number, number>({
        minBatchSize: 2,
        maxBatchSize: 2,
        maxQueueSize: 2,
        maxInflightBatches: 1,
        overflowStrategy: 'block',
        flush: async (batch) => {
          await sleep(200);
          return batch.map((x) => x + 100);
        },
      });

      const p1 = batcher.submit(1);
      const p2 = batcher.submit(2);
      await sleep(10);

      const blockStart = Date.now();
      const pBlock = batcher.submit(999);
      await sleep(50);

      const disposePromise = batcher.dispose();
      const err = await expectError(() => pBlock);
      await Promise.allSettled([p1, p2]);
      const blockDuration = Date.now() - blockStart;

      console.log(`  block等待${blockDuration}ms后, dispose触发错误: code=${err.code}`);
      expect(err.code, 'block中dispose应返回DISPOSED').toBe(BatcherErrorCode.DISPOSED);
      expect(blockDuration < 500, '不会一直等下去').toBe(true);

      await disposePromise;
    },
  },
  {
    name: '场景J: 回调隔离 - onFlush/onError错误不影响主流程',
    run: async () => {
      console.log('  测试1: onFlush同步抛错不影响');
      let onFlushError = false;
      const batcher1 = new AdaptiveBatcher<number, number>({
        minBatchSize: 2,
        maxBatchSize: 2,
        initialBatchSize: 2,
        minWaitMs: 5,
        maxWaitMs: 20,
        flush: async (batch) => batch.map((x) => x * 2),
        onFlush: () => {
          onFlushError = true;
          throw new Error('监控系统挂了');
        },
      });
      const r1 = await Promise.all([batcher1.submit(1), batcher1.submit(2)]);
      expect(onFlushError, 'onFlush确实被调用了').toBe(true);
      expect(r1[0], '主流程结果不受影响').toBe(2);
      await batcher1.dispose();

      console.log('  测试2: onFlush异步抛错不影响主流程,也不卡forceFlush');
      const batcher2 = new AdaptiveBatcher<number, number>({
        minBatchSize: 2,
        maxBatchSize: 2,
        initialBatchSize: 2,
        minWaitMs: 5,
        maxWaitMs: 20,
        flush: async (batch) => batch.map((x) => x * 2),
        onFlush: async () => {
          await sleep(500);
          throw new Error('异步监控挂了');
        },
      });
      const r2 = await Promise.all([batcher2.submit(3), batcher2.submit(4)]);
      expect(r2[0], '异步onFlush不影响结果').toBe(6);

      const flushStart = Date.now();
      await batcher2.forceFlush();
      const flushDuration = Date.now() - flushStart;
      console.log(`    forceFlush耗时=${flushDuration}ms (不被500ms的异步onFlush卡住)`);
      expect(flushDuration < 200, 'forceFlush不被异步回调卡住').toBe(true);
      await batcher2.dispose();

      console.log('  测试3: onError抛错不影响');
      const batcher3 = new AdaptiveBatcher<number, number>({
        minBatchSize: 1,
        maxBatchSize: 1,
        flush: async () => {
          throw new Error('下游挂了');
        },
        onError: () => {
          throw new Error('监控告警也挂了');
        },
      });
      const err = await expectError(() => batcher3.submit(1));
      expect(err.code, '主流程错误正常返回').toBe(BatcherErrorCode.FLUSH_FAILED);
      await batcher3.dispose();

      console.log('  测试4: dispose也不被卡住');
      const batcher4 = new AdaptiveBatcher<number, number>({
        minBatchSize: 2,
        maxBatchSize: 2,
        initialBatchSize: 2,
        minWaitMs: 5,
        maxWaitMs: 20,
        flush: async (batch) => {
          await sleep(30);
          return batch.map((x) => x);
        },
        onFlush: async () => {
          await sleep(500);
        },
      });
      const p4 = Promise.all([batcher4.submit(1), batcher4.submit(2)]);
      await sleep(10);
      const disposeStart = Date.now();
      await batcher4.dispose();
      const disposeDuration = Date.now() - disposeStart;
      console.log(`    dispose耗时=${disposeDuration}ms (不被500ms的异步onFlush卡住)`);
      expect(disposeDuration < 200, 'dispose不被异步回调卡住').toBe(true);
      await p4;
    },
  },
  {
    name: '回归测试: 双缓冲无阻塞、老功能正常',
    run: async () => {
      let maxQueueDuringFlush = 0;
      const batcher = new AdaptiveBatcher<number, number>({
        minBatchSize: 2,
        maxBatchSize: 20,
        initialBatchSize: 2,
        minWaitMs: 2,
        maxWaitMs: 20,
        maxInflightBatches: 1,
        flush: async (batch) => {
          await sleep(80);
          maxQueueDuringFlush = Math.max(maxQueueDuringFlush, batcher.metrics.currentQueueSize);
          return batch.map((x) => x + 1000);
        },
      });

      const wave1 = Promise.all([batcher.submit(0), batcher.submit(1)]);
      await sleep(15);
      const wave2 = [2, 3, 4, 5, 6].map((i) => batcher.submit(i));
      const queueMid = batcher.metrics.currentQueueSize;
      console.log(`  flush进行中队列深度=${queueMid} (>0 → 无阻塞)`);
      expect(queueMid > 0, '无阻塞入队').toBeTruthy();

      const r1 = await wave1;
      const r2 = await Promise.all(wave2);
      expect(JSON.stringify(r1), 'wave1=[1000,1001]').toBe(JSON.stringify([1000, 1001]));
      expect(r2.length, 'wave2=5个').toBe(5);

      await batcher.dispose();
    },
  },
];

(async () => {
  console.log('='.repeat(75));
  console.log('  自适应批量提交器 v3 - 生产可用版 测试验证');
  console.log('='.repeat(75));
  console.log();

  let passed = 0;
  let failed = 0;
  const failedNames: string[] = [];

  for (const tc of testCases) {
    console.log(tc.name);
    console.log('-'.repeat(75));
    try {
      await tc.run();
      console.log(`  ✅ PASS`);
      passed++;
    } catch (e) {
      if ((e as Error).name === 'AssertionError') {
        console.log(`  ❌ FAIL (Assertion): ${(e as Error).message}`);
      } else {
        console.log(`  ❌ FAIL (Exception): ${(e as Error).message}`);
        console.log((e as Error).stack);
      }
      failed++;
      failedNames.push(tc.name);
    }
    console.log();
  }

  console.log('='.repeat(75));
  console.log(`  结果: ${passed} 通过, ${failed} 失败, 共 ${testCases.length} 个测试`);
  if (failedNames.length > 0) console.log(`  失败: ${failedNames.join(', ')}`);
  console.log('='.repeat(75));
  process.exit(failed > 0 ? 1 : 0);
})();
