import {
  AdaptiveBatcher,
  PartitionedAdaptiveBatcher,
  BatcherError,
  BatcherErrorCode,
  RetryableFailure,
  PermanentFailure,
  FlushEvent,
  DisposeStrategy,
  RollingStats,
  HealthStatus,
  KeyStats,
  PartitionedSnapshot,
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
  toBeGreaterThanOrEqual: (n: number) => void;
  toBeLessThanOrEqual: (n: number) => void;
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
    toBeGreaterThanOrEqual(n: number) {
      if (typeof actual !== 'number' || !(actual >= n)) {
        const err = new Error(`${message}: 期望 >= ${n}, 实际 ${JSON.stringify(actual)}`);
        err.name = 'AssertionError';
        throw err;
      }
    },
    toBeLessThanOrEqual(n: number) {
      if (typeof actual !== 'number' || !(actual <= n)) {
        const err = new Error(`${message}: 期望 <= ${n}, 实际 ${JSON.stringify(actual)}`);
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
  // ========== v4 新功能测试 ==========
  {
    name: '场景1: 部分失败处理 - RetryableFailure 重试成功',
    run: async () => {
      let attemptCount = 0;
      const batcher = new AdaptiveBatcher<number, number>({
        minBatchSize: 3,
        maxBatchSize: 3,
        initialBatchSize: 3,
        minWaitMs: 5,
        maxWaitMs: 20,
        maxRetries: 2,
        retryDelayMs: 5,
        flush: (batch, attempt) => {
          attemptCount = attempt;
          if (attempt === 1) {
            return batch.map((x, i) =>
              i === 1 ? new RetryableFailure(`item ${x} 暂时失败`) : x * 10
            ) as number[];
          }
          return batch.map((x) => x * 10);
        },
      });

      const results = await Promise.all([batcher.submit(1), batcher.submit(2), batcher.submit(3)]);
      expect(attemptCount, '重试了一次,最终attempt=2').toBe(2);
      expect(results[0], 'item1=10').toBe(10);
      expect(results[1], 'item2重试成功=20').toBe(20);
      expect(results[2], 'item3=30').toBe(30);
      await batcher.dispose();
    },
  },
  {
    name: '场景2: 部分失败处理 - PermanentFailure 不重试、直接永久失败',
    run: async () => {
      let flushCount = 0;
      const batcher = new AdaptiveBatcher<number, number>({
        minBatchSize: 3,
        maxBatchSize: 3,
        initialBatchSize: 3,
        minWaitMs: 5,
        maxWaitMs: 20,
        maxRetries: 3,
        retryDelayMs: 5,
        flush: (batch) => {
          flushCount++;
          return batch.map((x, i) =>
            i === 1 ? new PermanentFailure(`item ${x} 永久失败`) : x * 10
          ) as number[];
        },
      });

      const p1 = batcher.submit(1);
      const p2 = batcher.submit(2);
      const p3 = batcher.submit(3);

      const [r1, err2, r3] = await Promise.all([p1, p2.catch((e) => e), p3]);
      expect(r1, 'item1成功=10').toBe(10);
      expect(r3, 'item3成功=30').toBe(30);
      expect(err2.code, 'item2是PERMANENT_FAILURE').toBe(BatcherErrorCode.PERMANENT_FAILURE);
      expect(err2.retryable, '永久失败不可重试').toBe(false);
      expect(flushCount, '只flush了1次,永久失败不重试').toBe(1);
      await batcher.dispose();
    },
  },
  {
    name: '场景3: 重试耗尽 - RETRY_EXHAUSTED 错误',
    run: async () => {
      const batcher = new AdaptiveBatcher<number, number>({
        minBatchSize: 2,
        maxBatchSize: 2,
        initialBatchSize: 2,
        minWaitMs: 5,
        maxWaitMs: 20,
        maxRetries: 2,
        retryDelayMs: 5,
        flush: (batch, attempt) => {
          return batch.map(() => new RetryableFailure(`第${attempt}次还是失败`)) as number[];
        },
      });

      const err = await expectError(() =>
        Promise.all([batcher.submit(1), batcher.submit(2)])
      );
      expect(err.code, '重试耗尽返回RETRY_EXHAUSTED').toBe(BatcherErrorCode.RETRY_EXHAUSTED);
      expect(err.retryable, '重试耗尽仍可重试(由调用方决定)').toBe(true);
      await batcher.dispose();
    },
  },
  {
    name: '场景4: flushing 中超时也能立即返回',
    run: async () => {
      const batcher = new AdaptiveBatcher<number, number>({
        minBatchSize: 2,
        maxBatchSize: 2,
        initialBatchSize: 2,
        minWaitMs: 5,
        maxWaitMs: 20,
        maxInflightBatches: 1,
        flush: async (batch) => {
          await sleep(500);
          return batch.map((x) => x * 10);
        },
      });

      const start = Date.now();
      const pFast = batcher.submit(1, { timeoutMs: 50 });
      const pSlow = batcher.submit(2);

      const err = await expectError(() => pFast);
      const fastDuration = Date.now() - start;

      console.log(`  超时请求 ${fastDuration}ms 后返回, code=${err.code}`);
      expect(err.code, 'flushing中超时返回TIMEOUT').toBe(BatcherErrorCode.TIMEOUT);
      expect(fastDuration, '50ms左右就返回,不用等500ms').toBeLessThan(200);
      expect(fastDuration, '至少等了50ms').toBeGreaterThanOrEqual(40);

      const slowResult = await pSlow;
      expect(slowResult, '慢请求正常完成=20').toBe(20);

      await batcher.dispose();
    },
  },
  {
    name: '场景5: kill 策略立即返回,不等慢下游',
    run: async () => {
      const results: Array<{ id: number; status: string; duration?: number }> = [];

      const batcher = new AdaptiveBatcher<number, number>({
        minBatchSize: 3,
        maxBatchSize: 3,
        initialBatchSize: 3,
        minWaitMs: 5,
        maxWaitMs: 20,
        maxInflightBatches: 1,
        flush: async (batch) => {
          await sleep(2000);
          return batch.map((x) => x + 100);
        },
      });

      const start = Date.now();
      const p1 = batcher.submit(1);
      const p2 = batcher.submit(2);
      const p3 = batcher.submit(3);

      await sleep(20);

      const killStart = Date.now();
      const disposePromise = batcher.dispose('kill');
      const p4 = batcher.submit(999);

      const [, , , err4] = await Promise.allSettled([p1, p2, p3, p4]);
      await disposePromise;
      const killDuration = Date.now() - killStart;

      console.log(`  kill 耗时: ${killDuration}ms (远小于2000ms)`);
      expect(killDuration, 'kill立即返回,<100ms').toBeLessThan(100);

      const err1 = await expectError(() => p1);
      expect(err1.code, 'inflight的请求也被kill').toBe(BatcherErrorCode.DISPOSED);

      const totalDuration = Date.now() - start;
      expect(totalDuration, '整体耗时远小于2000ms').toBeLessThan(500);

      await batcher.dispose();
    },
  },
  {
    name: '场景6: PartitionedBatcher 按 key 分组攒批,各 key 独立',
    run: async () => {
      const flushCalls: Array<{ key: string; batch: number[] }> = [];

      const pb = new PartitionedAdaptiveBatcher<number, number>({
        perKeyMinBatchSize: 2,
        perKeyMaxBatchSize: 2,
        perKeyInitialBatchSize: 2,
        perKeyMinWaitMs: 5,
        perKeyMaxWaitMs: 50,
        perKeyMaxInflightBatches: 1,
        perKeyMaxQueueSize: 10,
        maxTotalQueueSize: 100,
        maxKeys: 10,
        flush: (key, batch) => {
          flushCalls.push({ key, batch });
          return batch.map((x) => x + key.length * 100);
        },
      });

      const pa1 = pb.submit(1, { key: 'aaa' });
      const pb1 = pb.submit(10, { key: 'bb' });
      const pa2 = pb.submit(2, { key: 'aaa' });
      const pb2 = pb.submit(20, { key: 'bb' });

      const [ra1, ra2, rb1, rb2] = await Promise.all([pa1, pa2, pb1, pb2]);

      expect(flushCalls.length, '应该有2次flush,每个key各一次').toBe(2);
      const aaaFlush = flushCalls.find((f) => f.key === 'aaa')!;
      const bbFlush = flushCalls.find((f) => f.key === 'bb')!;
      expect(aaaFlush.batch.length, 'aaa批次大小=2').toBe(2);
      expect(bbFlush.batch.length, 'bb批次大小=2').toBe(2);

      expect(ra1, 'aaa的结果=301(aaa长3,3*100+1)').toBe(301);
      expect(rb1, 'bb的结果=210(bb长2,2*100+10)').toBe(210);

      expect(pb.keyCount, 'key数量=2').toBe(2);
      await pb.dispose();
    },
  },
  {
    name: '场景7: PartitionedBatcher 热点 key 爆了不影响冷门 key',
    run: async () => {
      const pb = new PartitionedAdaptiveBatcher<number, number>({
        perKeyMinBatchSize: 2,
        perKeyMaxBatchSize: 2,
        perKeyInitialBatchSize: 2,
        perKeyMinWaitMs: 5,
        perKeyMaxWaitMs: 50,
        perKeyMaxQueueSize: 3,
        perKeyMaxInflightBatches: 1,
        maxTotalQueueSize: 100,
        overflowStrategy: 'reject',
        flush: async (_, batch) => {
          await sleep(200);
          return batch.map((x) => x * 2);
        },
      });

      const hotPromises: Promise<number>[] = [];
      for (let i = 0; i < 4; i++) {
        hotPromises.push(pb.submit(i, { key: 'hot' }));
      }

      const coldP = pb.submit(42, { key: 'cold' });
      const allPromises = [...hotPromises, coldP];
      const allResults = await Promise.allSettled(allPromises);

      let overflowCount = 0;
      for (let i = 0; i < hotPromises.length; i++) {
        if (allResults[i].status === 'rejected' && allResults[i].reason.code === BatcherErrorCode.QUEUE_OVERFLOW) {
          overflowCount++;
        }
      }
      expect(overflowCount, 'hot key有1个溢出').toBe(1);

      const coldResult = allResults[allResults.length - 1];
      expect(coldResult.status, 'cold key成功').toBe('fulfilled');
      expect((coldResult as PromiseFulfilledResult<number>).value, 'cold key不受影响').toBe(84);

      expect(pb.keyCount, '有2个key').toBe(2);
      await pb.dispose('kill');
    },
  },
  {
    name: '场景8: PartitionedBatcher 全局队列限制',
    run: async () => {
      const pb = new PartitionedAdaptiveBatcher<number, number>({
        perKeyMinBatchSize: 2,
        perKeyMaxBatchSize: 2,
        perKeyInitialBatchSize: 2,
        perKeyMinWaitMs: 5,
        perKeyMaxWaitMs: 50,
        perKeyMaxQueueSize: 100,
        perKeyMaxInflightBatches: 1,
        maxTotalQueueSize: 5,
        maxKeys: 100,
        overflowStrategy: 'reject',
        flush: async (_, batch) => {
          await sleep(200);
          return batch.map((x) => x * 2);
        },
      });

      const promises: Promise<number>[] = [];
      for (let i = 0; i < 3; i++) {
        promises.push(pb.submit(i * 10, { key: 'a' }));
        promises.push(pb.submit(i * 10 + 1, { key: 'b' }));
      }
      promises.push(pb.submit(999, { key: 'c' }));

      const allResults = await Promise.allSettled(promises);
      let overflowCount = 0;
      for (const r of allResults) {
        if (r.status === 'rejected' && r.reason.code === BatcherErrorCode.QUEUE_OVERFLOW) {
          overflowCount++;
        }
      }
      expect(overflowCount > 0, '全局队列满了有溢出').toBe(true);

      await pb.dispose('kill');
    },
  },
  {
    name: '场景9: 快照导出 - per-key 统计、topN 排序、全局健康',
    run: async () => {
      const pb = new PartitionedAdaptiveBatcher<number, number>({
        perKeyMinBatchSize: 1,
        perKeyMaxBatchSize: 2,
        perKeyInitialBatchSize: 1,
        perKeyMinWaitMs: 2,
        perKeyMaxWaitMs: 10,
        perKeyMaxQueueSize: 10,
        perKeyMaxInflightBatches: 1,
        maxTotalQueueSize: 100,
        maxKeys: 10,
        statsWindowMs: 5000,
        flush: (key, batch) => {
          if (key === 'bad') {
            return batch.map(() => new PermanentFailure('一直失败')) as number[];
          }
          return batch.map((x) => x * 2);
        },
      });

      const proms: Promise<number>[] = [];
      for (let i = 0; i < 5; i++) {
        proms.push(pb.submit(i, { key: 'big' }));
      }
      for (let i = 0; i < 2; i++) {
        proms.push(pb.submit(i + 100, { key: 'small' }));
      }
      pb.submit(999, { key: 'bad' }).catch(() => {});
      pb.submit(888, { key: 'bad' }).catch(() => {});

      await Promise.allSettled(proms);
      await sleep(50);

      const snapshot = pb.getSnapshot({ topN: 3 });
      console.log(`  快照: totalKeys=${snapshot.totalKeys}, totalQueue=${snapshot.totalQueueSize}, globalHealth=${snapshot.globalHealth}`);
      console.log(`  topKeysByQueue: ${snapshot.topKeysByQueue.map((k) => `${k.key}(${k.queueSize})`).join(', ')}`);
      console.log(`  topKeysByFailure: ${snapshot.topKeysByFailure.map((k) => `${k.key}(${k.ewmaFailureRate})`).join(', ')}`);

      expect(snapshot.totalKeys, '有4个key?不对,应该是3个').toBeGreaterThanOrEqual(3);
      expect(snapshot.globalHealth, '全局健康状态').toBeTruthy();
      expect(snapshot.timestamp, '有时间戳').toBeGreaterThan(0);
      expect(snapshot.topKeysByQueue.length, 'topN<=3').toBeLessThanOrEqual(3);
      expect(snapshot.perKey['big'], 'big key有统计').toBeTruthy();
      expect(snapshot.perKey['small'], 'small key有统计').toBeTruthy();

      const badStats = pb.getKeyStats('bad');
      expect(badStats, 'bad key有统计').toBeTruthy();
      expect(badStats!.lastError != null, 'bad key有lastError').toBe(true);
      expect(badStats!.health.status !== 'healthy', 'bad key不健康').toBe(true);

      expect(snapshot.topKeysByFailure[0].key, '失败率最高的是bad').toBe('bad');

      await pb.dispose();
    },
  },
  {
    name: '场景10: flush 函数接收 attempt 参数',
    run: async () => {
      const attempts: number[] = [];
      const batcher = new AdaptiveBatcher<number, number>({
        minBatchSize: 2,
        maxBatchSize: 2,
        initialBatchSize: 2,
        minWaitMs: 5,
        maxWaitMs: 20,
        maxRetries: 2,
        retryDelayMs: 5,
        flush: (batch, attempt) => {
          attempts.push(attempt);
          if (attempt < 3) {
            return batch.map(() => new RetryableFailure('retry me')) as number[];
          }
          return batch.map((x) => x * 10);
        },
      });

      await Promise.all([batcher.submit(1), batcher.submit(2)]).catch(() => {});

      console.log(`  attempts: ${attempts.join(', ')}`);
      expect(attempts[0], '第一次attempt=1').toBe(1);
      expect(attempts.length, '总共3次attempt(1+2次重试)').toBe(3);
      expect(attempts[2], '第三次attempt=3').toBe(3);
      await batcher.dispose();
    },
  },

  // ========== v3 回归测试 ==========
  {
    name: '回归1: 基础攒批 + 双缓冲无阻塞',
    run: async () => {
      const batcher = new AdaptiveBatcher<number, number>({
        minBatchSize: 2,
        maxBatchSize: 20,
        initialBatchSize: 2,
        minWaitMs: 2,
        maxWaitMs: 20,
        maxInflightBatches: 1,
        flush: async (batch) => {
          await sleep(80);
          return batch.map((x) => x + 1000);
        },
      });

      const wave1 = Promise.all([batcher.submit(0), batcher.submit(1)]);
      await sleep(15);
      const wave2 = [2, 3, 4, 5, 6].map((i) => batcher.submit(i));
      const queueMid = batcher.metrics.currentQueueSize;
      expect(queueMid > 0, 'flush期间新请求无阻塞入队').toBeTruthy();

      const r1 = await wave1;
      const r2 = await Promise.all(wave2);
      expect(r1[0], 'wave1[0]=1000').toBe(1000);
      expect(r2.length, 'wave2有5个').toBe(5);

      await batcher.dispose();
    },
  },
  {
    name: '回归2: 可观测 + rollingStats + p50/p95 + stale',
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

      const proms: Promise<number>[] = [];
      for (let i = 0; i < 15; i++) {
        proms.push(batcher.submit(i));
        await sleep(3);
      }
      await Promise.all(proms);
      await sleep(50);

      const stats = batcher.rollingStats;
      expect(stats.p50QueuedWaitMs >= 0, 'p50Wait有值').toBeTruthy();
      expect(stats.p95QueuedWaitMs >= stats.p50QueuedWaitMs, 'p95>=p50').toBe(true);
      expect(stats.stale, '活跃时stale=false').toBe(false);

      const originalGetTime = Date.now;
      const now = originalGetTime();
      Date.now = () => now + 6000;
      const statsStale = batcher.rollingStats;
      expect(statsStale.stale, '6秒后stale=true').toBe(true);
      expect(statsStale.itemsProcessed, '过期后清零').toBe(0);
      Date.now = originalGetTime;

      await batcher.dispose();
    },
  },
  {
    name: '回归3: 背压三种策略 + 错误码',
    run: async () => {
      // reject
      const b1 = new AdaptiveBatcher<number, number>({
        minBatchSize: 2,
        maxBatchSize: 2,
        maxQueueSize: 3,
        maxInflightBatches: 1,
        overflowStrategy: 'reject',
        flush: async (b) => {
          await sleep(100);
          return b;
        },
      });
      const p1 = b1.submit(1);
      const p2 = b1.submit(2);
      const p3 = b1.submit(3);
      const err1 = await expectError(() => b1.submit(999));
      expect(err1.code, 'reject策略:QUEUE_OVERFLOW').toBe(BatcherErrorCode.QUEUE_OVERFLOW);
      await Promise.allSettled([p1, p2, p3]);
      await b1.dispose();

      // drop + fallback
      let dropped = 0;
      const b2 = new AdaptiveBatcher<number, number>({
        minBatchSize: 2,
        maxBatchSize: 2,
        maxQueueSize: 3,
        maxInflightBatches: 1,
        overflowStrategy: 'drop',
        flush: async (b) => {
          await sleep(100);
          return b;
        },
        fallback: (x) => {
          dropped++;
          return x + 500;
        },
      });
      const dp1 = b2.submit(1);
      const dp2 = b2.submit(2);
      const dp3 = b2.submit(3);
      const dp4 = await b2.submit(999);
      expect(dp4, 'drop+fallback返回降级值').toBe(1499);
      expect(dropped, '降级函数被调用').toBeGreaterThanOrEqual(1);
      await Promise.allSettled([dp1, dp2, dp3]);
      await b2.dispose();

      // drop 无降级不挂起
      const b3 = new AdaptiveBatcher<number, number>({
        minBatchSize: 2,
        maxBatchSize: 2,
        maxQueueSize: 3,
        maxInflightBatches: 1,
        overflowStrategy: 'drop',
        flush: async (b) => {
          await sleep(100);
          return b;
        },
      });
      const bp1 = b3.submit(1);
      const bp2 = b3.submit(2);
      const bp3 = b3.submit(3);
      const dropPromise = b3.submit(999);
      const race = await Promise.race([
        dropPromise.catch((e) => e),
        new Promise<string>((r) => setTimeout(() => r('timeout'), 100)),
      ]);
      expect(race instanceof BatcherError, '无降级drop立即抛错,不挂起').toBe(true);
      expect((race as BatcherError).code, '错误码DROPPED').toBe(BatcherErrorCode.DROPPED);
      await Promise.allSettled([bp1, bp2, bp3]);
      await b3.dispose();
    },
  },
  {
    name: '回归4: dispose 三策略 + 不卡死',
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
            console.log(`    flush 执行, size=${batch.length}`);
            await sleep(60);
            return batch.map((x) => x + 100);
          },
        });

        const promises: Promise<number>[] = [];
        for (let i = 0; i < 5; i++) {
          const p = batcher.submit(i);
          p.catch(() => {}); // prevent unhandledRejection in Node.js v24
          promises.push(p);
        }

        console.log(`  submit 完成, queue=${batcher.metrics.currentQueueSize}, inflight=${batcher.metrics.inflightBatches}`);
        await sleep(10);
        console.log(`  sleep 10ms 后, queue=${batcher.metrics.currentQueueSize}, inflight=${batcher.metrics.inflightBatches}`);
        
        const start = Date.now();
        await batcher.dispose(strategy);
        const duration = Date.now() - start;

        const settled = await Promise.allSettled(promises);
        console.log(`  dispose 完成, duration=${duration}ms, settled=${settled.length}`);

        expect(settled.length, '所有请求有归宿').toBe(5);
        expect(duration < 1000, `${strategy}不卡死`).toBe(true);

        if (strategy === 'kill') {
          expect(duration < 100, 'kill立即返回').toBe(true);
        }
      }
    },
  },
  {
    name: '回归5: 提交取消与超时',
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

      // 排队中取消
      const controller = new AbortController();
      const pCancel = batcher.submit(999, { signal: controller.signal });
      await sleep(5);
      controller.abort();
      const errCancel = await expectError(() => pCancel);
      expect(errCancel.code, '排队中取消:CANCELED').toBe(BatcherErrorCode.CANCELED);

      // 已取消的 signal
      const controller2 = new AbortController();
      controller2.abort();
      const errPre = await expectError(() => batcher.submit(777, { signal: controller2.signal }));
      expect(errPre.code, '已取消signal直接拒绝').toBe(BatcherErrorCode.CANCELED);

      await batcher.dispose();
    },
  },
  {
    name: '回归6: 回调错误隔离',
    run: async () => {
      const batcher = new AdaptiveBatcher<number, number>({
        minBatchSize: 2,
        maxBatchSize: 2,
        initialBatchSize: 2,
        minWaitMs: 5,
        maxWaitMs: 20,
        flush: async (batch) => batch.map((x) => x * 2),
        onFlush: () => {
          throw new Error('监控挂了');
        },
      });

      const results = await Promise.all([batcher.submit(1), batcher.submit(2)]);
      expect(results[0], 'onFlush错了不影响主流程').toBe(2);

      // 异步onFlush不卡forceFlush
      const b2 = new AdaptiveBatcher<number, number>({
        minBatchSize: 2,
        maxBatchSize: 2,
        initialBatchSize: 2,
        minWaitMs: 5,
        maxWaitMs: 20,
        flush: async (batch) => batch.map((x) => x * 2),
        onFlush: async () => {
          await sleep(500);
        },
      });
      await Promise.all([b2.submit(1), b2.submit(2)]);
      const start = Date.now();
      await b2.forceFlush();
      expect(Date.now() - start < 200, 'forceFlush不卡').toBe(true);

      await batcher.dispose();
      await b2.dispose();
    },
  },
  {
    name: '回归7: block策略关闭时快速返回',
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

      const start = Date.now();
      const pBlock = batcher.submit(999);
      await sleep(50);

      const disposePromise = batcher.dispose();
      const err = await expectError(() => pBlock);
      const duration = Date.now() - start;

      expect(err.code, 'block中dispose返回DISPOSED').toBe(BatcherErrorCode.DISPOSED);
      expect(duration < 500, '不会一直等').toBe(true);

      await disposePromise;
      await Promise.allSettled([p1, p2]);
    },
  },
];

(async () => {
  console.log('='.repeat(75));
  console.log('  自适应批量提交器 v4 - 可发 npm 版 测试验证');
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
