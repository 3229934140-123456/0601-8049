import {
  AdaptiveBatcher,
  BatcherError,
  BatcherErrorCode,
  FlushEvent,
} from './adaptive-batcher';

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

interface TestCase {
  name: string;
  run: () => Promise<void>;
}

function expect(actual: unknown, message: string): { toBe: (expected: unknown) => void; toBeTruthy: () => void; toBeFalsy: () => void } {
  return {
    toBe(expected: unknown) {
      if (actual !== expected) {
        throw new Error(`${message}: 期望 ${JSON.stringify(expected)}, 实际 ${JSON.stringify(actual)}`);
      }
    },
    toBeTruthy() {
      if (!actual) {
        throw new Error(`${message}: 期望真值, 实际 ${JSON.stringify(actual)}`);
      }
    },
    toBeFalsy() {
      if (actual) {
        throw new Error(`${message}: 期望假值, 实际 ${JSON.stringify(actual)}`);
      }
    },
  };
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

      console.log(`  onFlush 收到事件数: ${events.length} (预期 > 0)`);
      expect(events.length > 0, '应收到 onFlush 事件').toBeTruthy();

      const firstEvent = events[0];
      console.log(`  首个事件: batchSize=${firstEvent.batchSize}, queuedWaitMs=${firstEvent.queuedWaitMs.toFixed(0)}ms, flushDurationMs=${firstEvent.flushDurationMs.toFixed(0)}ms, success=${firstEvent.success}`);
      expect(firstEvent.batchSize > 0, 'batchSize > 0').toBeTruthy();
      expect(firstEvent.flushDurationMs >= 15, 'flushDurationMs >= 15ms (有波动余量)').toBeTruthy();
      expect(firstEvent.success, 'success=true').toBe(true);
      expect(firstEvent.items.length > 0, 'items 非空').toBeTruthy();
      expect(firstEvent.results!.length === firstEvent.items.length, 'results 与 items 等长').toBe(true);

      const stats = batcher.rollingStats;
      console.log(`  滚动统计: avgBatchSize=${stats.avgBatchSize}, avgQueuedWait=${stats.avgQueuedWaitMs.toFixed(1)}ms, avgFlush=${stats.avgFlushDurationMs.toFixed(1)}ms, failureRate=${stats.failureRate}, throughput=${stats.throughputPerSecond}/s, processed=${stats.itemsProcessed}个/${stats.batchesProcessed}批`);
      expect(stats.itemsProcessed, 'itemsProcessed=7').toBe(7);
      expect(stats.batchesProcessed >= 1, 'batchesProcessed >= 1').toBeTruthy();
      expect(stats.failureRate, 'failureRate=0').toBe(0);
      expect(stats.throughputPerSecond > 0, 'throughput > 0').toBeTruthy();

      const mode = batcher.mode;
      console.log(`  当前模式: ${mode} (low-latency/balanced/high-throughput)`);
      expect(['low-latency', 'balanced', 'high-throughput'].includes(mode), 'mode 枚举合法').toBe(true);

      const metrics = batcher.metrics;
      console.log(`  扩展 metrics: ewmaFlushDuration=${metrics.ewmaFlushDurationMs}ms, ewmaFailureRate=${metrics.ewmaFailureRate}, capacityUsed=${metrics.capacityUsedPercent}%`);
      expect(metrics.ewmaFlushDurationMs >= 10, 'ewmaFlushDuration >= 10ms (EWMA从0收敛中)').toBeTruthy();

      await batcher.dispose();
    },
  },
  {
    name: '场景B1: 背压策略 - 超出maxQueueSize用reject模式, 错误码区分',
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
      const metricsMid = batcher.metrics;
      console.log(`  队列积压中: inflight=${metricsMid.inflightBatches}, queue=${metricsMid.currentQueueSize}, capacityUsed=${metricsMid.capacityUsedPercent}%`);

      let overflowCaught = false;
      let caughtCode: BatcherErrorCode | null = null;
      let caughtIsRetryable: boolean | null = null;
      try {
        await batcher.submit(999);
      } catch (e) {
        if (e instanceof BatcherError) {
          overflowCaught = true;
          caughtCode = e.code;
          caughtIsRetryable = e.retryable;
          console.log(`  正确捕获到 BatcherError: code=${e.code}, message=${e.message}`);
          console.log(`  区分限流 vs 下游失败: code=${e.code} === QUEUE_OVERFLOW 是限流，FLUSH_FAILED 是下游错`);
        }
      }

      expect(overflowCaught, '应捕获到队列溢出错误').toBe(true);
      expect(caughtCode, '错误码应为 QUEUE_OVERFLOW').toBe(BatcherErrorCode.QUEUE_OVERFLOW);
      expect(caughtIsRetryable, '溢出错误应为可重试').toBe(true);

      let downstreamCaught = false;
      let downstreamCode: BatcherErrorCode | null = null;
      const badBatcher = new AdaptiveBatcher<number, number>({
        minBatchSize: 1,
        maxBatchSize: 1,
        maxQueueSize: 10,
        flush: async () => {
          throw new Error('下游数据库挂了');
        },
      });
      try {
        await badBatcher.submit(1);
      } catch (e) {
        if (e instanceof BatcherError) {
          downstreamCaught = true;
          downstreamCode = e.code;
          console.log(`  下游失败错误: code=${e.code}, cause=${(e as unknown as { cause: Error }).cause?.message}`);
        }
      }
      expect(downstreamCaught, '应捕获到下游失败').toBe(true);
      expect(downstreamCode, '错误码应为 FLUSH_FAILED').toBe(BatcherErrorCode.FLUSH_FAILED);

      const results1 = await Promise.all([p1, p2, p3]);
      const results2 = await Promise.all([p4, p5, p6]);
      console.log(`  正常请求结果: [${results1.join(',')}] [${results2.join(',')}]`);

      await batcher.dispose();
      await badBatcher.dispose();
    },
  },
  {
    name: '场景B2: 背压策略 - drop模式 + fallback降级',
    run: async () => {
      const drops: number[] = [];
      const batcher = new AdaptiveBatcher<number, number>({
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

      const p1 = batcher.submit(1);
      const p2 = batcher.submit(2);
      await sleep(5);

      const p3 = batcher.submit(3);
      const p4 = batcher.submit(4);
      const p5 = batcher.submit(99);

      const r1 = await p1;
      const r2 = await p2;
      const r3 = await p3;
      const r4 = await p4;
      const r5 = await p5;

      console.log(`  正常处理: [${r1},${r2}] [${r3},${r4}] (应为101/102, 103/104)`);
      console.log(`  降级处理: ${r5} (应为599), 触发降级的items: [${drops.join(',')}]`);

      expect(r1, 'p1=101').toBe(101);
      expect(r5, 'p5=599').toBe(599);
      expect(drops.includes(99), 'drops包含99').toBe(true);

      await batcher.dispose();
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
          if (failMode) {
            throw new Error('模拟下游失败');
          }
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
      console.log(`    → 目标批次=${m1.targetBatchSize}, 等待窗口=${m1.waitWindowMs.toFixed(1)}ms, flush耗时EWMA=${m1.ewmaFlushDurationMs}ms, EWMA速率=${m1.ewmaRate}/s`);
      const size1 = m1.targetBatchSize;
      expect(size1 > 20, '阶段1批次应较大 (>20)').toBeTruthy();

      console.log('  阶段2: 下游变慢(flush=500ms), 批次应收缩');
      slowMode = true;
      await batcher.forceFlush();
      for (let i = 100; i < 130; i++) {
        void batcher.submit(i);
        await sleep(5);
      }
      await sleep(1100);
      const m2 = batcher.metrics;
      console.log(`    → 目标批次=${m2.targetBatchSize}, 等待窗口=${m2.waitWindowMs.toFixed(1)}ms, flush耗时EWMA=${m2.ewmaFlushDurationMs}ms`);
      expect(m2.ewmaFlushDurationMs > 200, 'flush耗时EWMA应>200ms').toBeTruthy();
      expect(m2.targetBatchSize < size1 * 0.9, '下游变慢后批次应收缩至少10%').toBe(true);

      console.log('  阶段3: 下游开始失败, 批次应进一步收缩');
      failMode = true;
      await batcher.forceFlush();
      let rejected = 0;
      for (let i = 200; i < 220; i++) {
        batcher.submit(i).catch(() => { rejected++; });
        await sleep(5);
      }
      await sleep(300);
      const m3 = batcher.metrics;
      console.log(`    → 目标批次=${m3.targetBatchSize}, 等待窗口=${m3.waitWindowMs.toFixed(1)}ms, 失败率EWMA=${m3.ewmaFailureRate}, 已失败=${rejected}个`);
      expect(m3.ewmaFailureRate > 0, '失败率>0').toBe(true);
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
      console.log(`    → 目标批次=${m4.targetBatchSize}, 等待窗口=${m4.waitWindowMs.toFixed(1)}ms, flush耗时EWMA=${m4.ewmaFlushDurationMs}ms`);
      expect(m4.targetBatchSize > m3.targetBatchSize, '恢复后批次应回升').toBe(true);

      await batcher.dispose();
    },
  },
  {
    name: '场景D: 收尾不卡死 - maxInflight=1, 慢请求期间dispose正常结束',
    run: async () => {
      const results: { id: number; status: string; value?: number; error?: string }[] = [];

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
      console.log(`  dispose前: inflight=${metricsBefore.inflightBatches}, queue=${metricsBefore.currentQueueSize}`);
      expect(metricsBefore.inflightBatches, '此时inflight=1').toBe(1);
      expect(metricsBefore.currentQueueSize > 0, '队列仍有积压').toBe(true);

      const disposeStart = Date.now();
      const disposePromise = batcher.dispose();
      const timeout = new Promise<never>((_, r) =>
        setTimeout(() => r(new Error('dispose 卡死了!') as never), 2000)
      );

      await Promise.race([disposePromise, timeout]);
      const disposeDuration = Date.now() - disposeStart;
      console.log(`  dispose 耗时: ${disposeDuration}ms (应 < 2000ms, 实际未卡死)`);
      expect(disposeDuration < 2000, 'dispose 不应卡死').toBe(true);

      console.log(`  所有请求归宿: resolved=${results.filter((r) => r.status === 'resolved').length}, rejected=${results.filter((r) => r.status === 'rejected').length}, total=${results.length}`);
      expect(results.length, '所有10个请求都有归宿').toBe(10);
      expect(results.every((r) => r.status === 'resolved' || r.status === 'rejected'), '每个请求都resolve或reject').toBe(true);

      const disposedRejects = results.filter((r) => r.status === 'rejected' && r.error?.includes('DISPOSED'));
      console.log(`  因dispose被reject的请求: ${disposedRejects.length}个 (如果队列还没清完的话)`);
    },
  },
  {
    name: '场景E: 收尾不卡死 - forceFlush在并发占满时也能完成',
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
      console.log(`  forceFlush前: inflight=${m.inflightBatches}, queue=${m.currentQueueSize}`);
      expect(m.inflightBatches, 'inflight=1').toBe(1);
      expect(m.currentQueueSize > 0, '队列有积压').toBe(true);

      const flushStart = Date.now();
      const flushPromise = batcher.forceFlush();
      const timeout = new Promise<never>((_, r) =>
        setTimeout(() => r(new Error('forceFlush 卡死了!') as never), 2000)
      );
      await Promise.race([flushPromise, timeout]);
      const flushDuration = Date.now() - flushStart;
      console.log(`  forceFlush 耗时: ${flushDuration}ms (应 < 2000ms)`);
      expect(flushDuration < 2000, 'forceFlush 不应卡死').toBe(true);

      const m2 = batcher.metrics;
      console.log(`  forceFlush后: inflight=${m2.inflightBatches}, queue=${m2.currentQueueSize}`);
      expect(m2.inflightBatches, 'inflight=0').toBe(0);
      expect(m2.currentQueueSize, 'queue=0').toBe(0);

      const results = await Promise.all(proms);
      console.log(`  所有结果: [${results.join(', ')}]`);
      expect(results.length, '7个结果').toBe(7);

      await batcher.dispose();
    },
  },
  {
    name: '回归测试: 双缓冲无阻塞、稀疏/密集自适应 (老功能)',
    run: async () => {
      let flushInProgress = false;
      let maxQueueDuringFlush = 0;
      const batcher = new AdaptiveBatcher<number, number>({
        minBatchSize: 2,
        maxBatchSize: 20,
        initialBatchSize: 2,
        minWaitMs: 2,
        maxWaitMs: 20,
        maxInflightBatches: 1,
        flush: async (batch) => {
          flushInProgress = true;
          await sleep(80);
          maxQueueDuringFlush = Math.max(maxQueueDuringFlush, batcher.metrics.currentQueueSize);
          flushInProgress = false;
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

      const stats = batcher.rollingStats;
      console.log(`  回归测试通过: 批次=${stats.batchesProcessed}, 处理=${stats.itemsProcessed}个, 最大队列=${maxQueueDuringFlush}`);

      await batcher.dispose();
    },
  },
];

(async () => {
  console.log('='.repeat(75));
  console.log('  自适应批量提交器 v2 - 线上可用版 测试验证');
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
      console.log(`  ❌ FAIL: ${(e as Error).message}`);
      console.log((e as Error).stack);
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
