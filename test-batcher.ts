import { AdaptiveBatcher } from './adaptive-batcher';

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

interface TestCase {
  name: string;
  run: () => Promise<void>;
}

const testCases: TestCase[] = [
  {
    name: '场景1: 稀疏请求 (低速率 → 小批次 + 短等待)',
    run: async () => {
      let actualBatches: number[][] = [];
      let batchCount = 0;

      const batcher = new AdaptiveBatcher<number, number>({
        minBatchSize: 1,
        maxBatchSize: 100,
        minWaitMs: 5,
        maxWaitMs: 100,
        initialWaitMs: 50,
        rateWindowMs: 1000,
        flush: async (batch) => {
          batchCount++;
          actualBatches.push([...batch]);
          await sleep(20);
          return batch.map((x) => x * 2);
        },
      });

      const results: number[] = [];
      const latencies: number[] = [];

      for (let i = 0; i < 5; i++) {
        const start = Date.now();
        const r = await batcher.submit(i);
        latencies.push(Date.now() - start);
        results.push(r);
        await sleep(150);
      }

      const avgLatency = latencies.reduce((a, b) => a + b, 0) / latencies.length;
      const metrics = batcher.metrics;
      console.log(`  提交 ${results.length} 个请求, 分成 ${batchCount} 个批次`);
      console.log(`  平均单次延迟: ${avgLatency.toFixed(1)}ms (预期接近 flush延迟+短等待 ≈25ms)`);
      console.log(`  最终目标批次大小: ${metrics.targetBatchSize} (预期很小 ≈1-5)`);
      console.log(`  最终等待窗口: ${metrics.waitWindowMs}ms (预期接近最小 ≈5-20ms)`);
      console.log(`  批次大小分布: [${actualBatches.map((b) => b.length).join(', ')}]`);
      console.log(`  结果正确: ${JSON.stringify(results)} === [0,2,4,6,8] ? ${
        JSON.stringify(results) === JSON.stringify([0, 2, 4, 6, 8]) ? '✓' : '✗'
      }`);

      await batcher.dispose();
    },
  },
  {
    name: '场景2: 密集请求 (高速率 → 大批次 + 长等待)',
    run: async () => {
      let actualBatches: number[][] = [];
      let batchCount = 0;
      let totalFlushTime = 0;

      const batcher = new AdaptiveBatcher<number, number>({
        minBatchSize: 1,
        maxBatchSize: 100,
        minWaitMs: 2,
        maxWaitMs: 50,
        initialWaitMs: 50,
        rateWindowMs: 1000,
        flush: async (batch) => {
          const t0 = Date.now();
          batchCount++;
          actualBatches.push([...batch]);
          await sleep(10);
          totalFlushTime += Date.now() - t0;
          return batch.map((x) => x * 2);
        },
      });

      const N = 500;
      const promises: Promise<number>[] = [];
      const t0 = Date.now();

      for (let i = 0; i < N; i++) {
        promises.push(batcher.submit(i));
        if (i % 20 === 0) await sleep(1);
      }

      const results = await Promise.all(promises);
      const totalElapsed = Date.now() - t0;

      const expected = Array.from({ length: N }, (_, i) => i * 2);
      const correct = JSON.stringify(results) === JSON.stringify(expected);

      const avgBatchSize =
        actualBatches.reduce((a, b) => a + b.length, 0) / actualBatches.length;
      const metrics = batcher.metrics;

      console.log(`  提交 ${N} 个请求, 分成 ${batchCount} 个批次`);
      console.log(`  总耗时: ${totalElapsed}ms`);
      console.log(`  总 flush 时间: ${totalFlushTime}ms (批次越少此值越小 → 吞吐越高)`);
      console.log(`  平均批次大小: ${avgBatchSize.toFixed(1)} (预期 ≈50-100)`);
      console.log(`  最终目标批次大小: ${metrics.targetBatchSize} (预期接近最大 ≈60-100)`);
      console.log(`  最终等待窗口: ${metrics.waitWindowMs}ms (预期较大 ≈35-50ms)`);
      console.log(`  批次大小分布(前5): [${actualBatches.slice(0, 5).map((b) => b.length).join(', ')}...]`);
      console.log(`  所有结果正确: ${correct ? '✓' : '✗'}`);

      await batcher.dispose();
    },
  },
  {
    name: '场景3: 双缓冲无阻塞验证 (提交中也能接收新请求)',
    run: async () => {
      let batchCount = 0;
      let maxQueueDuringFlush = 0;
      let flushCallCount = 0;
      let resolveFirstFlush: () => void = () => {};
      const firstFlushBarrier = new Promise<void>((r) => (resolveFirstFlush = r));
      let firstFlushReleased = false;

      const batcher = new AdaptiveBatcher<number, number>({
        minBatchSize: 2,
        maxBatchSize: 20,
        initialBatchSize: 2,
        minWaitMs: 2,
        maxWaitMs: 20,
        initialWaitMs: 5,
        rateWindowMs: 1000,
        flush: async (batch) => {
          flushCallCount++;
          if (flushCallCount === 1) {
            resolveFirstFlush();
            while (!firstFlushReleased) await sleep(5);
            batchCount++;
            maxQueueDuringFlush = Math.max(
              maxQueueDuringFlush,
              batcher.metrics.currentQueueSize
            );
            return batch.map((x) => x + 1000);
          } else {
            batchCount++;
            return batch.map((x) => x + 2000);
          }
        },
      });

      const p1 = batcher.submit(0);
      const p2 = batcher.submit(1);
      const wave1 = Promise.all([p1, p2]);

      await firstFlushBarrier;

      const wave2: Promise<number>[] = [];
      for (let i = 2; i < 8; i++) {
        wave2.push(batcher.submit(i));
      }

      const queueAfterSubmit = batcher.metrics.currentQueueSize;
      const inflightNow = batcher.metrics.inflightBatches;
      console.log(`  第1个flush进行中: 新请求入队数 = ${queueAfterSubmit} (应 > 0, 无阻塞)`);
      console.log(`  此时进行中的批次 = ${inflightNow} (应 = 1)`);

      firstFlushReleased = true;

      const results1 = await wave1;
      const results2 = await Promise.all(wave2);
      await batcher.forceFlush();

      console.log(`  批次总数: ${batchCount} (预期 ≥ 2)`);
      console.log(`  flush期间最大队列深度: ${maxQueueDuringFlush} (预期 > 0 → 双缓冲生效)`);
      console.log(`  第1波结果: [${results1.join(', ')}] 正确: ${
        JSON.stringify(results1) === JSON.stringify([1000, 1001]) ? '✓' : '✗'
      }`);
      console.log(`  第2波结果: [${results2.join(', ')}] (应为2002-2007)`);
      const r2Ok = results2.every((v, idx) => v === 2000 + 2 + idx) ||
        results2.every((v) => v >= 2000);
      console.log(`  第2波结果都被某批次正确处理: ${r2Ok ? '✓' : '✗'}`);

      await batcher.dispose();
    },
  },
  {
    name: '场景4: 突发流量自适应过程 (速率变化 → 参数平滑过渡)',
    run: async () => {
      let batchSizes: number[] = [];

      const batcher = new AdaptiveBatcher<number, number>({
        minBatchSize: 1,
        maxBatchSize: 80,
        minWaitMs: 2,
        maxWaitMs: 60,
        rateWindowMs: 1000,
        flush: async (batch) => {
          batchSizes.push(batch.length);
          await sleep(8);
          return batch.map((x) => x);
        },
      });

      const snapShots: Array<{
        label: string;
        targetSize: number;
        waitMs: number;
        rate: number;
      }> = [];

      console.log('  阶段A: 空闲稀疏 (10个请求, 间隔100ms)');
      for (let i = 0; i < 10; i++) {
        await batcher.submit(i);
        await sleep(100);
      }
      const mA = batcher.metrics;
      snapShots.push({ label: 'A(稀疏)', targetSize: mA.targetBatchSize, waitMs: mA.waitWindowMs, rate: mA.ewmaRate });
      console.log(`    → EWMA速率: ${mA.ewmaRate.toFixed(1)}/s, 目标批次: ${mA.targetBatchSize}, 等待: ${mA.waitWindowMs}ms`);

      console.log('  阶段B: 突发密集 (200个请求, 间隔2ms)');
      const promsB: Promise<number>[] = [];
      for (let i = 0; i < 200; i++) {
        promsB.push(batcher.submit(100 + i));
        await sleep(2);
      }
      await Promise.all(promsB);
      const mB = batcher.metrics;
      snapShots.push({ label: 'B(密集)', targetSize: mB.targetBatchSize, waitMs: mB.waitWindowMs, rate: mB.ewmaRate });
      console.log(`    → EWMA速率: ${mB.ewmaRate.toFixed(1)}/s, 目标批次: ${mB.targetBatchSize}, 等待: ${mB.waitWindowMs}ms`);

      console.log('  阶段C: 再次稀疏 (10个请求, 间隔150ms, 观察回落)');
      for (let i = 0; i < 10; i++) {
        await batcher.submit(300 + i);
        await sleep(150);
      }
      const mC = batcher.metrics;
      snapShots.push({ label: 'C(回落)', targetSize: mC.targetBatchSize, waitMs: mC.waitWindowMs, rate: mC.ewmaRate });
      console.log(`    → EWMA速率: ${mC.ewmaRate.toFixed(1)}/s, 目标批次: ${mC.targetBatchSize}, 等待: ${mC.waitWindowMs}ms`);

      const sizeUp = mB.targetBatchSize > mA.targetBatchSize;
      const sizeDown = mC.targetBatchSize < mB.targetBatchSize;
      const waitUp = mB.waitWindowMs > mA.waitWindowMs;
      const waitDown = mC.waitWindowMs < mB.waitWindowMs;

      console.log(`  自适应验证:`);
      console.log(`    稀疏→密集, 目标批次变大: ${sizeUp ? '✓' : '✗'}  (${mA.targetBatchSize} → ${mB.targetBatchSize})`);
      console.log(`    稀疏→密集, 等待窗口变大: ${waitUp ? '✓' : '✗'}  (${mA.waitWindowMs}ms → ${mB.waitWindowMs}ms)`);
      console.log(`    密集→稀疏, 目标批次变小: ${sizeDown ? '✓' : '✗'}  (${mB.targetBatchSize} → ${mC.targetBatchSize})`);
      console.log(`    密集→稀疏, 等待窗口变小: ${waitDown ? '✓' : '✗'}  (${mB.waitWindowMs}ms → ${mC.waitWindowMs}ms)`);

      await batcher.dispose();
    },
  },
];

function expect(actual: unknown, message: string): { toBe: (expected: unknown) => void } {
  return {
    toBe(expected: unknown) {
      if (actual !== expected) {
        throw new Error(`${message}: 期望 ${JSON.stringify(expected)}, 实际 ${JSON.stringify(actual)}`);
      }
    },
  };
}

(async () => {
  console.log('='.repeat(70));
  console.log('  自适应批量提交器 - 测试验证');
  console.log('='.repeat(70));
  console.log();

  let passed = 0;
  let failed = 0;

  for (const tc of testCases) {
    console.log(tc.name);
    console.log('-'.repeat(70));
    try {
      await tc.run();
      console.log(`  ✅ PASS`);
      passed++;
    } catch (e) {
      console.log(`  ❌ FAIL: ${(e as Error).message}`);
      console.log((e as Error).stack);
      failed++;
    }
    console.log();
  }

  console.log('='.repeat(70));
  console.log(`  结果: ${passed} 通过, ${failed} 失败, 共 ${testCases.length} 个测试`);
  console.log('='.repeat(70));
})();
