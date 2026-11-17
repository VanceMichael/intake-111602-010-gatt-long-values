import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { startSource } from '../source/service.mjs';
import { startApplication } from '../src/app.mjs';
import { SourceClient } from '../src/transport.mjs';

const directory = await mkdtemp(join(tmpdir(), 'intake-'));
const source = await startSource({ scenario: 'transition' });
const app = await startApplication({ source: source.base, root: directory });
try {
  const preview = await fetch(`${app.base}/preview`).then(response => response.json());
  process.stdout.write(`当前资料预览\n${JSON.stringify(preview, null, 2)}\n`);
  const saved = await fetch(`${app.base}/receipts`, { method: 'POST', body: '{}' }).then(response => response.json());
  const body = await fetch(`${app.base}/objects/${saved.digest}`).then(response => response.arrayBuffer());
  if (body.byteLength !== saved.bytes) throw new Error('原始资料长度不符');
  process.stdout.write(`原件已接收：${saved.digest}，${saved.bytes} 字节\n`);

  const collect = () => fetch(`${app.base}/collection`, { method: 'POST', body: '{}' }).then(response => response.json());
  const summarize = report => {
    process.stdout.write(`目录 ${report.databaseHash}（设备通告 ${report.announcedDatabaseHash ?? '无'}）\n`);
    for (const value of report.values) {
      process.stdout.write(`  值 ${value.characteristic}@${value.databaseHash} ${value.version}：`
        + `${value.confirmed ? '读取确认' : '仅通知'}${value.current ? '（当前）' : ''}${value.superseded ? '（旧目录）' : ''}\n`);
    }
    for (const item of report.pending) {
      process.stdout.write(`  待重读 ${item.characteristic}@${item.databaseHash}：${item.detail}\n`);
    }
  };
  process.stdout.write('升级前采集\n');
  summarize(await collect());
  await new SourceClient(source.base).control({ action: 'advance' });
  process.stdout.write('来源已推进发布日期（模拟固件升级）\n升级后采集\n');
  summarize(await collect());
} finally {
  await app.close();
  await source.close();
  await rm(directory, { recursive: true, force: true });
}
