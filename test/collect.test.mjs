import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { startSource } from '../source/service.mjs';
import { SourceClient } from '../src/transport.mjs';
import { decode } from '../src/decode.mjs';
import { Collector, compareVersions } from '../src/collect.mjs';
import { startApplication } from '../src/app.mjs';

const LONG_VALUE = 'VISIT-2026;temperature=23.5;humidity=60;serial=lab-a';
const HEX_0 = '56495349542d323032363b74656d7065726174757265';
const HEX_22 = '3d32332e353b68756d69646974793d36303b73657269';
const HEX_44 = '616c3d6c61622d61';

const silentClient = { request: async () => { throw new Error('不应发起来源请求'); } };

function fragment(overrides) {
  return { connection: 'conn-1', databaseHash: 'db-a', service: '181A', characteristic: '2A6E',
    handle: 33, offset: 0, mtu: 23, version: 'record-7', valueHex: '', complete: false, ...overrides };
}

async function tempRoot() {
  return mkdtemp(join(tmpdir(), 'collect-test-'));
}

test('记录版本按同前缀数字比较，无法比较时不猜测新旧', () => {
  assert.equal(compareVersions('record-7', 'record-8'), -1);
  assert.equal(compareVersions('record-8', 'record-7'), 1);
  assert.equal(compareVersions('record-7', 'record-7'), 0);
  assert.equal(compareVersions('record-7', 'other-7'), null);
  assert.equal(compareVersions('record-7', null), null);
});

test('长值按版本一致的连续分片拼接，另一版本的片段被隔离', async () => {
  const root = await tempRoot();
  try {
    const collector = await new Collector({ client: silentClient, root, targets: ['2A6E'] }).initialize();
    assert.equal((await collector.ingestFragment(fragment({ offset: 0, valueHex: HEX_0 }))).reason, 'started');
    const stray = await collector.ingestFragment(fragment({ offset: 22, version: 'record-8', valueHex: '1500' }));
    assert.equal(stray.accepted, false);
    assert.equal(stray.reason, 'stray');
    assert.equal((await collector.ingestFragment(fragment({ offset: 22, valueHex: HEX_22 }))).reason, 'appended');
    assert.equal((await collector.ingestFragment(fragment({ offset: 44, valueHex: HEX_44, complete: true }))).accepted, true);
    const assemblies = Object.values(collector.state.assemblies);
    assert.equal(assemblies.length, 1);
    assert.equal(assemblies[0].complete, true);
    assert.equal(Buffer.from(assemblies[0].parts.map(part => part.hex).join(''), 'hex').toString('utf8'), LONG_VALUE);
    // 偏移缺口同样被拒绝并标记重读
    await collector.ingestFragment(fragment({ characteristic: '2A6F', handle: 35, offset: 0, valueHex: HEX_0 }));
    const gap = await collector.ingestFragment(fragment({ characteristic: '2A6F', handle: 35, offset: 44, valueHex: HEX_44, complete: true }));
    assert.equal(gap.reason, 'gap');
    assert.ok(collector.state.reRead['2A6F']);
  } finally { await rm(root, { recursive: true, force: true }); }
});

test('迟到通知不覆盖刚完成的长读，版本更新的通知才生效', async () => {
  const root = await tempRoot();
  try {
    const collector = await new Collector({ client: silentClient, root, targets: ['2A6E'] }).initialize();
    collector.state.directory = { databaseHash: 'db-a', verifiedAt: new Date().toISOString(),
      services: [{ uuid: '181A', start: 16, end: 40, characteristics: [{ uuid: '2A6E', handle: 33 }] }] };
    await collector.ingestFragment(fragment({ offset: 0, version: 'record-8', valueHex: '1500', complete: true }));
    await collector.onNotification({ kind: 'notification', handle: 33, version: 'record-7', valueHex: '9999' });
    let current = collector.currentFor('db-a', '2A6E');
    assert.equal(current.version, 'record-8');
    assert.equal(current.source, 'read');
    await collector.onNotification({ kind: 'notification', handle: 33, version: 'record-9', valueHex: '9999' });
    current = collector.currentFor('db-a', '2A6E');
    assert.equal(current.version, 'record-9');
    assert.equal(current.source, 'notification');
    // 同版本但内容不同：不覆盖，标记重读核对
    await collector.onNotification({ kind: 'notification', handle: 33, version: 'record-9', valueHex: 'aaaa' });
    assert.ok(collector.state.reRead['2A6E']);
    assert.equal(collector.currentFor('db-a', '2A6E').parts[0].hex, '9999');
  } finally { await rm(root, { recursive: true, force: true }); }
});

test('目录哈希变化才触发重读标记，同哈希重复核对不动作', async () => {
  const root = await tempRoot();
  const dirA = { databaseHash: 'db-a', services: [{ uuid: '181A', start: 16, end: 40, characteristics: [{ uuid: '2A6E', handle: 33 }] }] };
  const dirB = { databaseHash: 'db-b', services: [{ uuid: '181A', start: 16, end: 40, characteristics: [{ uuid: '2A6E', handle: 37 }] }] };
  let served = dirA;
  const client = { request: async path => {
    assert.equal(path, '/gatt/services');
    return { status: 200, bytes: Buffer.from(JSON.stringify(served)) };
  } };
  try {
    const collector = await new Collector({ client, root, targets: ['2A6E'] }).initialize();
    await collector.ingestFragment(fragment({ offset: 0, valueHex: '1500', complete: true }));
    await collector.verifyDirectory();
    await collector.verifyDirectory();
    assert.deepEqual(Object.keys(collector.state.reRead), []);
    served = dirB;
    await collector.verifyDirectory();
    assert.ok(collector.state.reRead['2A6E']);
    assert.equal(collector.state.directory.databaseHash, 'db-b');
  } finally { await rm(root, { recursive: true, force: true }); }
});

test('连续采集分段取得长值，通知按版本决定当前值，原件可回读', async () => {
  const root = await tempRoot();
  const source = await startSource();
  const app = await startApplication({ source: source.base, root });
  try {
    const status = await fetch(`${app.base}/collection/run`, { method: 'POST', body: '{}' }).then(response => response.json());
    assert.equal(status.databaseHash, 'db-a');
    assert.equal(status.settled, true);
    assert.deepEqual(status.needsReRead, []);
    const row = status.values.find(value => value.characteristic === '2A6E');
    assert.equal(row.current.version, 'record-8');
    assert.equal(row.current.source, 'notification');
    const read = row.assemblies.find(asm => asm.source === 'read');
    assert.equal(read.complete, true);
    assert.equal(read.bytes, 52);
    const again = await fetch(`${app.base}/collection/status`).then(response => response.json());
    assert.equal(again.settled, true);
    const receipts = await fetch(`${app.base}/receipts`).then(response => response.json());
    const saved = receipts.find(receipt => receipt.preview?.version === 'record-7' && receipt.preview?.source === 'read');
    assert.ok(saved);
    const body = await fetch(`${app.base}/objects/${saved.digest}`).then(response => response.text());
    assert.equal(body, LONG_VALUE);
  } finally {
    await app.close();
    await source.close();
    await rm(root, { recursive: true, force: true });
  }
});

test('目录迁移后重新核对受影响服务，旧目录迟到响应不归入新目录', async () => {
  const root = await tempRoot();
  const source = await startSource({ scenario: 'transition' });
  const app = await startApplication({ source: source.base, root });
  const client = new SourceClient(source.base);
  try {
    const first = await fetch(`${app.base}/collection/run`, { method: 'POST', body: '{}' }).then(response => response.json());
    const before = first.values.find(value => value.databaseHash === 'db-a' && value.characteristic === '2A6E');
    assert.equal(before.current.version, 'record-8');
    assert.equal(before.assemblies.find(asm => asm.source === 'read').complete, true);
    await client.control({ action: 'advance' });
    const second = await fetch(`${app.base}/collection/run`, { method: 'POST', body: '{}' }).then(response => response.json());
    assert.equal(second.databaseHash, 'db-b');
    assert.equal(second.settled, true);
    assert.deepEqual(second.needsReRead, []);
    const current = second.values.find(value => value.databaseHash === 'db-b' && value.characteristic === '2A6E');
    assert.equal(current.current.version, 'record-8');
    assert.equal(current.current.source, 'read');
    assert.equal(current.current.valueHex, '1500');
    assert.equal(current.superseded, false);
    assert.equal(second.values.find(value => value.databaseHash === 'db-a' && value.characteristic === '2A6E').superseded, true);
    const inspect = await client.control({ action: 'inspect' });
    const phase1 = inspect.requests.filter(item => item.phase === 1).map(item => item.path);
    assert.ok(phase1.some(path => path.startsWith('/gatt/services')));
    assert.ok(phase1.some(path => path.startsWith('/gatt/read/37')));
    assert.ok(!phase1.some(path => path.startsWith('/gatt/read/33')));
    // 旧目录下发出的请求此时才返回：按响应自身身份归档，不污染旧值也不归入新目录
    const late = decode((await client.request('/gatt/read/33?offset=22')).bytes);
    assert.equal(late.databaseHash, 'db-a');
    const outcome = await app.collector.ingestFragment(late, { issuedUnder: 'db-a' });
    assert.equal(outcome.reason, 'duplicate');
    const after = app.collector.status();
    const oldRow = after.values.find(value => value.databaseHash === 'db-a' && value.characteristic === '2A6E');
    assert.equal(oldRow.assemblies.find(asm => asm.source === 'read').parts, 3);
    // 换目录后同一句柄的响应属于另一特征，归入新身份而不是旧值
    const remapped = decode((await client.request('/gatt/read/33?offset=0')).bytes);
    assert.equal(remapped.characteristic, '2A6F');
    assert.equal(remapped.databaseHash, 'db-b');
    const filed = await app.collector.ingestFragment(remapped, { issuedUnder: 'db-a' });
    assert.equal(filed.accepted, true);
    const final = app.collector.status();
    const newRow = final.values.find(value => value.databaseHash === 'db-b' && value.characteristic === '2A6F');
    assert.ok(newRow);
    assert.equal(newRow.current.complete, false);
    assert.ok(final.pending.some(item => item.characteristic === '2A6F' && item.databaseHash === 'db-b'));
    assert.ok(final.audit.some(entry => entry.note.includes('不归入新目录')));
  } finally {
    await app.close();
    await source.close();
    await rm(root, { recursive: true, force: true });
  }
});

test('断线后状态可续传，重连先重新核对目录再读取，不轻信旧句柄', async () => {
  const root = await tempRoot();
  const source = await startSource({ scenario: 'drop' });
  const client = new SourceClient(source.base);
  let app = await startApplication({ source: source.base, root });
  try {
    const first = await fetch(`${app.base}/collection/run`, { method: 'POST', body: '{}' }).then(response => response.json());
    assert.equal(first.settled, false);
    assert.ok(first.transportError);
    assert.ok(first.pending.some(item => item.connection === 'conn-1' && item.nextOffset === 22));
    const saved = JSON.parse(await readFile(join(root, 'collection', 'state.json'), 'utf8'));
    assert.equal(saved.directory.databaseHash, 'db-a');
    await app.close();
    await client.control({ action: 'advance' });
    app = await startApplication({ source: source.base, root });
    assert.equal(app.collector.state.directory.databaseHash, 'db-a');
    const second = await fetch(`${app.base}/collection/run`, { method: 'POST', body: '{}' }).then(response => response.json());
    assert.equal(second.settled, true);
    assert.equal(second.databaseHash, 'db-b');
    const row = second.values.find(value => value.databaseHash === 'db-b' && value.characteristic === '2A6E');
    assert.equal(row.current.complete, true);
    assert.equal(row.current.valueHex, '1500');
    assert.ok(second.pending.some(item => item.connection === 'conn-1' && item.databaseHash === 'db-a'));
    const inspect = await client.control({ action: 'inspect' });
    const phase1 = inspect.requests.filter(item => item.phase === 1).map(item => item.path);
    assert.ok(phase1.some(path => path.startsWith('/gatt/services')));
    assert.ok(phase1.some(path => path.startsWith('/gatt/read/37')));
    assert.ok(!phase1.some(path => path.startsWith('/gatt/read/33')));
  } finally {
    await app.close();
    await source.close();
    await rm(root, { recursive: true, force: true });
  }
});
