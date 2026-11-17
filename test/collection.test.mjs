import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Collector } from '../src/collect.mjs';
import { SourceClient } from '../src/transport.mjs';
import { startSource } from '../source/service.mjs';
import { startApplication } from '../src/app.mjs';

const FULL_VALUE = 'VISIT-2026;temperature=23.5;humidity=60;serial=lab-a';

function fakeClient(routes) {
  return { async request(path) {
    const hit = routes[path];
    if (hit === undefined) return { status: 404, headers: {}, bytes: Buffer.from('not found'), url: path };
    const payload = typeof hit === 'function' ? hit() : hit;
    return { status: 200, headers: {}, bytes: Buffer.from(JSON.stringify(payload)), url: path };
  } };
}

function fragment(offset, version, valueHex, complete) {
  return { connection: 'conn-1', databaseHash: 'db-a', service: '181A', characteristic: '2A6E',
    handle: 33, offset, mtu: 23, version, valueHex, complete };
}

const directory181A = { databaseHash: 'db-a', services: [{ uuid: '181A', start: 16, end: 40,
  characteristics: [{ uuid: '2A6E', handle: 33 }] }] };

test('长读分段按版本归集，其它版本的分片不拼入同一完整值', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'collect-mix-'));
  try {
    let switched = false;
    const client = fakeClient({
      '/gatt/services': directory181A,
      '/gatt/events': { events: [] },
      '/gatt/read/33?offset=0': () => fragment(0, switched ? 'record-8' : 'record-7', 'aabb', false),
      '/gatt/read/33?offset=2': () => { switched = true; return fragment(2, 'record-8', 'ccdd', false); },
      '/gatt/read/33?offset=4': fragment(4, 'record-8', 'eeff', true)
    });
    const collector = await Collector.open({ root: join(directory, 'collection'), device: 'unit' });
    const report = await collector.runPass(client);
    const value = report.values.find(item => item.characteristic === '2A6E');
    assert.equal(value.version, 'record-8');
    assert.equal(value.valueHex, 'aabbccddeeff');
    assert.equal(value.complete, true);
    const identity = collector.state.identities['2A6E@db-a'];
    assert.equal(identity.assemblies['record-7'].fragments.length, 1);
    assert.equal(identity.assemblies['record-7'].fragments[0].offset, 0);
    assert.ok(!report.values.some(item => item.version === 'record-7'));
  } finally { await rm(directory, { recursive: true, force: true }); }
});

test('晚到的通知不自动视为比刚完成的长读更新', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'collect-notify-'));
  try {
    const client = fakeClient({
      '/gatt/services': directory181A,
      '/gatt/events': { events: [{ kind: 'notification', handle: 33, version: 'record-8', valueHex: '1500' }] },
      '/gatt/read/33?offset=0': fragment(0, 'record-7', 'aabb', true)
    });
    const collector = await Collector.open({ root: join(directory, 'collection'), device: 'unit' });
    const report = await collector.runPass(client);
    const current = report.values.find(value => value.current);
    assert.equal(current.version, 'record-7');
    assert.equal(current.confirmed, true);
    const notified = report.values.find(value => value.version === 'record-8');
    assert.equal(notified.confirmed, false);
    assert.equal(notified.current, false);
    assert.ok(report.pending.some(item => item.reason === 'diverged'));
  } finally { await rm(directory, { recursive: true, force: true }); }
});

test('目录代次变更后重新核对，旧目录迟到片段不归入新目录', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'collect-transition-'));
  const source = await startSource({ scenario: 'transition' });
  try {
    const client = new SourceClient(source.base);
    const device = source.base;
    const first = await Collector.open({ root: join(directory, 'collection'), device });
    const report1 = await first.runPass(client);
    assert.equal(report1.databaseHash, 'db-a');
    assert.equal(report1.announcedDatabaseHash, 'db-b');
    const confirmed = report1.values.filter(value => value.confirmed);
    assert.equal(confirmed.length, 1);
    assert.equal(confirmed[0].version, 'record-7');
    assert.equal(Buffer.from(confirmed[0].valueHex, 'hex').toString(), FULL_VALUE);
    const notified = report1.values.find(value => value.version === 'record-8');
    assert.equal(notified.confirmed, false);
    assert.equal(notified.current, false);
    assert.ok(report1.pending.some(item => item.reason === 'diverged'));

    await client.control({ action: 'advance' });
    const second = await Collector.open({ root: join(directory, 'collection'), device });
    const report2 = await second.runPass(client);
    assert.equal(report2.databaseHash, 'db-b');
    assert.ok(report2.generationChanges.some(change => change.from === 'db-a' && change.to === 'db-b'));
    const moved = report2.values.find(value => value.databaseHash === 'db-b' && value.characteristic === '2A6E');
    assert.equal(moved.confirmed, true);
    assert.equal(moved.valueHex, '1500');
    const old = report2.values.find(value => value.databaseHash === 'db-a'
      && value.characteristic === '2A6E' && value.version === 'record-7');
    assert.equal(old.superseded, true);
    assert.ok(report2.strays.some(item => item.received.databaseHash === 'db-a'
      && item.requested.databaseHash === 'db-b'));
    const blocked = report2.pending.find(item => item.characteristic === '2A6F' && item.databaseHash === 'db-b');
    assert.equal(blocked.reason, 'blocked');
    assert.ok(!report2.values.some(value => value.databaseHash === 'db-b' && value.characteristic === '2A6F'));
  } finally {
    await source.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test('断线后按持久化进度续传，重连不沿用旧句柄', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'collect-drop-'));
  const source = await startSource({ scenario: 'drop', domain: false });
  try {
    const client = new SourceClient(source.base);
    const device = source.base;
    const first = await Collector.open({ root: join(directory, 'collection'), device });
    const report1 = await first.runPass(client);
    assert.ok(report1.lastSession.aborted);
    assert.equal(report1.values.length, 0);
    assert.ok(report1.pending.some(item => item.reason === 'error'));

    await client.control({ action: 'advance' });
    const reopened = await Collector.open({ root: join(directory, 'collection'), device });
    const report2 = await reopened.runPass(client);
    assert.equal(report2.databaseHash, 'db-a');
    const value = report2.values.find(item => item.characteristic === '2A6E');
    assert.equal(value.complete, true);
    assert.equal(Buffer.from(value.valueHex, 'hex').toString(), FULL_VALUE);
    assert.deepEqual([...value.connections].sort(), ['conn-1', 'conn-2']);
    assert.equal(report2.pending.length, 0);
    const audit = await client.control({ action: 'inspect' });
    const phase1 = audit.requests.filter(item => item.phase === 1).map(item => item.path);
    assert.ok(phase1.includes('/gatt/services'));
    assert.ok(phase1.includes('/gatt/read/35?offset=22'));
    assert.ok(!phase1.some(path => path.startsWith('/gatt/read/33')));
    assert.ok(!phase1.includes('/gatt/read/35?offset=0'));
    assert.ok(phase1.indexOf('/gatt/services') < phase1.findIndex(path => path.startsWith('/gatt/read/')));
  } finally {
    await source.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test('采集报告落盘，来源断开后仍可离线回查', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'collect-http-'));
  const source = await startSource();
  const app = await startApplication({ source: source.base, root: directory });
  try {
    const response = await fetch(`${app.base}/collection`, { method: 'POST', body: '{}' });
    assert.equal(response.status, 200);
    const report = await response.json();
    assert.equal(report.databaseHash, 'db-a');
    assert.equal(report.announcedDatabaseHash, 'db-b');
    assert.ok(report.values.some(value => value.confirmed && value.version === 'record-7'));
    assert.ok(report.values.some(value => !value.confirmed && value.version === 'record-8'));
    assert.ok(report.pending.some(item => item.reason === 'diverged'));
  } finally { await app.close(); }
  await source.close();
  const offline = await startApplication({ source: source.base, root: directory });
  try {
    const report = await (await fetch(`${offline.base}/collection`)).json();
    assert.ok(report.values.some(value => value.confirmed && value.version === 'record-7'));
    const failed = await fetch(`${offline.base}/collection`, { method: 'POST', body: '{}' });
    assert.equal(failed.status, 422);
  } finally {
    await offline.close();
    await rm(directory, { recursive: true, force: true });
  }
});
