import test from 'node:test';
import assert from 'node:assert/strict';
import { startSource } from '../source/service.mjs';
import { SourceClient } from '../src/transport.mjs';
import { decode } from '../src/decode.mjs';

test('数据库变更重映射句柄，旧长读片段仍可能迟到', async () => {
  const source = await startSource({ scenario: 'transition' });
  try {
    const client = new SourceClient(source.base);
    const first = decode((await client.request('/gatt/read/33?offset=0')).bytes);
    const second = decode((await client.request('/gatt/read/33?offset=22')).bytes);
    assert.equal(first.version, second.version); assert.equal(second.offset, 22);
    await client.control({ action: 'advance' });
    const services = JSON.parse((await client.request('/gatt/services')).bytes);
    assert.equal(services.services[0].characteristics.find(item => item.uuid === '2A6E').handle, 37);
    const stale = decode((await client.request('/gatt/read/33?offset=22')).bytes);
    const current = decode((await client.request('/gatt/read/37?offset=0')).bytes);
    assert.notEqual(stale.databaseHash, current.databaseHash);
  } finally { await source.close(); }
});
