function document(bytes) {
  const value = JSON.parse(bytes.toString('utf8'));
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('资料不是对象');
  return value;
}

export function decode(bytes) {
  const value = document(bytes);
  if (!Number.isInteger(value.handle) || !Number.isInteger(value.offset) || !/^(?:[a-f0-9]{2})*$/i.test(value.valueHex || '')) throw new Error('属性分片无效');
  const data = Buffer.from(value.valueHex, 'hex');
  if (data.length > value.mtu - 1) throw new Error('属性分片超过协商长度');
  return { connection: value.connection, databaseHash: value.databaseHash, characteristic: value.characteristic,
    handle: value.handle, offset: value.offset, version: value.version, valueHex: value.valueHex, complete: value.complete === true };
}
