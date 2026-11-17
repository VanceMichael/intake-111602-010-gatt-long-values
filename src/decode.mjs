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
  return { connection: value.connection, databaseHash: value.databaseHash, service: value.service ?? null,
    characteristic: value.characteristic, handle: value.handle, offset: value.offset,
    version: value.version, valueHex: value.valueHex, complete: value.complete === true };
}

export function decodeDirectory(bytes) {
  const value = document(bytes);
  if (typeof value.databaseHash !== 'string' || !Array.isArray(value.services)) throw new Error('服务目录无效');
  const characteristics = [];
  for (const service of value.services) {
    if (typeof service?.uuid !== 'string' || !Array.isArray(service.characteristics)) throw new Error('服务目录无效');
    for (const item of service.characteristics) {
      if (typeof item?.uuid !== 'string' || !Number.isInteger(item.handle)) throw new Error('服务目录无效');
      characteristics.push({ service: service.uuid, uuid: item.uuid, handle: item.handle });
    }
  }
  return { databaseHash: value.databaseHash, characteristics };
}

export function decodeEvents(bytes) {
  const value = document(bytes);
  if (!Array.isArray(value.events)) throw new Error('事件资料无效');
  return value.events.map(event => {
    if (event?.kind === 'notification') {
      if (!Number.isInteger(event.handle) || !/^(?:[a-f0-9]{2})*$/i.test(event.valueHex || '')) throw new Error('通知事件无效');
      return { kind: 'notification', handle: event.handle, version: event.version ?? null, valueHex: event.valueHex };
    }
    if (event?.kind === 'service-changed') {
      if (!Number.isInteger(event.start) || !Number.isInteger(event.end) || typeof event.databaseHash !== 'string') throw new Error('目录变更事件无效');
      return { kind: 'service-changed', start: event.start, end: event.end, databaseHash: event.databaseHash };
    }
    throw new Error('未知事件类型');
  });
}
