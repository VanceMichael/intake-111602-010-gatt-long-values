import { createHash, randomUUID } from 'node:crypto';
import { mkdir, open, readFile, rename } from 'node:fs/promises';
import { join } from 'node:path';
import { decode } from './decode.mjs';

const VERSION_TAIL = /^(.*?)(\d+)$/;

// 记录版本比较：同前缀数字后缀可比，其余情形不可比（返回 null）。
export function compareVersions(a, b) {
  if (a === b) return 0;
  if (typeof a !== 'string' || typeof b !== 'string' || !a || !b) return null;
  const pa = VERSION_TAIL.exec(a);
  const pb = VERSION_TAIL.exec(b);
  if (!pa || !pb || pa[1] !== pb[1]) return null;
  const na = BigInt(pa[2]);
  const nb = BigInt(pb[2]);
  return na === nb ? 0 : na < nb ? -1 : 1;
}

function now() {
  return new Date().toISOString();
}

async function atomicWrite(path, bytes) {
  const temporary = `${path}.${randomUUID()}.tmp`;
  const handle = await open(temporary, 'wx');
  try {
    await handle.writeFile(bytes);
    await handle.sync();
  } finally {
    await handle.close();
  }
  await rename(temporary, path);
}

export function parseDirectory(bytes) {
  let value;
  try {
    value = JSON.parse(Buffer.from(bytes).toString('utf8'));
  } catch {
    throw new Error('服务目录不是有效 JSON');
  }
  if (!value || typeof value !== 'object' || typeof value.databaseHash !== 'string'
    || !value.databaseHash || !Array.isArray(value.services)) throw new Error('服务目录无效');
  const services = value.services.map(service => {
    if (!service || typeof service.uuid !== 'string' || !Number.isInteger(service.start)
      || !Number.isInteger(service.end) || !Array.isArray(service.characteristics)) throw new Error('服务目录无效');
    const characteristics = service.characteristics.map(item => {
      if (!item || typeof item.uuid !== 'string' || !Number.isInteger(item.handle)) throw new Error('服务目录无效');
      return { uuid: item.uuid, handle: item.handle };
    });
    return { uuid: service.uuid, start: service.start, end: service.end, characteristics };
  });
  return { databaseHash: value.databaseHash, services };
}

export function resolveCharacteristic(directory, uuid) {
  for (const service of directory?.services ?? []) {
    const found = service.characteristics.find(item => item.uuid === uuid);
    if (found) return { service: service.uuid, handle: found.handle };
  }
  return null;
}

export function resolveHandle(directory, handle) {
  for (const service of directory?.services ?? []) {
    const found = service.characteristics.find(item => item.handle === handle);
    if (found) return { service: service.uuid, characteristic: found.uuid };
  }
  return null;
}

function parseEvents(bytes) {
  const value = JSON.parse(Buffer.from(bytes).toString('utf8'));
  if (!value || !Array.isArray(value.events)) throw new Error('事件响应无效');
  return value.events;
}

function eventHash(event) {
  const keys = event && typeof event === 'object' ? Object.keys(event).sort() : undefined;
  return createHash('sha256').update(JSON.stringify(event, keys)).digest('hex');
}

// 当前值排序：完整优先于未完成，未破损优先于破损，版本新者优先，其余按时间。
function rankAssemblies(list) {
  return [...list].sort((a, b) => {
    if (a.complete !== b.complete) return a.complete ? -1 : 1;
    if (a.broken !== b.broken) return a.broken ? 1 : -1;
    const cmp = compareVersions(a.version, b.version);
    if (cmp !== null && cmp !== 0) return cmp > 0 ? -1 : 1;
    return String(b.updatedAt).localeCompare(String(a.updatedAt));
  });
}

function freshState() {
  return { directory: null, assemblies: {}, reRead: {}, errors: {},
    processedEvents: [], eventsApplied: 0, eventsSkipped: 0, audit: [] };
}

// 连续资料采集器。读取身份 = 设备连接 + 特征（属性语义）+ 数据库哈希；
// 句柄只是某一目录版本下的寻址方式，不作为身份。
export class Collector {
  constructor({ client, root, catalog = null, targets = [], maxCycles = 6, maxSegments = 64 } = {}) {
    if (!client) throw new TypeError('采集器需要来源客户端');
    if (!root) throw new TypeError('采集器需要状态目录');
    this.client = client;
    this.catalog = catalog;
    this.targets = [...targets];
    this.maxCycles = maxCycles;
    this.maxSegments = maxSegments;
    this.stateDir = join(root, 'collection');
    this.statePath = join(this.stateDir, 'state.json');
    this.state = freshState();
    this.busy = Promise.resolve();
  }

  async initialize() {
    await mkdir(this.stateDir, { recursive: true });
    try {
      const loaded = JSON.parse(await readFile(this.statePath, 'utf8'));
      this.state = { ...freshState(), ...loaded };
    } catch (error) {
      if (error.code !== 'ENOENT') throw error;
    }
    return this;
  }

  async persist() {
    await mkdir(this.stateDir, { recursive: true });
    await atomicWrite(this.statePath, Buffer.from(JSON.stringify(this.state, null, 2)));
  }

  audit(note) {
    this.state.audit.push({ at: now(), note });
    if (this.state.audit.length > 100) this.state.audit.splice(0, this.state.audit.length - 100);
  }

  // 核对服务目录。哈希变化时把旧目录下已知的特征标记为待重读；
  // 哈希未变但句柄映射变化属于来源不一致，同样标记。重连后必须先经过这里再使用句柄。
  async verifyDirectory() {
    const result = await this.client.request('/gatt/services');
    if (result.status !== 200) throw new Error(`服务目录读取失败：${result.status}`);
    const directory = parseDirectory(result.bytes);
    const before = this.state.directory;
    if (!before) {
      this.state.directory = { ...directory, verifiedAt: now() };
      this.audit(`已核对服务目录 ${directory.databaseHash}`);
    } else if (before.databaseHash !== directory.databaseHash) {
      const affected = new Set();
      for (const asm of Object.values(this.state.assemblies)) {
        if (asm.databaseHash === before.databaseHash) affected.add(asm.characteristic);
      }
      for (const characteristic of affected) this.state.reRead[characteristic] = true;
      this.state.directory = { ...directory, verifiedAt: now() };
      this.audit(`数据库哈希变化 ${before.databaseHash} → ${directory.databaseHash}，重新核对受影响服务：${[...affected].join('、') || '无'}`);
    } else {
      for (const service of directory.services) {
        for (const item of service.characteristics) {
          const previous = resolveCharacteristic(before, item.uuid);
          if (previous && previous.handle !== item.handle) {
            this.state.reRead[item.uuid] = true;
            this.audit(`目录哈希未变但 ${item.uuid} 句柄 ${previous.handle} → ${item.handle}，需重读`);
          }
        }
      }
      this.state.directory = { ...directory, verifiedAt: now() };
    }
    await this.persist();
    return this.state.directory;
  }

  async drainEvents() {
    const result = await this.client.request('/gatt/events');
    if (result.status === 404) return false;
    if (result.status !== 200) throw new Error(`事件读取失败：${result.status}`);
    let progressed = false;
    for (const event of parseEvents(result.bytes)) {
      const hash = eventHash(event);
      if (this.state.processedEvents.includes(hash)) {
        this.state.eventsSkipped += 1;
        continue;
      }
      this.state.processedEvents.push(hash);
      if (this.state.processedEvents.length > 512) this.state.processedEvents.shift();
      await this.applyEvent(event);
      this.state.eventsApplied += 1;
      progressed = true;
    }
    await this.persist();
    return progressed;
  }

  async applyEvent(event) {
    if (event?.kind === 'notification') return this.onNotification(event);
    if (event?.kind === 'service-changed') return this.onServiceChanged(event);
    this.audit(`忽略无法识别的事件：${JSON.stringify(event)}`);
  }

  // 通知按收到时已核对的目录解析句柄；只有版本确实更新才覆盖当前值，
  // 迟到不代表更新。通知不携带连接身份，归入空连接身份，不与读取片段混排。
  async onNotification(event) {
    const directory = this.state.directory;
    if (!directory) return this.audit('通知到达时目录尚未核对，已丢弃');
    const resolved = resolveHandle(directory, event.handle);
    if (!resolved) return this.audit(`通知句柄 ${event.handle} 不在已核对目录中，已丢弃`);
    if (typeof event.version !== 'string' || !event.version || !/^(?:[a-f0-9]{2})*$/i.test(event.valueHex || '')) {
      return this.audit('通知缺少记录版本或内容无效，已丢弃');
    }
    const valueHex = event.valueHex.toLowerCase();
    const { characteristic, service } = resolved;
    const current = this.currentFor(directory.databaseHash, characteristic);
    if (current) {
      const cmp = compareVersions(event.version, current.version);
      if (cmp === null) {
        this.state.reRead[characteristic] = true;
        await this.persist();
        return this.audit(`通知版本 ${event.version} 与当前版本 ${current.version ?? '无'} 无法比较，${characteristic} 需重读核对`);
      }
      if (cmp < 0) return this.audit(`通知版本 ${event.version} 不新于当前 ${current.version}，迟到不代表更新，已忽略`);
      if (cmp === 0) {
        const existing = Buffer.from(current.parts.map(part => part.hex).join(''), 'hex');
        if (existing.equals(Buffer.from(valueHex, 'hex'))) return this.audit(`通知版本 ${event.version} 与当前一致，重复通知已忽略`);
        this.state.reRead[characteristic] = true;
        await this.persist();
        return this.audit(`通知版本 ${event.version} 与当前相同但内容不同，${characteristic} 需重读核对`);
      }
    }
    const key = `|${directory.databaseHash}|${characteristic}|${event.version}`;
    this.state.assemblies[key] = { key, connection: null, databaseHash: directory.databaseHash,
      characteristic, service, handle: event.handle, version: event.version,
      parts: [{ offset: 0, hex: valueHex }], nextOffset: valueHex.length / 2,
      complete: true, broken: false, source: 'notification', updatedAt: now(), digest: null };
    this.audit(`通知版本 ${event.version} 更新 ${characteristic}（句柄 ${event.handle}，目录 ${directory.databaseHash}）`);
    await this.storeValue(this.state.assemblies[key]);
    await this.persist();
  }

  // Service Changed 范围是句柄闭区间。收到后重新核对服务目录，
  // 区间受影响或映射变化的特征标记为待重读。
  async onServiceChanged(event) {
    if (!Number.isInteger(event?.start) || !Number.isInteger(event?.end)) return this.audit('Service Changed 事件无效，已忽略');
    const before = this.state.directory;
    const inRange = directory => (directory?.services ?? []).flatMap(service => service.characteristics)
      .filter(item => item.handle >= event.start && item.handle <= event.end).map(item => item.uuid);
    const affected = new Set(inRange(before));
    await this.verifyDirectory();
    const after = this.state.directory;
    for (const uuid of inRange(after)) affected.add(uuid);
    const hashChanged = !before || before.databaseHash !== after.databaseHash;
    for (const uuid of affected) {
      const oldHandle = before ? resolveCharacteristic(before, uuid)?.handle : undefined;
      const newHandle = resolveCharacteristic(after, uuid)?.handle;
      if (hashChanged || oldHandle !== newHandle) this.state.reRead[uuid] = true;
    }
    const claimed = typeof event.databaseHash === 'string' && event.databaseHash !== after.databaseHash
      ? `（事件声称 ${event.databaseHash}）` : '';
    this.audit(hashChanged
      ? `Service Changed [${event.start}, ${event.end}]${claimed}，目录 ${before?.databaseHash ?? '无'} → ${after.databaseHash}，受影响特征需重读`
      : `Service Changed [${event.start}, ${event.end}]${claimed}，核对后目录哈希仍为 ${after.databaseHash}`);
    await this.persist();
  }

  // 分片按响应自身身份（连接 + 数据库哈希 + 特征 + 记录版本）归档。
  // 旧目录下发出的请求即便迟到，也只归入响应自身的身份，不并入新目录的值。
  async ingestFragment(fragment, { issuedUnder } = {}) {
    const f = fragment;
    if (!f || typeof f.characteristic !== 'string' || typeof f.databaseHash !== 'string'
      || !Number.isInteger(f.offset) || !/^(?:[a-f0-9]{2})*$/i.test(f.valueHex || '')) {
      throw new TypeError('属性分片无效');
    }
    const valueHex = f.valueHex.toLowerCase();
    if (issuedUnder && issuedUnder !== f.databaseHash) {
      this.audit(`请求在 ${issuedUnder} 目录下发出，响应属于 ${f.databaseHash}，按响应自身身份归档，不归入新目录`);
    }
    if (typeof f.version !== 'string' || !f.version) {
      if (!(f.offset === 0 && f.complete === true)) {
        this.state.reRead[f.characteristic] = true;
        await this.persist();
        this.audit(`特征 ${f.characteristic} 无记录版本，分片相邻不能证明一致，需重读`);
        return { accepted: false, reason: 'unversioned', key: null };
      }
    }
    const key = `${f.connection ?? ''}|${f.databaseHash}|${f.characteristic}|${f.version ?? ''}`;
    let asm = this.state.assemblies[key];
    if (!asm) {
      if (f.offset !== 0) {
        this.audit(`迟到片段（${f.characteristic}@${f.version} 偏移 ${f.offset}）无法起始新值，已隔离`);
        return { accepted: false, reason: 'stray', key };
      }
      asm = { key, connection: f.connection ?? null, databaseHash: f.databaseHash, characteristic: f.characteristic,
        service: f.service ?? null, handle: f.handle ?? null, version: f.version ?? null,
        parts: [], nextOffset: 0, complete: false, broken: false, source: 'read', updatedAt: now(), digest: null };
      this.state.assemblies[key] = asm;
    }
    const existing = asm.parts.find(part => part.offset === f.offset);
    if (existing) {
      if (existing.hex !== valueHex) {
        asm.broken = true;
        this.state.reRead[f.characteristic] = true;
        this.audit(`特征 ${f.characteristic} 偏移 ${f.offset} 内容冲突，值已标记重读`);
        await this.persist();
        return { accepted: false, reason: 'conflict', key };
      }
      return { accepted: true, reason: 'duplicate', key };
    }
    if (asm.complete) {
      this.audit(`特征 ${f.characteristic} 完整值不再接受偏移 ${f.offset} 的追加`);
      return { accepted: false, reason: 'after-complete', key };
    }
    if (f.offset !== asm.nextOffset) {
      asm.broken = true;
      this.state.reRead[f.characteristic] = true;
      this.audit(`特征 ${f.characteristic} 分片偏移 ${f.offset} 与期望 ${asm.nextOffset} 不符，值已标记重读`);
      await this.persist();
      return { accepted: false, reason: f.offset > asm.nextOffset ? 'gap' : 'misaligned', key };
    }
    asm.parts.push({ offset: f.offset, hex: valueHex });
    asm.nextOffset = f.offset + valueHex.length / 2;
    asm.updatedAt = now();
    if (f.complete === true) {
      asm.complete = true;
      await this.storeValue(asm);
    }
    await this.persist();
    return { accepted: true, reason: f.offset === 0 ? 'started' : 'appended', key };
  }

  async storeValue(asm) {
    if (!this.catalog || asm.digest) return;
    const bytes = Buffer.from(asm.parts.map(part => part.hex).join(''), 'hex');
    try {
      const receipt = await this.catalog.accept({
        source: `gatt:${asm.connection ?? 'notification'}:${asm.databaseHash}:${asm.characteristic}@${asm.version ?? 'unversioned'}`,
        bytes, mediaType: 'application/octet-stream',
        preview: { connection: asm.connection, databaseHash: asm.databaseHash, characteristic: asm.characteristic,
          service: asm.service, version: asm.version, source: asm.source } });
      asm.digest = receipt.digest;
    } catch (error) {
      this.audit(`完整值入库失败（${asm.characteristic}@${asm.version}）：${error.message}`);
    }
  }

  // 长属性分段读取。句柄只取自当前已核对的目录；读取途中响应身份变化即中止，
  // 变化的分片仍按自身身份归档，不并入本次值。
  async readCharacteristic(characteristic) {
    const directory = this.state.directory;
    if (!directory) throw new Error('尚未核对服务目录');
    const resolved = resolveCharacteristic(directory, characteristic);
    if (!resolved) {
      delete this.state.reRead[characteristic];
      this.state.errors[characteristic] = '特征不在已核对目录中';
      this.audit(`特征 ${characteristic} 不在目录 ${directory.databaseHash} 中，跳过重读`);
      await this.persist();
      return false;
    }
    let offset = 0;
    let passKey = null;
    const visited = new Set();
    for (let segment = 0; segment < this.maxSegments; segment += 1) {
      if (visited.has(offset)) {
        this.state.errors[characteristic] = '分片偏移循环';
        this.audit(`特征 ${characteristic} 分片偏移循环，中止本次读取`);
        await this.persist();
        return false;
      }
      visited.add(offset);
      const result = await this.client.request(`/gatt/read/${resolved.handle}?offset=${offset}`);
      if (result.status !== 200) throw new Error(`读取 ${characteristic}（句柄 ${resolved.handle}）失败：${result.status}`);
      const fragment = decode(result.bytes);
      const outcome = await this.ingestFragment(fragment, { issuedUnder: directory.databaseHash });
      if (passKey === null) {
        if (!outcome.accepted) {
          this.state.errors[characteristic] = `首段未建立读取：${outcome.reason}`;
          await this.persist();
          return false;
        }
        passKey = outcome.key;
      } else if (outcome.key !== passKey) {
        this.state.reRead[characteristic] = true;
        this.audit(`读取 ${characteristic} 途中响应身份变化，本次读取中止，旧目录片段不并入新值`);
        await this.persist();
        return false;
      } else if (!outcome.accepted && outcome.reason !== 'duplicate') {
        this.state.errors[characteristic] = `分片未被接受：${outcome.reason}`;
        await this.persist();
        return false;
      }
      if (fragment.complete === true) {
        delete this.state.reRead[characteristic];
        delete this.state.errors[characteristic];
        await this.persist();
        return true;
      }
      offset = fragment.offset + Math.floor((fragment.valueHex || '').length / 2);
    }
    this.state.errors[characteristic] = '超过分段上限';
    await this.persist();
    return false;
  }

  needsRead(characteristic) {
    if (this.state.reRead[characteristic]) return true;
    const directory = this.state.directory;
    if (!directory || !resolveCharacteristic(directory, characteristic)) return false;
    return !Object.values(this.state.assemblies).some(asm => asm.complete && !asm.broken
      && asm.source === 'read' && asm.databaseHash === directory.databaseHash
      && asm.characteristic === characteristic);
  }

  isSettled(targets) {
    return Boolean(this.state.directory) && targets.every(characteristic => !this.needsRead(characteristic));
  }

  run({ targets } = {}) {
    const list = [...(targets ?? this.targets)];
    const operation = this.busy.then(() => this.runExclusive(list));
    this.busy = operation.catch(() => {});
    return operation;
  }

  // 连续采集：每轮先核对目录，再处理事件，最后分段读取待办特征。
  // 断线等传输失败保留状态退出，下次运行先重新核对目录再继续。
  async runExclusive(targets) {
    for (let cycle = 0; cycle < this.maxCycles; cycle += 1) {
      try {
        await this.verifyDirectory();
        await this.drainEvents();
        for (const characteristic of targets) {
          if (!this.needsRead(characteristic)) continue;
          await this.readCharacteristic(characteristic);
        }
      } catch (error) {
        this.state.errors.transport = error.message;
        this.audit(`采集中断：${error.message}，状态已保存，可断线后续传`);
        await this.persist();
        break;
      }
      delete this.state.errors.transport;
      if (this.isSettled(targets)) break;
    }
    await this.persist();
    return this.status(targets);
  }

  currentFor(databaseHash, characteristic) {
    const list = Object.values(this.state.assemblies)
      .filter(asm => asm.databaseHash === databaseHash && asm.characteristic === characteristic);
    return list.length ? rankAssemblies(list)[0] : null;
  }

  summarize(asm) {
    return { version: asm.version, connection: asm.connection, source: asm.source, handle: asm.handle,
      bytes: asm.nextOffset, complete: asm.complete, digest: asm.digest ?? null, updatedAt: asm.updatedAt,
      valueHex: asm.parts.map(part => part.hex).join('') };
  }

  // 现场查询：每个身份下的当前值、分段进度、哪些完整、哪些还要重读。
  status(targets = this.targets) {
    const directory = this.state.directory;
    const groups = new Map();
    for (const asm of Object.values(this.state.assemblies)) {
      const key = `${asm.databaseHash}|${asm.characteristic}`;
      if (!groups.has(key)) groups.set(key, []);
      groups.get(key).push(asm);
    }
    const values = [];
    for (const [key, list] of [...groups.entries()].sort()) {
      const [databaseHash, characteristic] = key.split('|');
      const ranked = rankAssemblies(list);
      const current = ranked[0];
      const resolved = directory && databaseHash === directory.databaseHash
        ? resolveCharacteristic(directory, characteristic) : null;
      values.push({ characteristic, databaseHash,
        service: current.service ?? resolved?.service ?? null,
        handle: resolved?.handle ?? current.handle ?? null,
        superseded: directory ? databaseHash !== directory.databaseHash : false,
        current: this.summarize(current),
        assemblies: ranked.map(asm => ({ connection: asm.connection, version: asm.version, source: asm.source,
          bytes: asm.nextOffset, parts: asm.parts.length, complete: asm.complete, broken: asm.broken, updatedAt: asm.updatedAt })),
        lastError: this.state.errors[characteristic] ?? null });
    }
    for (const characteristic of targets) {
      const covered = values.some(value => value.characteristic === characteristic
        && (!directory || value.databaseHash === directory.databaseHash));
      if (!covered) {
        const resolved = directory ? resolveCharacteristic(directory, characteristic) : null;
        values.push({ characteristic, databaseHash: directory?.databaseHash ?? null,
          service: resolved?.service ?? null, handle: resolved?.handle ?? null,
          superseded: false, current: null, assemblies: [],
          lastError: this.state.errors[characteristic] ?? null });
      }
    }
    const needsReRead = Object.keys(this.state.reRead).filter(characteristic => this.state.reRead[characteristic]).sort();
    const pending = Object.values(this.state.assemblies).filter(asm => !asm.complete)
      .map(asm => ({ connection: asm.connection, databaseHash: asm.databaseHash, characteristic: asm.characteristic,
        version: asm.version, nextOffset: asm.nextOffset, broken: asm.broken }))
      .sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
    return { databaseHash: directory?.databaseHash ?? null, verifiedAt: directory?.verifiedAt ?? null,
      targets: [...targets], settled: this.isSettled(targets), needsReRead,
      transportError: this.state.errors.transport ?? null, values, pending,
      events: { applied: this.state.eventsApplied, skipped: this.state.eventsSkipped },
      audit: this.state.audit.slice(-20) };
  }
}
