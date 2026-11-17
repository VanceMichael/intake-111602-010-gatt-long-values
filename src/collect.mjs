import { createHash, randomUUID } from 'node:crypto';
import { mkdir, open, readFile, rename } from 'node:fs/promises';
import { join } from 'node:path';
import { decode, decodeDirectory, decodeEvents } from './decode.mjs';

const KEEP = 50;

function digestOf(bytes) {
  return createHash('sha256').update(bytes).digest('hex');
}

function versionKey(version) {
  return version == null ? '' : String(version);
}

export function identityKey(characteristic, databaseHash) {
  return `${characteristic}@${databaseHash}`;
}

class PassAborted extends Error {}

function coverageOf(fragments) {
  const sorted = [...fragments].sort((a, b) => a.offset - b.offset);
  let covered = 0;
  for (const fragment of sorted) {
    if (fragment.offset > covered) break;
    covered = Math.max(covered, fragment.offset + fragment.valueHex.length / 2);
  }
  return covered;
}

function assemblePrefix(fragments, end) {
  const sorted = [...fragments].sort((a, b) => a.offset - b.offset);
  let output = '';
  let covered = 0;
  for (const fragment of sorted) {
    if (fragment.offset > covered || covered >= end) break;
    output += fragment.valueHex.slice(Math.max(0, covered - fragment.offset) * 2);
    covered = Math.max(covered, fragment.offset + fragment.valueHex.length / 2);
  }
  return output.slice(0, end * 2);
}

function freshState(device) {
  return {
    device,
    databaseHash: null,
    directory: null,
    directoryCheckedAt: null,
    announcedDatabaseHash: null,
    processedEvents: [],
    identities: {},
    strays: [],
    conflicts: [],
    unattributed: [],
    generationChanges: [],
    lastSession: null,
    updatedAt: null
  };
}

function newSession(startedAt) {
  return { startedAt, finishedAt: null, reads: 0, duplicates: 0,
    eventsApplied: 0, eventsSkipped: 0, expiredVersionless: 0,
    notifications: [], serviceChanged: [], generationChanges: [],
    completed: [], strays: [], errors: [], aborted: null };
}

export class CollectionStore {
  constructor(root) { this.root = root; }

  pathFor(device) { return join(this.root, `${digestOf(device)}.json`); }

  async load(device) {
    try { return JSON.parse(await readFile(this.pathFor(device), 'utf8')); }
    catch (error) { if (error.code === 'ENOENT') return null; throw error; }
  }

  async save(device, state) {
    await mkdir(this.root, { recursive: true });
    const path = this.pathFor(device);
    const temporary = `${path}.${randomUUID()}.tmp`;
    const handle = await open(temporary, 'wx');
    try { await handle.writeFile(JSON.stringify(state, null, 2)); await handle.sync(); }
    finally { await handle.close(); }
    await rename(temporary, path);
  }
}

export class Collector {
  static async open({ root, device }) {
    const store = new CollectionStore(root);
    const state = (await store.load(device)) ?? freshState(device);
    return new Collector({ store, device, state });
  }

  constructor({ store, device, state }) {
    this.store = store;
    this.device = device;
    this.state = state;
    this.now = () => new Date().toISOString();
  }

  async save() {
    this.state.updatedAt = this.now();
    await this.store.save(this.device, this.state);
  }

  identity(characteristic, databaseHash, service = null) {
    const key = identityKey(characteristic, databaseHash);
    let identity = this.state.identities[key];
    if (!identity) {
      identity = { service, characteristic, databaseHash,
        assemblies: {}, values: {}, currentVersionKey: null,
        verificationNeeded: false, diverged: false, superseded: false,
        lastError: null, lastBlocked: null };
      this.state.identities[key] = identity;
    }
    if (service && !identity.service) identity.service = service;
    return identity;
  }

  async fetchServices(client) {
    const result = await client.request('/gatt/services');
    if (result.status !== 200) throw new Error(`服务目录读取失败：${result.status}`);
    return decodeDirectory(result.bytes);
  }

  async reconcile(directory, session) {
    const hash = directory.databaseHash;
    if (this.state.databaseHash !== hash) {
      const from = this.state.databaseHash;
      for (const identity of Object.values(this.state.identities)) {
        if (identity.databaseHash === hash) {
          identity.superseded = false;
          identity.verificationNeeded = true;
        } else {
          identity.superseded = true;
        }
      }
      this.state.databaseHash = hash;
      const change = { from, to: hash, at: this.now() };
      this.state.generationChanges.push(change);
      if (this.state.generationChanges.length > KEEP) this.state.generationChanges.shift();
      session.generationChanges.push(change);
    }
    this.state.directory = directory;
    this.state.directoryCheckedAt = this.now();
    await this.save();
  }

  async drainEvents(client, session) {
    const result = await client.request('/gatt/events');
    if (result.status === 404) return;
    if (result.status !== 200) throw new Error(`事件读取失败：${result.status}`);
    const digest = digestOf(result.bytes);
    if (this.state.processedEvents.includes(digest)) { session.eventsSkipped += 1; return; }
    const events = decodeEvents(result.bytes);
    for (const event of events) await this.applyEvent(event, client, session);
    this.state.processedEvents.push(digest);
    if (this.state.processedEvents.length > KEEP) this.state.processedEvents.shift();
    session.eventsApplied += events.length;
    await this.save();
  }

  async applyEvent(event, client, session) {
    if (event.kind === 'notification') {
      const target = this.state.directory?.characteristics.find(item => item.handle === event.handle);
      if (!target) {
        this.state.unattributed.push({ ...event, at: this.now() });
        if (this.state.unattributed.length > KEEP) this.state.unattributed.shift();
        return;
      }
      const identity = this.identity(target.uuid, this.state.databaseHash, target.service);
      this.mergeObservation(identity, event.version, event.valueHex, 'notification', []);
      session.notifications.push({ characteristic: target.uuid, version: event.version });
      return;
    }
    if (event.kind === 'service-changed') {
      this.state.announcedDatabaseHash = event.databaseHash;
      const affected = (this.state.directory?.characteristics ?? [])
        .filter(item => item.handle >= event.start && item.handle <= event.end);
      for (const item of affected) {
        this.identity(item.uuid, this.state.databaseHash, item.service).verificationNeeded = true;
      }
      session.serviceChanged.push({ start: event.start, end: event.end,
        databaseHash: event.databaseHash, affected: affected.map(item => item.uuid) });
      const directory = await this.fetchServices(client);
      await this.reconcile(directory, session);
      return;
    }
    throw new Error(`未知事件类型：${event.kind}`);
  }

  mergeObservation(identity, version, valueHex, source, connections) {
    const key = versionKey(version);
    const at = this.now();
    const current = identity.values[key] ??= { version: version ?? null, valueHex,
      sources: [], connections: [], confirmedByRead: false, firstAt: at, updatedAt: at };
    if (current.valueHex !== valueHex) {
      this.recordConflict(identity, version, current.valueHex, valueHex, source);
      if (source === 'read' && !current.confirmedByRead) {
        current.valueHex = valueHex;
        current.confirmedByRead = true;
        identity.currentVersionKey = key;
        if (!current.sources.includes('read')) current.sources.push('read');
        for (const connection of connections) {
          if (connection && !current.connections.includes(connection)) current.connections.push(connection);
        }
        current.updatedAt = at;
      }
      this.refreshDivergence(identity);
      return;
    }
    if (!current.sources.includes(source)) current.sources.push(source);
    for (const connection of connections) {
      if (connection && !current.connections.includes(connection)) current.connections.push(connection);
    }
    if (source === 'read') {
      current.confirmedByRead = true;
      identity.currentVersionKey = key;
    }
    current.updatedAt = at;
    this.refreshDivergence(identity);
  }

  refreshDivergence(identity) {
    identity.diverged = Object.entries(identity.values)
      .some(([key, value]) => value.sources.includes('notification')
        && !value.confirmedByRead && key !== identity.currentVersionKey);
  }

  recordConflict(identity, version, kept, rejected, source) {
    this.state.conflicts.push({ characteristic: identity.characteristic,
      databaseHash: identity.databaseHash, version: version ?? null,
      kept, rejected, source, at: this.now() });
    if (this.state.conflicts.length > KEEP) this.state.conflicts.shift();
  }

  fileFragment(fragment, session) {
    const identity = this.identity(fragment.characteristic, fragment.databaseHash, fragment.service ?? null);
    const key = versionKey(fragment.version);
    const existing = identity.values[key];
    if (existing?.confirmedByRead) {
      session.duplicates += 1;
      const slice = existing.valueHex.slice(fragment.offset * 2, fragment.offset * 2 + fragment.valueHex.length);
      if (slice !== fragment.valueHex) this.recordConflict(identity, fragment.version, slice, fragment.valueHex, 'read');
      return 'duplicate';
    }
    const at = this.now();
    const assembly = identity.assemblies[key] ??= { version: fragment.version ?? null,
      fragments: [], endOffset: null, startedAt: at, updatedAt: at };
    const length = fragment.valueHex.length / 2;
    const prior = assembly.fragments.find(item => item.offset === fragment.offset);
    if (prior) {
      session.duplicates += 1;
      if (prior.valueHex !== fragment.valueHex) {
        this.recordConflict(identity, fragment.version, prior.valueHex, fragment.valueHex, 'read');
      }
      return 'duplicate';
    }
    const covered = coverageOf(assembly.fragments);
    if (fragment.offset < covered) {
      session.duplicates += 1;
      const slice = assemblePrefix(assembly.fragments, covered)
        .slice(fragment.offset * 2, fragment.offset * 2 + fragment.valueHex.length);
      if (slice !== fragment.valueHex) this.recordConflict(identity, fragment.version, slice, fragment.valueHex, 'read');
      return 'duplicate';
    }
    assembly.fragments.push({ offset: fragment.offset, valueHex: fragment.valueHex,
      connection: fragment.connection ?? null });
    assembly.updatedAt = at;
    if (fragment.complete) assembly.endOffset = fragment.offset + length;
    const next = coverageOf(assembly.fragments);
    if (assembly.endOffset != null && next >= assembly.endOffset) {
      delete identity.assemblies[key];
      const connections = [...new Set(assembly.fragments.map(item => item.connection).filter(Boolean))];
      this.mergeObservation(identity, fragment.version,
        assemblePrefix(assembly.fragments, assembly.endOffset), 'read', connections);
      return 'complete';
    }
    return 'progress';
  }

  activeAssembly(identity) {
    let chosen = null;
    for (const assembly of Object.values(identity.assemblies)) {
      if (!chosen || (assembly.updatedAt ?? '') > (chosen.updatedAt ?? '')) chosen = assembly;
    }
    return chosen;
  }

  expireVersionlessAssemblies(session) {
    for (const identity of Object.values(this.state.identities)) {
      if (identity.assemblies['']) {
        delete identity.assemblies[''];
        session.expiredVersionless += 1;
      }
    }
  }

  async readCharacteristic(client, target, session, budget) {
    const hash = this.state.databaseHash;
    const identity = this.identity(target.characteristic, hash, target.service);
    identity.lastError = null;
    identity.lastBlocked = null;
    while (budget.steps > 0) {
      budget.steps -= 1;
      const assembly = this.activeAssembly(identity);
      const offset = assembly ? coverageOf(assembly.fragments) : 0;
      let result;
      try {
        result = await client.request(`/gatt/read/${target.handle}?offset=${offset}`);
      } catch (error) {
        identity.lastError = `传输中断：${error.message}`;
        session.errors.push({ characteristic: target.characteristic, error: error.message });
        await this.save();
        throw new PassAborted('连接中断，本次采集提前结束，已收到的分段可在下次继续');
      }
      session.reads += 1;
      if (result.status !== 200) {
        identity.lastError = `来源响应：${result.status}`;
        session.errors.push({ characteristic: target.characteristic, status: result.status });
        await this.save();
        return;
      }
      let fragment;
      try {
        fragment = decode(result.bytes);
      } catch (error) {
        identity.lastError = `分片无法解释：${error.message}`;
        session.errors.push({ characteristic: target.characteristic, error: error.message });
        await this.save();
        return;
      }
      if (fragment.handle !== target.handle || fragment.offset !== offset
          || typeof fragment.characteristic !== 'string' || typeof fragment.databaseHash !== 'string') {
        identity.lastError = '分片与读取请求不符';
        session.errors.push({ characteristic: target.characteristic, error: identity.lastError });
        await this.save();
        return;
      }
      if (fragment.databaseHash !== hash || fragment.characteristic !== target.characteristic) {
        this.fileFragment(fragment, session);
        identity.lastBlocked = { offset, fragmentDatabaseHash: fragment.databaseHash,
          fragmentCharacteristic: fragment.characteristic,
          fragmentVersion: fragment.version ?? null, at: this.now() };
        const stray = { requested: { characteristic: target.characteristic, databaseHash: hash,
            handle: target.handle, offset },
          received: { characteristic: fragment.characteristic, databaseHash: fragment.databaseHash,
            version: fragment.version ?? null },
          at: this.now() };
        session.strays.push(stray);
        this.state.strays.push(stray);
        if (this.state.strays.length > KEEP) this.state.strays.shift();
        await this.save();
        return;
      }
      const outcome = this.fileFragment(fragment, session);
      if (outcome === 'complete') {
        identity.verificationNeeded = false;
        session.completed.push(target.characteristic);
      }
      await this.save();
      try {
        await this.drainEvents(client, session);
      } catch (error) {
        throw new PassAborted(`事件读取中断：${error.message}`);
      }
      if (this.state.databaseHash !== hash) {
        session.aborted = '目录代次已变更，本次采集提前结束';
        return;
      }
      if (outcome !== 'progress') return;
    }
  }

  async runPass(client, { maxSteps = 64 } = {}) {
    const session = newSession(this.now());
    const directory = await this.fetchServices(client);
    await this.reconcile(directory, session);
    this.expireVersionlessAssemblies(session);
    const budget = { steps: maxSteps };
    try {
      await this.drainEvents(client, session);
    } catch (error) {
      session.aborted = `事件读取中断：${error.message}`;
    }
    try {
      for (const entry of session.aborted ? [] : this.pending()) {
        if (session.aborted || budget.steps <= 0) break;
        await this.readCharacteristic(client, entry, session, budget);
      }
    } catch (error) {
      if (!(error instanceof PassAborted)) throw error;
      session.aborted = error.message;
    }
    if (budget.steps <= 0 && !session.aborted) session.aborted = '达到本次采集步数上限';
    session.finishedAt = this.now();
    this.state.lastSession = session;
    await this.save();
    return this.report();
  }

  pending() {
    const directory = this.state.directory;
    const hash = this.state.databaseHash;
    if (!directory || !hash) return [];
    const list = [];
    for (const target of directory.characteristics) {
      const identity = this.state.identities[identityKey(target.uuid, hash)];
      const base = { service: target.service, characteristic: target.uuid,
        databaseHash: hash, handle: target.handle };
      if (!identity) {
        list.push({ ...base, reason: 'missing', detail: '当前目录版本下尚未读取' });
        continue;
      }
      const confirmed = Object.values(identity.values).some(value => value.confirmedByRead);
      const assemblies = Object.values(identity.assemblies);
      if (!confirmed) {
        if (identity.lastBlocked) {
          list.push({ ...base, reason: 'blocked',
            received: assemblies.reduce((total, item) => total + item.fragments.length, 0),
            detail: `偏移 ${identity.lastBlocked.offset} 收到旧目录（${identity.lastBlocked.fragmentDatabaseHash}）的迟到片段，需在目录 ${hash} 下重读` });
        } else if (identity.lastError) {
          list.push({ ...base, reason: 'error', detail: identity.lastError });
        } else if (assemblies.length) {
          const nextOffset = coverageOf(assemblies[0].fragments);
          list.push({ ...base, reason: 'incomplete',
            received: assemblies[0].fragments.length, nextOffset,
            detail: `长读未完成：已收 ${assemblies[0].fragments.length} 段，下一偏移 ${nextOffset}` });
        } else if (Object.keys(identity.values).length) {
          list.push({ ...base, reason: 'unconfirmed', detail: '仅有通知来源的值，尚未经读取确认' });
        } else {
          list.push({ ...base, reason: 'missing', detail: '当前目录版本下尚未读取' });
        }
      } else if (identity.diverged) {
        const unconfirmed = Object.values(identity.values)
          .filter(value => value.sources.includes('notification') && !value.confirmedByRead)
          .map(value => value.version ?? '无版本').join('、');
        list.push({ ...base, reason: 'diverged',
          detail: `通知版本 ${unconfirmed} 未经读取确认，晚到不视为更新` });
      } else if (identity.verificationNeeded) {
        list.push({ ...base, reason: 'verify', detail: 'Service Changed 涉及该属性，尚未重新核对' });
      }
    }
    return list;
  }

  serviceOf(identity) {
    if (identity.databaseHash !== this.state.databaseHash) return null;
    return this.state.directory?.characteristics
      .find(item => item.uuid === identity.characteristic)?.service ?? null;
  }

  report() {
    const values = [];
    for (const identity of Object.values(this.state.identities)) {
      for (const [key, value] of Object.entries(identity.values)) {
        values.push({
          service: identity.service ?? this.serviceOf(identity),
          characteristic: identity.characteristic,
          databaseHash: identity.databaseHash,
          version: value.version,
          complete: true,
          confirmed: value.confirmedByRead === true,
          current: value.confirmedByRead === true && identity.currentVersionKey === key,
          sources: value.sources,
          connections: value.connections,
          valueHex: value.valueHex,
          superseded: identity.superseded === true,
          diverged: identity.diverged === true,
          updatedAt: value.updatedAt ?? null
        });
      }
    }
    values.sort((a, b) => `${a.characteristic}@${a.databaseHash}@${a.version ?? ''}`
      .localeCompare(`${b.characteristic}@${b.databaseHash}@${b.version ?? ''}`));
    return {
      device: this.device,
      databaseHash: this.state.databaseHash,
      announcedDatabaseHash: this.state.announcedDatabaseHash,
      directoryCheckedAt: this.state.directoryCheckedAt,
      values,
      pending: this.pending(),
      strays: this.state.strays,
      conflicts: this.state.conflicts,
      unattributed: this.state.unattributed,
      generationChanges: this.state.generationChanges,
      lastSession: this.state.lastSession,
      updatedAt: this.state.updatedAt
    };
  }
}
