export function dispatch({ url, phase }) {
  if (url.pathname === '/gatt/services' && phase) return { json: { databaseHash: 'db-b', services: [{ uuid: '181A', start: 16, end: 40,
    characteristics: [{ uuid: '2A6F', handle: 33 }, { uuid: '2A6E', handle: 37 }] }] } };
  if (url.pathname === '/gatt/read/37') return { json: { connection: 'conn-2', databaseHash: 'db-b', characteristic: '2A6E', handle: 37,
    offset: Number(url.searchParams.get('offset') || 0), mtu: 23, version: 'record-8', valueHex: '1500', complete: true } };
  return null;
}
