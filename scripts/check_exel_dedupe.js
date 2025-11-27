const fs = require('fs');
const path = require('path');
const files = fs.readdirSync(path.join(__dirname, '..'));
const matches = files.filter(f => f.startsWith('raw_activities_export_2025-11-27') && f.endsWith('.json'));
if (!matches.length) { console.error('No export found'); process.exit(1); }
const EX = path.join(__dirname, '..', matches.sort().slice(-1)[0]);
console.log('Using export file:', EX);
const raw = JSON.parse(fs.readFileSync(EX, 'utf8'));
const list = raw.raw_activities || [];

const normalizeName = s => String(s || '').trim().toLowerCase();
const fuzz = { absMeters: 10, rel: 0.02, mtSeconds: 10 };
const groups = new Map();
let gi = 0;
const findGroup = (d) => {
  const name = normalizeName(d.athlete_name);
  const dist = Number(d.distance || 0);
  const mt = Math.round(Number(d.moving_time || d.elapsed_time || 0));
  for (const [k, arr] of groups.entries()) {
    const rep = arr[0];
    if (!rep) continue;
    const repName = normalizeName(rep.athlete_name);
    if (repName !== name) continue;
    const repDist = Number(rep.distance || 0);
    const repMt = Math.round(Number(rep.moving_time || rep.elapsed_time || 0));
    const absd = Math.abs(repDist - dist);
    const reld = absd / Math.max(1, Math.max(Math.abs(repDist), Math.abs(dist)));
    const absmt = Math.abs(repMt - mt);
    const distOk = (absd <= fuzz.absMeters) || (reld <= fuzz.rel);
    const mtOk = absmt <= fuzz.mtSeconds;
    if (distOk && mtOk) return k;
  }
  return null;
}
for (const doc of list) {
  const gk = findGroup(doc);
  if (gk) groups.get(gk).push(doc);
  else groups.set(`g${++gi}`, [doc]);
}

let exelGroups = 0; let exelTotal = 0;
for (const [k, arr] of groups.entries()) {
  if (arr.some(a => (a.athlete_name || '').toLowerCase().includes('exel'))) {
    exelGroups++;
    exelTotal += arr.length;
    console.log('Group', k, 'count', arr.length, 'ids:', arr.map(x=>x.id).join(', '))
  }
}
console.log('Exel groups count:', exelGroups, 'total exel docs in groups:', exelTotal);
console.log('Total groups', groups.size);

process.exit(0);
