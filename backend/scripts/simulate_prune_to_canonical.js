// Simulate prune-raw-to-canonical on raw_activities.json
// Usage: node simulate_prune_to_canonical.js [round_unit]

const fs = require('fs');
const path = require('path');

const file = path.join(__dirname, '..', '..', 'raw_activities.json');
if (!fs.existsSync(file)) {
  console.error('raw_activities.json not found at repo root');
  process.exit(2);
}
const j = JSON.parse(fs.readFileSync(file, 'utf8'));
const raws = j.raw_activities || [];

const argUnit = process.argv[2] ? Number(process.argv[2]) : 3.5;
const roundUnit = Number.isFinite(argUnit) ? argUnit : 3.5;
const dateOnly = true;

function normalizeName(d) {
  if (!d) return '';
  if (d.athlete && typeof d.athlete === 'object') {
    const fn = d.athlete.firstname || d.athlete.first_name || '';
    const ln = d.athlete.lastname || d.athlete.last_name || '';
    const full = (fn + ' ' + ln).trim();
    if (full) return full;
  }
  if (d.athlete_name) return d.athlete_name;
  return d.athlete_id ? String(d.athlete_id || '') : '';
}

function datePart(s) {
  if (!s) return '';
  return dateOnly ? String(s).slice(0, 10) : String(s);
}

function roundNearest(n, unit) {
  if (!Number.isFinite(Number(n))) return 0;
  return Math.round(Number(n) / unit) * unit;
}

const groups = new Map();
for (const doc of raws) {
  const rawDistance = Number(doc.distance || doc.distance_m || doc.distanceMeters || 0);
  const rounded = roundNearest(rawDistance, roundUnit);
  const name = (normalizeName(doc) || '').toString().trim().toLowerCase();
  const keyParts = [name || '', datePart(doc.start_date || doc.start_date_local || ''), String(rounded)];
  const key = keyParts.map(s => (s || '').toString().toLowerCase().replace(/\s+/g, ' ').trim()).join('|');
  if (!key) continue;
  if (!groups.has(key)) groups.set(key, []);
  groups.get(key).push(doc);
}

let totalGroups = groups.size;
let groupsWithDup = 0;
let totalRemovable = 0;
const sample = [];
for (const [k, list] of groups.entries()) {
  if (list.length <= 1) continue;
  groupsWithDup++;
  // choose keep heuristic
  let keep = list[0];
  const withStravaId = list.find(x => x.strava_id || x.stravaId);
  if (withStravaId) keep = withStravaId;
  else {
    const withStravaSource = list.find(x => x.source && String(x.source).toLowerCase().includes('strava'));
    if (withStravaSource) keep = withStravaSource;
    else {
      // earliest updated
      let earliest = list[0];
      for (const it of list) {
        const ts = Number(it.updated_at || it.fetched_at || it.created_at || Number.MAX_SAFE_INTEGER);
        const best = Number(earliest.updated_at || earliest.fetched_at || earliest.created_at || Number.MAX_SAFE_INTEGER);
        if (ts < best) earliest = it;
      }
      keep = earliest;
    }
  }
  const deletes = list.filter(x => x.id !== keep.id).map(x => x.id);
  totalRemovable += deletes.length;
  if (sample.length < 50) sample.push({ key: k, keep: keep.id, deletes, all: list.map(x => ({ id: x.id, distance: x.distance, start_date: x.start_date })) });
}

console.log('TOTAL RAW across input:', raws.length);
console.log('round_unit:', roundUnit);
console.log('total groups found (post normalization grouping):', totalGroups);
console.log('groups with duplicates:', groupsWithDup);
console.log('total removable docs (per this heuristic):', totalRemovable);
console.log('expected kept ->', raws.length - totalRemovable);
console.log('sample groups (up to 50):', JSON.stringify(sample, null, 2));

if (totalRemovable === 0) process.exitCode = 2; else process.exitCode = 0;
