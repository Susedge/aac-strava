// Simulate normalization + cleanup on raw_activities.json
// Usage: node simulate_normalize_and_cleanup.js

const fs = require('fs');
const path = require('path');

const file = path.join(__dirname, '..', '..', 'raw_activities.json');
if (!fs.existsSync(file)) {
  console.error('raw_activities.json not found at repo root')
  process.exit(2)
}
const j = JSON.parse(fs.readFileSync(file, 'utf8'));
const raws = j.raw_activities || [];

function normalize(data){
  const out = Object.assign({}, data);
  // athlete_name
  if (!out.athlete_name && out.athlete && typeof out.athlete === 'object'){
    const fn = out.athlete.firstname || out.athlete.first_name || '';
    const ln = out.athlete.lastname || out.athlete.last_name || '';
    const full = (fn + ' ' + ln).trim(); if (full) out.athlete_name = full;
  }
  // athlete_id
  if (!out.athlete_id && out.athlete && (out.athlete.id || out.athlete.id_str || out.athlete_id)){
    out.athlete_id = String(out.athlete.id || out.athlete.id_str || out.athlete_id)
  }
  // distance
  const dCandidates = [out.distance, out.distance_m, out.distanceMeters, out.distance_meters];
  const dFound = dCandidates.find(v => typeof v !== 'undefined' && v !== null && v !== '');
  if (typeof out.distance === 'undefined' && typeof dFound !== 'undefined') out.distance = Number(dFound || 0);
  if (typeof out.distance === 'undefined' && typeof dFound === 'undefined') out.distance = 0;
  // moving_time
  if (typeof out.moving_time === 'undefined'){
    const mt = typeof out.moving_time !== 'undefined' ? out.moving_time : (typeof out.movingTime !== 'undefined' ? out.movingTime : null);
    out.moving_time = mt !== null && typeof mt !== 'undefined' ? Math.round(Number(mt||0)) : 0;
  }
  // elapsed_time
  if (typeof out.elapsed_time === 'undefined'){
    if (typeof out.elapsed_time !== 'undefined') out.elapsed_time = Number(out.elapsed_time || 0);
    else if (typeof out.moving_time !== 'undefined') out.elapsed_time = Math.round(Number(out.moving_time || 0));
    else out.elapsed_time = 0;
  }
  // elevation_gain
  if (typeof out.elevation_gain === 'undefined'){
    const egCandidates = [out.total_elevation_gain, out.elev_total, out.elevation];
    const eg = egCandidates.find(v => typeof v !== 'undefined' && v !== null);
    out.elevation_gain = Number(eg || 0);
  }
  // name
  if (!out.name) out.name = out.type ? `${out.type} Activity` : 'Activity';
  // type
  if (!out.type) out.type = 'Run';
  // sport/workout
  if (typeof out.sport_type === 'undefined' && out.type) out.sport_type = out.type;
  if (typeof out.workout_type === 'undefined') out.workout_type = out.workout_type || null;
  if (!out.updated_at) out.updated_at = Date.now();
  return out;
}

function buildDupKey(data){
  if (!data) return null;
  // athlete name pref, else athlete id
  let fn = '', ln = '';
  if (data.athlete && typeof data.athlete === 'object'){
    fn = (data.athlete.firstname || data.athlete.first_name || '').toString().trim();
    ln = (data.athlete.lastname || data.athlete.last_name || '').toString().trim();
  } else if (data.athlete_name) {
    const parts = data.athlete_name.toString().trim().split(/\s+/);
    fn = parts[0] || '';
    ln = parts.slice(1).join(' ') || '';
  }
  const activityName = (data.name || '').toString().trim();
  const athleteId = data.athlete && (data.athlete.id || data.athlete.id_str || data.athlete_id) ? String(data.athlete.id || data.athlete.id_str || data.athlete_id) : '';
  const rawDistanceMeters = Number(data.distance || data.distance_m || 0);
  const distanceKey = (Number.isFinite(rawDistanceMeters) ? (Math.round(rawDistanceMeters * 10) / 10).toFixed(1) : '0.0');
  const moving_time = Math.round(Number(data.moving_time || 0));
  const elapsed_time = Math.round(Number(data.elapsed_time || 0));
  const elevRaw = Number(data.total_elevation_gain || data.elevation_gain || data.elev_total || 0);
  const elev = Number.isFinite(elevRaw) ? (Math.round(elevRaw * 10) / 10).toFixed(1) : '0.0';
  const athleteNameNormalized = ((data.athlete_name || ((fn + ' ' + ln).trim())) || '').toString().trim();
  const parts = [athleteNameNormalized ? athleteNameNormalized : String(athleteId || ''), activityName, String(distanceKey), String(moving_time), String(elapsed_time), String(elev)];
  return parts.map(s => (s || '').toString().toLowerCase().replace(/\s+/g,' ').trim()).join('|');
}

// Normalize entries
const normalized = raws.map(r => normalize(r));

// Group by dup key
const groups = new Map();
normalized.forEach(doc => {
  const key = buildDupKey(doc);
  if (!key) return;
  if (!groups.has(key)) groups.set(key, []);
  groups.get(key).push(doc);
});

let totalGroups = 0; let totalRemovable = 0; let sampleGroups = [];
for (const [k, list] of groups.entries()){
  if (list.length <= 1) continue;
  totalGroups++;
  // pick keep per cleanup heuristic: prefer strava_id -> source contains strava -> earliest updated_at
  let keep = list[0];
  const withStravaId = list.find(x=>x.strava_id);
  if (withStravaId) keep = withStravaId;
  else {
    const withStravaSource = list.find(x=> x.source && String(x.source).toLowerCase().includes('strava'));
    if (withStravaSource) keep = withStravaSource;
    else {
      // prefer earliest updated_at
      let earliest = list[0];
      for (const it of list) if ((it.updated_at||Number.MAX_SAFE_INTEGER) < (earliest.updated_at||Number.MAX_SAFE_INTEGER)) earliest = it;
      keep = earliest;
    }
  }
  const deletes = list.filter(x => x.id !== keep.id).map(x=>x.id);
  totalRemovable += deletes.length;
  if (sampleGroups.length < 30) sampleGroups.push({ key: k, keep: keep.id, deletes, all: list.map(x => ({ id: x.id, distance: x.distance, moving_time: x.moving_time, elapsed_time: x.elapsed_time, elevation_gain: x.elevation_gain, name: x.name, source: x.source })) });
}

console.log('TOTAL RAW across input:', raws.length);
console.log('Groups found with duplicates after normalization:', totalGroups);
console.log('Total removable duplicate documents (per heuristic):', totalRemovable);
console.log('Sample duplicate groups (up to 30):');
console.log(JSON.stringify(sampleGroups, null, 2));

if (totalRemovable === 0) process.exitCode = 2; else process.exitCode = 0;
