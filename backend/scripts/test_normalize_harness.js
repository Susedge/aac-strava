// Test harness: reads raw_activities.json from repo root and simulates normalize-raw-activities behavior
const fs = require('fs');
const path = require('path');

const file = path.join(__dirname, '..', '..', 'raw_activities.json');
const j = JSON.parse(fs.readFileSync(file, 'utf8'));
const raws = j.raw_activities || [];
// note: harness now only simulates normalization on raw_activities (not backups)

function normalizeEntry(data) {
  const out = {};
  // ensure athlete_name
  if (!data.athlete_name && data.athlete && typeof data.athlete === 'object') {
    const fn = data.athlete.firstname || data.athlete.first_name || '';
    const ln = data.athlete.lastname || data.athlete.last_name || '';
    const full = (fn + ' ' + ln).trim();
    if (full) out.athlete_name = full;
  }
  // athlete_id
  if (!data.athlete_id && data.athlete && (data.athlete.id || data.athlete.id_str || data.athlete_id)) {
    out.athlete_id = String(data.athlete.id || data.athlete.id_str || data.athlete_id);
  }
  // distance
  const dCandidates = [data.distance, data.distance_m, data.distanceMeters, data.distance_meters];
  const dFound = dCandidates.find(v => typeof v !== 'undefined' && v !== null && v !== '');
  if (typeof data.distance === 'undefined' && typeof dFound !== 'undefined') out.distance = Number(dFound || 0);
  if (typeof data.distance === 'undefined' && typeof dFound === 'undefined') out.distance = 0;
  // moving_time
  if (typeof data.moving_time === 'undefined') {
    const mt = typeof data.moving_time !== 'undefined' ? data.moving_time : (typeof data.movingTime !== 'undefined' ? data.movingTime : null);
    if (mt !== null && typeof mt !== 'undefined') out.moving_time = Math.round(Number(mt || 0));
    else out.moving_time = 0;
  }
  // elapsed_time
  if (typeof data.elapsed_time === 'undefined') {
    if (typeof data.elapsed_time !== 'undefined') out.elapsed_time = Number(data.elapsed_time || 0);
    else if (typeof data.moving_time !== 'undefined') out.elapsed_time = Math.round(Number(data.moving_time || 0));
    else out.elapsed_time = 0;
  }
  // elevation
  if (typeof data.elevation_gain === 'undefined') {
    const egCandidates = [data.total_elevation_gain, data.elev_total, data.elevation];
    const eg = egCandidates.find(v => typeof v !== 'undefined' && v !== null);
    out.elevation_gain = Number(eg || 0);
  }
  // name
  if (!data.name) out.name = data.type ? `${data.type} Activity` : 'Activity';
  // type
  if (!data.type) out.type = 'Run';
  // sport/workout
  if (typeof data.sport_type === 'undefined' && data.type) out.sport_type = data.type;
  if (typeof data.workout_type === 'undefined') out.workout_type = data.workout_type || null;
  // updated_at
  if (!data.updated_at) out.updated_at = Date.now();
  return out;
}

function runSet(list) {
  let updatedCount = 0;
  const sample = [];
  for (const d of list) {
    const n = normalizeEntry(d);
    if (Object.keys(n).length > 0) { updatedCount++; sample.push({ id: d.id, changes: n }); }
  }
  return { updatedCount, sample: sample.slice(0,10) };
}

console.log('Raw activities:', raws.length);
const rres = runSet(raws);
console.log('raw normalized candidates:', rres.updatedCount);
console.log('sample changes (raw):', rres.sample.slice(0,5));

if (rres.updatedCount > 0) process.exitCode = 0; else process.exitCode = 2;
