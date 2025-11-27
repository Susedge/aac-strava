#!/usr/bin/env node
const fs = require('fs');
const path = require('path');

const file = path.join(__dirname, '..', 'raw_activities.json');

const requiredKeys = [
  'athlete_id','athlete_name','distance','elapsed_time','elevation_gain','fetched_at','id','moving_time','name','source','sport_type','type','updated_at','workout_type'
];

function isSuperset(keys, required) {
  const set = new Set(keys);
  return required.every(k => set.has(k));
}

try {
  const raw = fs.readFileSync(file, 'utf8');
  const parsed = JSON.parse(raw);
  const arr = parsed.raw_activities || [];

  let exact = 0;
  let superset = 0;
  const samples = [];

  arr.forEach((a, i) => {
    const keys = Object.keys(a).sort();
    const sig = keys.join(',');
    if (sig === requiredKeys.join(',')) exact++;
    if (isSuperset(keys, requiredKeys)) {
      superset++;
      if (samples.length < 10) samples.push({ id: a.id, idx: i, keys })
    }
  });

  console.log('total', arr.length);
  console.log('exact signature matches:', exact);
  console.log('superset (contains required keys):', superset);
  console.log('samples (up to 10):', samples);
} catch (e) {
  console.error('error reading file', e && e.message ? e.message : e);
  process.exit(1);
}
