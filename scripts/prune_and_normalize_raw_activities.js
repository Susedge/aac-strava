#!/usr/bin/env node
const fs = require('fs');
const path = require('path');

const IN = path.join(__dirname, '..', 'raw_activities.json');
const OUT = path.join(__dirname, '..', 'raw_activities.pruned_normalized.json');

function signature(obj) {
  if (!obj || typeof obj !== 'object') return '';
  return Object.keys(obj).sort().join(',');
}

function is192(obj) {
  // matches the signature we found earlier (the 192 group)
  const sig = signature(obj);
  const target = 'athlete_id,athlete_name,distance,elapsed_time,elevation_gain,fetched_at,id,moving_time,name,source,sport_type,type,updated_at,workout_type';
  return sig === target;
}

function normalize(act) {
  // target shape provided by user
  const targetKeys = [
    'id',
    'distance',
    'athlete_id',
    'source',
    'elevation_gain',
    'type',
    'workout_type',
    'elapsed_time',
    'name',
    'athlete_name',
    'moving_time',
    'sport_type',
    'fetched_at',
    'updated_at'
  ];

  const out = {};

  // prefer existing values; for fetched_at/updated_at prefer fetched_at/updated_at then created_at
  const createdAt = act.created_at;

  // helpers
  const pick = (k, fallback = null) => (act.hasOwnProperty(k) ? act[k] : fallback);

  out.id = pick('id');
  out.distance = pick('distance', null);
  out.athlete_id = pick('athlete_id', null);
  out.source = pick('source', null);
  out.elevation_gain = pick('elevation_gain', null);
  out.type = pick('type', null);
  out.workout_type = pick('workout_type', null);
  out.elapsed_time = pick('elapsed_time', null);
  out.name = pick('name', null);
  out.athlete_name = pick('athlete_name', null);
  out.moving_time = pick('moving_time', null);
  out.sport_type = pick('sport_type', null);

  // timestamps: use fetched_at -> created_at -> null
  out.fetched_at = pick('fetched_at', pick('created_at', null));
  out.updated_at = pick('updated_at', pick('created_at', null));

  // ensure exactly the target keys order
  return targetKeys.reduce((acc, k) => {
    acc[k] = out[k] === undefined ? null : out[k];
    return acc;
  }, {});
}

try {
  const raw = fs.readFileSync(IN, 'utf8');
  const parsed = JSON.parse(raw);

  if (!parsed || !Array.isArray(parsed.raw_activities)) {
    console.error('file does not contain top-level `raw_activities` array');
    process.exit(2);
  }

  const activities = parsed.raw_activities;

  const removed = [];
  const kept = [];

  activities.forEach((a, i) => {
    if (is192(a)) removed.push(a);
    else kept.push(a);
  });

  const normalized = kept.map(normalize);

  const out = {
    exported_at: parsed.exported_at || Date.now(),
    raw_activities: normalized
  };

  fs.writeFileSync(OUT, JSON.stringify(out, null, 2));
  console.log('Input total:', activities.length);
  console.log('Removed (192 group) count:', removed.length);
  console.log('Remaining count:', normalized.length);
  console.log('Wrote pruned+normalized file:', OUT);
} catch (err) {
  console.error('error:', err && err.message ? err.message : err);
  process.exit(1);
}
