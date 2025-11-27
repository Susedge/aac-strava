#!/usr/bin/env node
const fs = require('fs');
const path = require('path');

const P = path.join(__dirname, '..', 'raw_activities_export_2025-11-27T14-30-53-547Z.json');
if (!fs.existsSync(P)) {
  console.error('Export file not found:', P);
  process.exit(2);
}

const raw = JSON.parse(fs.readFileSync(P, 'utf8'));
const list = raw && Array.isArray(raw.raw_activities) ? raw.raw_activities : (Array.isArray(raw) ? raw : []);

// Signature for candidate duplicate group detection
const sig = a => `${String(a.athlete_name||'').trim().toLowerCase()}|${Number(a.distance||0)}|${Number(a.moving_time||a.elapsed_time||0)}|${String(a.name||'').trim().toLowerCase()}`;

const byId = new Map();
const bySig = new Map();

list.forEach(it => {
  if (!it || !it.id) return;
  const id = String(it.id);
  const s = sig(it);

  if (!byId.has(id)) byId.set(id, []);
  byId.get(id).push(it);

  if (!bySig.has(s)) bySig.set(s, []);
  bySig.get(s).push(it);
});

const dupById = Array.from(byId.entries()).filter(([id, arr]) => arr.length > 1);
const dupBySig = Array.from(bySig.entries()).filter(([s, arr]) => arr.length > 1);

console.log('Total activities in export:', list.length);
console.log('Distinct ids:', byId.size);
console.log('Duplicate id groups (count):', dupById.length);
console.log('Duplicate signature groups (same athlete|distance|time|name) count:', dupBySig.length);

if (dupBySig.length) {
  console.log('\nSamples of duplicate signature groups (showing up to 5 groups):');
  dupBySig.slice(0,5).forEach(([s, arr], i)=>{
    console.log(`\nGroup ${i+1}: sig='${s}' count=${arr.length}`);
    arr.slice(0,5).forEach(a=>console.log(' - id:', a.id, 'athlete:', a.athlete_name, 'distance:', a.distance, 'moving_time:', a.moving_time, 'source:', a.source));
  });
}

if (dupById.length) {
  console.log('\nDuplicate id groups (showing up to 5):');
  dupById.slice(0,5).forEach(([id, arr])=>{
    console.log(`\nID ${id} appears ${arr.length} times`);
  });
}

process.exit(0);
