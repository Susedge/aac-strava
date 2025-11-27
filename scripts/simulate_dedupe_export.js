#!/usr/bin/env node
const fs = require('fs');
const path = require('path');

const EX = path.join(__dirname, '..', 'raw_activities_export_2025-11-27T14-30-53-547Z.json');
if (!fs.existsSync(EX)) { console.error('Export file not found:', EX); process.exit(2); }

const raw = JSON.parse(fs.readFileSync(EX, 'utf8'));
const list = raw.raw_activities || [];

const normalizeName = s => String(s || '').trim().toLowerCase();
const round = n => Math.round(Number(n || 0));

const groups = new Map();
for (const doc of list) {
  const key = `${normalizeName(doc.athlete_name)}|${round(doc.distance)}|${round(doc.moving_time || doc.elapsed_time || 0)}`;
  if (!groups.has(key)) groups.set(key, []);
  groups.get(key).push(doc);
}

const dupGroups = Array.from(groups.entries()).filter(([k, arr]) => arr.length > 1);
console.log('export total', list.length);
console.log('duplicate groups', dupGroups.length);

// If we dedupe: keep earliest fetched_at per group
let removed = 0;
const final = [];
for (const [k, arr] of groups.entries()) {
  if (arr.length === 1) { final.push(arr[0]); continue; }
  // choose earliest fetched_at
  arr.sort((a, b) => (Number(a.fetched_at || 0) - Number(b.fetched_at || 0)));
  final.push(arr[0]);
  removed += arr.length - 1;
}

console.log('after dedupe final count', final.length, 'removed', removed);
// print some sample duplicate groups
if (dupGroups.length) {
  console.log('\nSample duplicate groups (first 5):');
  dupGroups.slice(0,5).forEach(([k, arr], i) => {
    console.log(`\nGroup ${i+1} key=${k} count=${arr.length}`);
    arr.slice(0,6).forEach(a => console.log(' -', a.id, a.distance, a.moving_time, a.elapsed_time, a.athlete_name));
  });
}

process.exit(0);
