#!/usr/bin/env node
const fs = require('fs');
const path = require('path');

const file = path.join(__dirname, '..', 'raw_activities.json');

function keySignature(obj) {
  // Use sorted keys as the structural signature
  if (obj === null) return 'null';
  if (Array.isArray(obj)) return 'array[' + (obj.length ? keySignature(obj[0]) : '') + ']';
  if (typeof obj !== 'object') return typeof obj;
  const keys = Object.keys(obj).sort();
  return keys.join(',');
}

try {
  const raw = fs.readFileSync(file, 'utf8');
  const parsed = JSON.parse(raw);

  if (!parsed || !Array.isArray(parsed.raw_activities)) {
    console.error('file does not contain top-level `raw_activities` array');
    process.exit(2);
  }

  const activities = parsed.raw_activities;
  console.log('Total activities:', activities.length);

  const groups = new Map();

  activities.forEach((act, idx) => {
    const sig = keySignature(act);
    const arr = groups.get(sig) || [];
    arr.push({ id: act.id, index: idx });
    groups.set(sig, arr);
  });

  // Convert to array of {signature, count}
  const byCount = Array.from(groups.entries()).map(([signature, items]) => ({ signature, count: items.length, sample: items.slice(0,3) }));
  byCount.sort((a, b) => b.count - a.count);

  console.log('\nTop structures by count (signature keys):');
  byCount.slice(0, 30).forEach((g, i) => {
    console.log(`${i+1}. ${g.count} — ${g.signature}`);
  });

  console.log('\nTotal distinct structures:', byCount.length);
  // Optionally show any duplicates groups with more than 1
  const dupGroups = byCount.filter(g => g.count > 1);
  console.log('\nStructures with duplicates (count > 1):', dupGroups.length);

  // Write a small JSON summary for easier consumption
  const out = { total: activities.length, distinctStructures: byCount.length, groups: byCount };
  fs.writeFileSync(path.join(__dirname, '..', 'raw_activities.structure_summary.json'), JSON.stringify(out, null, 2));
  console.log('\nWrote summary to raw_activities.structure_summary.json');
} catch (err) {
  console.error('error:', err && err.message ? err.message : err);
  process.exit(1);
}

