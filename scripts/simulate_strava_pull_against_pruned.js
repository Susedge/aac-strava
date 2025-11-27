#!/usr/bin/env node
const fs = require('fs');
const path = require('path');

const PRUNED = path.join(__dirname, '..', 'raw_activities.pruned_normalized.json');
const EXPORT = path.join(__dirname, '..', 'raw_activities_export_2025-11-27T14-30-53-547Z.json');

if (!fs.existsSync(PRUNED) || !fs.existsSync(EXPORT)) {
  console.error('Missing files; ensure pruned and export JSON exist');
  process.exit(2);
}

const pruned = JSON.parse(fs.readFileSync(PRUNED, 'utf8')).raw_activities || [];
const exportAll = JSON.parse(fs.readFileSync(EXPORT, 'utf8')).raw_activities || [];

// Determine the 'pulled' items: items in export that are NOT in pruned (extra 191)
const prunedIds = new Set(pruned.map(p => String(p.id)));
const pulled = exportAll.filter(e => !prunedIds.has(String(e.id)));

console.log('pruned count:', pruned.length);
console.log('export count:', exportAll.length);
console.log('pulled (extras) count:', pulled.length);

// matching util (same logic used in backend)
const DISTANCE_TOLERANCE_REL = 0.02;
const DISTANCE_TOLERANCE_STRICT = 10;
const MT_TOLERANCE_STRICT = 10;

function matchPruned(incoming, candidate) {
  const incName = (incoming.athlete_name || '').toString().trim().toLowerCase();
  const candName = (candidate.athlete_name || '').toString().trim().toLowerCase();
  if (!incName || !candName || incName !== candName) return false;

  const cd = Number(candidate.distance || 0);
  const id = Number(incoming.distance || 0);
  const cm = Number(candidate.moving_time || candidate.elapsed_time || 0);
  const im = Number(incoming.moving_time || incoming.elapsed_time || 0);

  const ceExists = candidate.hasOwnProperty('elapsed_time') && candidate.elapsed_time !== null;
  const ieExists = incoming.hasOwnProperty('elapsed_time') && incoming.elapsed_time !== null;
  const ce = ceExists ? Number(candidate.elapsed_time) : null;
  const ie = ieExists ? Number(incoming.elapsed_time) : null;

  const distStrict = Math.abs(cd - id) <= DISTANCE_TOLERANCE_STRICT || (Math.abs(cd - id) / Math.max(1, Math.max(Math.abs(cd), Math.abs(id)))) <= DISTANCE_TOLERANCE_REL;
  const mtStrict = Math.abs(cm - im) <= MT_TOLERANCE_STRICT;
  const elapsedStrict = (!ieExists || !ceExists) ? true : Math.abs(ce - ie) <= MT_TOLERANCE_STRICT;

  return distStrict && mtStrict && elapsedStrict;
}

let updates = 0, creates = 0;
const matchedPairs = [];

for (const inc of pulled) {
  const found = pruned.find(p => matchPruned(inc, p));
  if (found) {
    updates++;
    matchedPairs.push({ incomingId: inc.id, matchedId: found.id });
  } else {
    creates++;
  }
}

console.log('Pulled items that would update existing pruned:', updates);
console.log('Pulled items that would create NEW docs:', creates);
if (matchedPairs.length) console.log('Sample matched pairs:', matchedPairs.slice(0,10));

process.exit(0);
