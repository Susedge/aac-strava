#!/usr/bin/env node
const fs = require('fs');
const path = require('path');

const PRUNED = path.join(__dirname, '..', 'raw_activities.pruned_normalized.json');
const EXPORT = path.join(__dirname, '..', 'raw_activities_export_2025-11-27T14-30-53-547Z.json');

if (!fs.existsSync(PRUNED)) { console.error('Pruned normalized file not found:', PRUNED); process.exit(2); }
if (!fs.existsSync(EXPORT)) { console.error('Export file not found:', EXPORT); process.exit(2); }

const pruned = JSON.parse(fs.readFileSync(PRUNED, 'utf8')).raw_activities || [];
const exp = JSON.parse(fs.readFileSync(EXPORT, 'utf8')).raw_activities || [];

// matching tolerances (align with backend code)
const DISTANCE_TOLERANCE_REL = 0.02;
const DISTANCE_TOLERANCE_STRICT = 10;
const MT_TOLERANCE_STRICT = 10;

function matchByAthleteNameNumeric(incoming, candidate) {
  const candName = (candidate.athlete_name || '').toString().trim().toLowerCase();
  const incName = (incoming.athlete_name || '').toString().trim().toLowerCase();
  if (!candName || !incName || candName !== incName) return false;

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
// We'll use a Set of export doc ids to represent existing docs; when an incoming would update, we mark it
const existing = exp.slice();

for (const inc of pruned) {
  // If any export doc matches by athlete_name + numeric fields, we consider it an update
  const found = existing.find(c => matchByAthleteNameNumeric(inc, c));
  if (found) updates++;
  else creates++;
}

console.log('Pruned size:', pruned.length);
console.log('Export size:', exp.length);
console.log('Incoming that would update existing by athlete_name+distance+time:', updates);
console.log('Incoming that would create new docs:', creates);

process.exit(0);
