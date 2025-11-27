// Test script to verify that fuzzy matches do not overwrite existing raw_activities
// Run: node scripts/test_preserve_duplicates.js

function sanitize(s) { return String(s || '').replace(/[^a-zA-Z0-9_-]/g,'_').replace(/_+/g, '_'); }

// Reuse matching logic from test_fuzzy_matching (lightweight copy)
function findFuzzyMatchInArray(candidates, {distanceVal, movingTimeVal, startDateVal, nameVal, elevationVal, elapsedVal, stravaIdVal, typeVal, sportTypeVal, workoutTypeVal, athleteResourceState}) {
  const MAX_CANDIDATES = 50;
  const DISTANCE_TOLERANCE_ABS = 50; // meters
  const DISTANCE_TOLERANCE_REL = 0.03; // 3%
  const MT_TOLERANCE_SEC = 60; // seconds
  const START_DATE_TOLERANCE_MS = 2 * 60 * 1000; // 2 minutes (strong)
  const START_DATE_TOLERANCE_LOOSE_MS = 5 * 60 * 1000; // 5 minutes (looser)

  const withinTolerance = (a, b) => {
    if (typeof a !== 'number' || typeof b !== 'number') return false;
    const abs = Math.abs(a - b);
    const rel = Math.abs(a - b) / Math.max(1, Math.max(Math.abs(a), Math.abs(b)));
    return abs <= DISTANCE_TOLERANCE_ABS || rel <= DISTANCE_TOLERANCE_REL;
  };
  const toMs = (v) => {
    try { const ms = new Date(String(v)).getTime(); return Number.isNaN(ms) ? null : ms; } catch(e){return null}
  };
  const curMs = toMs(startDateVal);

  for (let i = 0; i < Math.min(MAX_CANDIDATES, candidates.length); i++) {
    const d = candidates[i];
    if (stravaIdVal && d.strava_id && String(d.strava_id) === String(stravaIdVal)) return { matched: true, doc: d, type: 'strava_id' };
    if (d.start_date && curMs) {
      const candMs = toMs(d.start_date);
      if (candMs && Math.abs(candMs - curMs) <= START_DATE_TOLERANCE_MS) return { matched: true, doc: d, type: 'start_date_strict' };
      if (candMs && Math.abs(candMs - curMs) <= START_DATE_TOLERANCE_LOOSE_MS) {
        const candDist = Number(d.distance || 0);
        const candMt = Number(d.moving_time || 0);
        const candElapsed = Number(d.elapsed_time || 0);
        const incElapsed = Number(elapsedVal || 0);
        const distLoose = Math.abs(candDist - Number(distanceVal || 0)) <= 50 || (Math.abs(candDist - Number(distanceVal || 0)) / Math.max(1, Math.max(Math.abs(candDist), Math.abs(Number(distanceVal || 0))))) <= 0.03;
        const mtLoose = Math.abs(candMt - Number(movingTimeVal || 0)) <= 60 || (incElapsed && Math.abs(candElapsed - incElapsed) <= 60);
        if (distLoose && mtLoose) return { matched: true, doc: d, type: 'start_date_loose' };
      }
      if (candMs && Math.abs(candMs - curMs) > START_DATE_TOLERANCE_LOOSE_MS) continue;
    }

    const candDist = Number(d.distance || 0);
    const candMt = Number(d.moving_time || 0);
    const candElapsed = Number(d.elapsed_time || 0);
    const incElapsed = Number(elapsedVal || 0);
    const distStrict = Math.abs(candDist - Number(distanceVal || 0)) <= 10 || (Math.abs(candDist - Number(distanceVal || 0)) / Math.max(1, Math.max(Math.abs(candDist), Math.abs(Number(distanceVal || 0))))) <= 0.02;
    const mtStrict = Math.abs(candMt - Number(movingTimeVal || 0)) <= 10 || (incElapsed && Math.abs(candElapsed - incElapsed) <= 10);
    if (distStrict && mtStrict) {
      const candName = (d.name || '').toString().trim().toLowerCase();
      const incName = (nameVal || '').toString().trim().toLowerCase();
      const nameMatch = candName && incName && candName === incName;
      const candType = (d.type || '').toString().trim().toLowerCase();
      const typeMatch = candType && typeVal && candType === String(typeVal).toLowerCase();
      const candSportType = (d.sport_type || '').toString().trim().toLowerCase();
      const sportMatch = candSportType && sportTypeVal && candSportType === String(sportTypeVal).toLowerCase();
      const candWorkoutType = typeof d.workout_type !== 'undefined' ? String(d.workout_type) : null;
      const workoutMatch = (typeof d.workout_type !== 'undefined' && typeof workoutTypeVal !== 'undefined') && String(d.workout_type) === String(workoutTypeVal);
      const candEg = Number(d.elevation_gain || d.total_elevation_gain || 0);
      const incEg = Number(elevationVal || 0);
      const elevMatch = Number.isFinite(candEg) && Math.abs(candEg - incEg) <= 5;
      const incElapsed = Number(elapsedVal || 0);
      const elapsedMatch = incElapsed && candElapsed && Math.abs(candElapsed - incElapsed) <= 10;
      let score = 0;
      if (nameMatch) score += 3;
      if (distStrict) score += 2;
      if (mtStrict) score += 2;
      if (typeMatch) score += 1;
      if (sportMatch) score += 1;
      if (workoutMatch) score += 1;
      if (elevMatch) score += 1;
      if (elapsedMatch) score += 1;
      if (score >= 6) return { matched: true, doc: d, type: 'strict_numeric_full' };
      if (score >= 4) return { matched: true, doc: d, type: 'strict_numeric' };
    }
  }
  return { matched: false };
}

// Simulated stored docs
const stored = [
  { id: 'str_111', resource_state: 2, athlete: { resource_state: 2, firstname: 'Alice', lastname: 'A.' }, athlete_id: '100', distance: 4000, moving_time: 1200, elapsed_time: 1210, name: 'Special Run', strava_id: '111', start_date: '2025-11-01T05:00:00Z', total_elevation_gain: 5, type: 'Run', sport_type: 'Run', workout_type: null },
  { id: 's1', resource_state: 2, athlete: { resource_state: 2, firstname: 'Alice', lastname: 'A.' }, athlete_id: '100', distance: 5000, moving_time: 1500, elapsed_time: 1510, name: 'Morning Run', start_date: '2025-11-01T06:00:00Z', total_elevation_gain: 10, type: 'Run', sport_type: 'Run', workout_type: null },
  // stored earlier '50 left' activity (newer) which should not cause older '82 left' to be merged
  { id: 's50', resource_state: 2, athlete: { resource_state: 2, firstname: 'k1LLu4', lastname: 'Z.' }, athlete_id: '100', distance: 3170.7, moving_time: 2303, elapsed_time: 2780, name: '50 left', start_date: '2025-11-02T06:00:00Z', total_elevation_gain: 3.3, type: 'Run', sport_type: 'Run', workout_type: null },
  { id: 's2', resource_state: 2, athlete: { resource_state: 2, firstname: 'Alice', lastname: 'A.' }, athlete_id: '100', distance: 5000, moving_time: 1500, elapsed_time: 1510, name: 'Morning Run' }
];

function storeIncoming(incoming) {
  // find candidates by athleteId or name
  const candidates = incoming.athleteId ? stored.filter(s=>s.athlete_id === incoming.athleteId) : (incoming.athleteName ? stored.filter(s => String(s.athlete_name||'').toLowerCase() === String(incoming.athleteName||'').toLowerCase()) : []);
  const found = candidates.length ? findFuzzyMatchInArray(candidates, { distanceVal: incoming.distance, movingTimeVal: incoming.moving_time, startDateVal: incoming.start_date, nameVal: incoming.name, elevationVal: incoming.elevation_gain, elapsedVal: incoming.elapsed_time, stravaIdVal: incoming.strava_id, typeVal: incoming.type, sportTypeVal: incoming.sport_type, workoutTypeVal: incoming.workout_type, athleteResourceState: incoming.athlete && incoming.athlete.resource_state }) : { matched: false };

  // storage decision: only update when match type is 'strava_id' or 'start_date_strict'
  const allowedUpdate = ['strava_id','start_date_strict'];
  if (found.matched && allowedUpdate.includes(found.type)) {
    return { action: 'update', targetId: found.doc.id };
  }
  // create new doc id
  const incomingStart = incoming.start_date ? sanitize(incoming.start_date) : '';
  let newId;
  if (incoming.strava_id) newId = `strava_${incoming.strava_id}`;
  else newId = sanitize(`${incoming.athleteId || incoming.athleteName || 'anon'}_${Math.round(incoming.distance||0)}_${Math.round(incoming.moving_time||0)}_${incomingStart}`) + '_new';
  return { action: 'create', newId };
}

const tests = [
  { name: 'Strava id match updates', incoming: { athleteId: '100', distance: 3990, moving_time: 1210, strava_id: '111' }, expectAction: 'update', expectedTarget: 'str_111' },
  { name: 'Exact start_date match updates', incoming: { athleteId: '100', distance: 5002, moving_time: 1498, start_date: '2025-11-01T06:00:00Z' }, expectAction: 'update', expectedTarget: 's1' },
  { name: 'Strict numeric but fuzzy should NOT overwrite', incoming: { athleteId: '100', distance: 5003, moving_time: 1496, name: 'Morning Run' }, expectAction: 'create' }
  ,
  { name: "82 left shouldn't be treated as duplicate of 50 left", incoming: { athleteId: '100', distance: 3109.5, moving_time: 2121, elapsed_time: 2639, name: '82 left' }, expectAction: 'create' }
];

let passed = 0;
for (const t of tests) {
  const out = storeIncoming(t.incoming);
  const ok = out.action === t.expectAction && (t.expectedTarget ? out.targetId === t.expectedTarget : true);
  console.log(`${t.name} -> action=${out.action}${out.targetId?(' target=' + out.targetId):(' newId=' + out.newId)} expected=${t.expectAction} => ${ok ? 'PASS' : 'FAIL'}`);
  if (ok) passed++;
}
console.log(`${passed}/${tests.length} tests passed`);
if (passed !== tests.length) process.exitCode = 2;
