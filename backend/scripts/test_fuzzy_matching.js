// Local test script to exercise fuzzy matching used by /aggregate/weekly
// Run: node scripts/test_fuzzy_matching.js

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
    try {
      const ms = new Date(String(v)).getTime();
      return Number.isNaN(ms) ? null : ms;
    } catch (e) { return null; }
  };

  const curMs = toMs(startDateVal);

  for (let i = 0; i < Math.min(MAX_CANDIDATES, candidates.length); i++) {
    const d = candidates[i];
    // start_date proximity
    // If incoming has a Strava id and candidate has the same id, match immediately
    if (stravaIdVal && d.strava_id && String(d.strava_id) === String(stravaIdVal)) return { matched: true, doc: d, reason: 'strava_id match', type: 'strava_id' };

    if (d.start_date && curMs) {
      const candMs = toMs(d.start_date);
      if (candMs && Math.abs(candMs - curMs) <= START_DATE_TOLERANCE_MS) {
        return { matched: true, doc: d, reason: 'start_date close', type: 'start_date_strict' };
      }
      // within looser window -> allow looser numeric match
      if (candMs && Math.abs(candMs - curMs) <= START_DATE_TOLERANCE_LOOSE_MS) {
        const candDist = Number(d.distance || 0);
        const candMt = Number(d.moving_time || 0);
        const candElapsed = Number(d.elapsed_time || 0);
        const incElapsed = Number(elapsedVal || 0);
        const distLoose = Math.abs(candDist - Number(distanceVal || 0)) <= 50 || (Math.abs(candDist - Number(distanceVal || 0)) / Math.max(1, Math.max(Math.abs(candDist), Math.abs(Number(distanceVal || 0))))) <= 0.03;
        const mtLoose = Math.abs(candMt - Number(movingTimeVal || 0)) <= 60 || (incElapsed && Math.abs(candElapsed - incElapsed) <= 60);
        if (distLoose && mtLoose) return { matched: true, doc: d, reason: 'start_date_loose+loose_numeric', type: 'start_date_loose' };
      }
      // If start dates are present on both records but outside the loose window,
      // they are unlikely to be the same activity - skip candidate.
      if (candMs && Math.abs(candMs - curMs) > START_DATE_TOLERANCE_LOOSE_MS) continue;
    }

    const candDist = Number(d.distance || 0);
    const candMt = Number(d.moving_time || 0);
    const candElapsed = Number(d.elapsed_time || 0);
    const incElapsed = Number(elapsedVal || 0);
    // strict numeric check
    const distStrict = Math.abs(candDist - Number(distanceVal || 0)) <= 10 || (Math.abs(candDist - Number(distanceVal || 0)) / Math.max(1, Math.max(Math.abs(candDist), Math.abs(Number(distanceVal || 0))))) <= 0.02;
    const mtStrict = Math.abs(candMt - Number(movingTimeVal || 0)) <= 10 || (incElapsed && Math.abs(candElapsed - incElapsed) <= 10);
    if (distStrict && mtStrict) {
      // require name or elevation similarity to avoid collapsing distinct activities
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
      // scoring to approximate server logic
      let score = 0;
      if (nameMatch) score += 3;
      if (distStrict) score += 2;
      if (mtStrict) score += 2;
      if (typeMatch) score += 1;
      if (sportMatch) score += 1;
      if (workoutMatch) score += 1;
      if (elevMatch) score += 1;
      const elapsedMatch = incElapsed && candElapsed && Math.abs(candElapsed - incElapsed) <= 10;
      if (elapsedMatch) score += 1;
      // Conservative policy: require a strong signal (name, elapsed, or elevation)
      // before declaring a numeric-only match to avoid collapsing distinct activities.
      // Conservative policy: require a strong signal (name or elapsed time) for matching.
      // Elevation alone is not considered a sufficient tiebreaker anymore.
      if (score >= 6 && (nameMatch || elapsedMatch)) return { matched: true, doc: d, reason: 'strict_numeric_full', type: 'strict_numeric_full' };
      if (score >= 4 && (nameMatch || elapsedMatch)) return { matched: true, doc: d, reason: 'strict_numeric+nameOrElapsed', type: 'strict_numeric' };
    }
  }

  return { matched: false };
}

// Sample stored raw activities
  const stored = [
  { id: 'a1', resource_state: 2, athlete: { resource_state: 2, firstname: 'Alice', lastname: 'A.' }, athlete_id: '100', athlete_name: 'Alice', distance: 5000, moving_time: 1500, elapsed_time: 1510, start_date: '2025-11-01T06:00:00Z', name: 'Morning Run', total_elevation_gain: 10, source: 'strava_api', strava_id: '9998', type: 'Run', sport_type: 'Run', workout_type: null },
  // stored activity with explicit Strava id
  { id: 'a_strava', resource_state: 2, athlete: { resource_state: 2, firstname: 'Alice', lastname: 'A.' }, athlete_id: '100', athlete_name: 'Alice', distance: 4000, moving_time: 1200, elapsed_time: 1210, start_date: '2025-11-01T05:00:00Z', name: 'Special Run', total_elevation_gain: 5, source: 'strava_api', strava_id: '9999', type: 'Run', sport_type: 'Run', workout_type: null },
  // another distinct activity by same athlete at a different time
  { id: 'a1b', resource_state: 2, athlete: { resource_state: 2, firstname: 'Alice', lastname: 'A.' }, athlete_id: '100', athlete_name: 'Alice', distance: 5003, moving_time: 1496, start_date: '2025-11-01T08:00:00Z', name: 'Evening Workout', total_elevation_gain: 12, source: 'strava_api', type: 'Run', sport_type: 'Run', workout_type: null },
  { id: 'a2', resource_state: 2, athlete: { resource_state: 2, firstname: 'Bob', lastname: '' }, athlete_id: '200', athlete_name: 'Bob', distance: 10000, moving_time: 3600, start_date: '2025-11-02T06:30:00Z', name: 'Long Ride', total_elevation_gain: 30, source: 'strava_api', type: 'Ride', sport_type: 'Ride', workout_type: null },
  { id: 'a3', resource_state: 2, athlete: null, athlete_id: null, athlete_name: 'Charlie', distance: 7000, moving_time: 2200, name: 'Trail Run', total_elevation_gain: 20, source: 'manual', type: 'Run', sport_type: 'Run', workout_type: null }
];

const tests = [
  // near-match by athlete_id with small distance/time differences
  { incoming: { athleteId: '100', distance: 5005, moving_time: 1498, elapsed_time: 1510, start_date: '2025-11-01T06:00:25Z', type: 'Run', sport_type: 'Run', workout_type: null }, expect: true },
  // slightly outside time tolerance
  { incoming: { athleteId: '100', distance: 5020, moving_time: 1560, elapsed_time: 1565, start_date: '2025-11-01T06:04:00Z', type: 'Run', sport_type: 'Run', workout_type: null }, expect: true },
  // similar but distinct activity (different start_date by 2 hours) — should NOT match
  { incoming: { athleteId: '100', distance: 5002, moving_time: 1498, start_date: '2025-11-01T09:00:00Z', type: 'Run', sport_type: 'Run', workout_type: null }, expect: false },
  // same athlete, numbers close but different name -> should NOT match
  { incoming: { athleteId: '100', distance: 5002, moving_time: 1498, name: 'Different Run', type: 'Run', sport_type: 'Run', workout_type: null }, expect: false },
  // same athlete, numbers close and same name -> should match a1b or a1 accordingly
  { incoming: { athleteId: '100', distance: 5003, moving_time: 1496, elapsed_time: 1496, name: 'Evening Workout', type: 'Run', sport_type: 'Run', workout_type: null }, expect: true },
  // match by strava_id should succeed even if numbers differ slightly
  { incoming: { athleteId: '100', distance: 3990, moving_time: 1210, elapsed_time: 1210, strava_id: '9999', type: 'Run', sport_type: 'Run', workout_type: null }, expect: true },
  // distance too far
  { incoming: { athleteId: '100', distance: 5200, moving_time: 1500, type: 'Run', sport_type: 'Run', workout_type: null }, expect: false },
  // match by athlete_name
  { incoming: { athleteName: 'Charlie', distance: 6990, moving_time: 2210, elapsed_time: 2210, name: 'Trail Run', type: 'Run', sport_type: 'Run', workout_type: null }, expect: true },
  // no match
  { incoming: { athleteId: '300', distance: 5000, moving_time: 1500, type: 'Run', sport_type: 'Run', workout_type: null }, expect: false }
  ,
  // real-world bug regression test: similar distances but significantly different moving_time
  // '82 left' vs '50 left' (should NOT match / be treated as duplicate)
  { incoming: { athleteId: '100', distance: 3109.5, moving_time: 2121, elapsed_time: 2639, name: '82 left', type: 'Run', sport_type: 'Run', workout_type: null }, expect: false, note: '82 left older' },
  { incoming: { athleteId: '100', distance: 3170.7, moving_time: 2303, elapsed_time: 2780, name: '50 left', type: 'Run', sport_type: 'Run', workout_type: null }, expect: false, note: '50 left newer' }
];

console.log('Running fuzzy-matching tests...');
let ok = 0;
  for (const t of tests) {
  // Mirror the server behavior: only search candidate docs when athleteId or athleteName exist.
  const candidates = t.incoming.athleteId ? stored.filter(s => s.athlete_id === t.incoming.athleteId) : (t.incoming.athleteName ? stored.filter(s => String(s.athlete_name || '').toLowerCase() === String(t.incoming.athleteName || '').toLowerCase()) : []);
  const res = candidates.length ? findFuzzyMatchInArray(candidates, { distanceVal: t.incoming.distance, movingTimeVal: t.incoming.moving_time, startDateVal: t.incoming.start_date, nameVal: t.incoming.name, elevationVal: t.incoming.elevation_gain, elapsedVal: t.incoming.elapsed_time, stravaIdVal: t.incoming.strava_id, typeVal: t.incoming.type, sportTypeVal: t.incoming.sport_type, workoutTypeVal: t.incoming.workout_type, athleteResourceState: t.incoming.athlete && t.incoming.athlete.resource_state }) : { matched: false };
  const passed = Boolean(res.matched) === Boolean(t.expect);
  console.log(`Test incoming=${JSON.stringify(t.incoming)} -> matched=${res.matched}${res.matched ? ' id='+res.doc.id+' reason='+res.reason : ''} expected=${t.expect} => ${passed ? 'PASS' : 'FAIL'}`);
  if (passed) ok++;
}

console.log(`\n${ok}/${tests.length} tests passed.`);

if (ok !== tests.length) process.exitCode = 2;
