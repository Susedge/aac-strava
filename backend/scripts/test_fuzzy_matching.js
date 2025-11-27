// Local test script to exercise fuzzy matching used by /aggregate/weekly
// Run: node scripts/test_fuzzy_matching.js

function findFuzzyMatchInArray(candidates, {distanceVal, movingTimeVal, startDateVal, nameVal, elevationVal, elapsedVal, stravaIdVal}) {
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
      const candEg = Number(d.elevation_gain || d.total_elevation_gain || 0);
      const incEg = Number(elevationVal || 0);
      const elevMatch = Number.isFinite(candEg) && Math.abs(candEg - incEg) <= 5;
      if (nameMatch || elevMatch) return { matched: true, doc: d, reason: 'strict_numeric+nameOrElev', type: 'strict_numeric' };
    }
  }

  return { matched: false };
}

// Sample stored raw activities
  const stored = [
  { id: 'a1', athlete_id: '100', athlete_name: 'Alice', distance: 5000, moving_time: 1500, start_date: '2025-11-01T06:00:00Z', name: 'Morning Run', elevation_gain: 10, source: 'strava_api', strava_id: '9998', elapsed_time: 1510 },
  // stored activity with explicit Strava id
  { id: 'a_strava', athlete_id: '100', athlete_name: 'Alice', distance: 4000, moving_time: 1200, start_date: '2025-11-01T05:00:00Z', name: 'Special Run', elevation_gain: 5, source: 'strava_api', strava_id: '9999', elapsed_time: 1210 },
  // another distinct activity by same athlete at a different time
  { id: 'a1b', athlete_id: '100', athlete_name: 'Alice', distance: 5003, moving_time: 1496, start_date: '2025-11-01T08:00:00Z', name: 'Evening Workout', elevation_gain: 12, source: 'strava_api' },
  { id: 'a2', athlete_id: '200', athlete_name: 'Bob', distance: 10000, moving_time: 3600, start_date: '2025-11-02T06:30:00Z' },
  { id: 'a3', athlete_id: null, athlete_name: 'Charlie', distance: 7000, moving_time: 2200 }
];

const tests = [
  // near-match by athlete_id with small distance/time differences
  { incoming: { athleteId: '100', distance: 5005, moving_time: 1498, start_date: '2025-11-01T06:00:25Z' }, expect: true },
  // slightly outside time tolerance
  { incoming: { athleteId: '100', distance: 5020, moving_time: 1560, start_date: '2025-11-01T06:04:00Z' }, expect: true },
  // similar but distinct activity (different start_date by 2 hours) — should NOT match
  { incoming: { athleteId: '100', distance: 5002, moving_time: 1498, start_date: '2025-11-01T09:00:00Z' }, expect: false },
  // same athlete, numbers close but different name -> should NOT match
  { incoming: { athleteId: '100', distance: 5002, moving_time: 1498, name: 'Different Run' }, expect: false },
  // same athlete, numbers close and same name -> should match a1b or a1 accordingly
  { incoming: { athleteId: '100', distance: 5003, moving_time: 1496, name: 'Evening Workout' }, expect: true },
  // match by strava_id should succeed even if numbers differ slightly
  { incoming: { athleteId: '100', distance: 3990, moving_time: 1210, strava_id: '9999' }, expect: true },
  // distance too far
  { incoming: { athleteId: '100', distance: 5200, moving_time: 1500 }, expect: false },
  // match by athlete_name
  { incoming: { athleteName: 'Charlie', distance: 6990, moving_time: 2210 }, expect: true },
  // no match
  { incoming: { athleteId: '300', distance: 5000, moving_time: 1500 }, expect: false }
];

console.log('Running fuzzy-matching tests...');
let ok = 0;
  for (const t of tests) {
  // Mirror the server behavior: only search candidate docs when athleteId or athleteName exist.
  const candidates = t.incoming.athleteId ? stored.filter(s => s.athlete_id === t.incoming.athleteId) : (t.incoming.athleteName ? stored.filter(s => String(s.athlete_name || '').toLowerCase() === String(t.incoming.athleteName || '').toLowerCase()) : []);
  const res = candidates.length ? findFuzzyMatchInArray(candidates, { distanceVal: t.incoming.distance, movingTimeVal: t.incoming.moving_time, startDateVal: t.incoming.start_date, nameVal: t.incoming.name, elevationVal: t.incoming.elevation_gain }) : { matched: false };
  const passed = Boolean(res.matched) === Boolean(t.expect);
  console.log(`Test incoming=${JSON.stringify(t.incoming)} -> matched=${res.matched}${res.matched ? ' id='+res.doc.id+' reason='+res.reason : ''} expected=${t.expect} => ${passed ? 'PASS' : 'FAIL'}`);
  if (passed) ok++;
}

console.log(`\n${ok}/${tests.length} tests passed.`);

if (ok !== tests.length) process.exitCode = 2;
