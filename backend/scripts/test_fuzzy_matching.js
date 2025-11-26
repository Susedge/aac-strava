// Local test script to exercise fuzzy matching used by /aggregate/weekly
// Run: node scripts/test_fuzzy_matching.js

function findFuzzyMatchInArray(candidates, {distanceVal, movingTimeVal, startDateVal}) {
  const MAX_CANDIDATES = 50;
  const DISTANCE_TOLERANCE_ABS = 50; // meters
  const DISTANCE_TOLERANCE_REL = 0.03; // 3%
  const MT_TOLERANCE_SEC = 60; // seconds
  const START_DATE_TOLERANCE_MS = 5 * 60 * 1000; // 5 minutes

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
    if (d.start_date && curMs) {
      const candMs = toMs(d.start_date);
      if (candMs && Math.abs(candMs - curMs) <= START_DATE_TOLERANCE_MS) {
        return { matched: true, doc: d, reason: 'start_date close' };
      }
    }

    const candDist = Number(d.distance || 0);
    const candMt = Number(d.moving_time || 0);
    const distMatch = withinTolerance(candDist, Number(distanceVal || 0));
    const mtMatch = Math.abs(candMt - Number(movingTimeVal || 0)) <= MT_TOLERANCE_SEC;
    if (distMatch && mtMatch) return { matched: true, doc: d, reason: 'distance+mt tolerant' };
  }

  return { matched: false };
}

// Sample stored raw activities
const stored = [
  { id: 'a1', athlete_id: '100', athlete_name: 'Alice', distance: 5000, moving_time: 1500, start_date: '2025-11-01T06:00:00Z' },
  { id: 'a2', athlete_id: '200', athlete_name: 'Bob', distance: 10000, moving_time: 3600, start_date: '2025-11-02T06:30:00Z' },
  { id: 'a3', athlete_id: null, athlete_name: 'Charlie', distance: 7000, moving_time: 2200 }
];

const tests = [
  // near-match by athlete_id with small distance/time differences
  { incoming: { athleteId: '100', distance: 5005, moving_time: 1498, start_date: '2025-11-01T06:00:25Z' }, expect: true },
  // slightly outside time tolerance
  { incoming: { athleteId: '100', distance: 5020, moving_time: 1560, start_date: '2025-11-01T06:04:00Z' }, expect: true },
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
  const res = candidates.length ? findFuzzyMatchInArray(candidates, { distanceVal: t.incoming.distance, movingTimeVal: t.incoming.moving_time, startDateVal: t.incoming.start_date }) : { matched: false };
  const passed = Boolean(res.matched) === Boolean(t.expect);
  console.log(`Test incoming=${JSON.stringify(t.incoming)} -> matched=${res.matched}${res.matched ? ' id='+res.doc.id+' reason='+res.reason : ''} expected=${t.expect} => ${passed ? 'PASS' : 'FAIL'}`);
  if (passed) ok++;
}

console.log(`\n${ok}/${tests.length} tests passed.`);

if (ok !== tests.length) process.exitCode = 2;
