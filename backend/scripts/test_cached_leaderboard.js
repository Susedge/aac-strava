// Test script for cached leaderboard retrieval
// Run: node scripts/test_cached_leaderboard.js

function formatActivitiesDocs(docs, lastAggregationAt) {
  // Simulate the backend normalization flow in GET /activities after aggregation
  const rows = docs.map(d => ({ id: d.id, ...d }));
  const normalizedRows = rows.map(r => ({
    id: r.id,
    athlete: r.athlete || null,
    summary: r.summary ? { ...r.summary, updated_at: r.summary.updated_at || lastAggregationAt || Date.now() } : { distance: 0, count: 0, longest: 0, avg_pace: null, elev_gain: 0, updated_at: lastAggregationAt || Date.now() }
  }));
  return { rows: normalizedRows, last_aggregation_at: lastAggregationAt };
}

const docs = [
  { id: 'alice', athlete: { name: 'Alice', firstname: 'Alice', lastname: 'A.' }, summary: { distance: 12000, count: 2, longest: 7000, avg_pace: 280, elev_gain: 40 } },
  { id: 'bob', athlete: { name: 'Bob' }, summary: { distance: 34000, count: 4, longest: 10000, avg_pace: 300, elev_gain: 120, updated_at: 1690000000000 } },
  { id: 'carol', athlete: { name: 'Carol' } }, // missing summary
];

const lastAgg = Date.now() - 1000 * 60 * 60; // 1 hour ago
const out = formatActivitiesDocs(docs, lastAgg);
console.log(JSON.stringify(out, null, 2));
