// Simulate snapshot creation and reading logic without Firestore
// Run: node scripts/test_snapshot_handling.js

function makeSnapshotFromActivities(activities, aggregatedAt = Date.now()) {
  const rows = activities.map(d => ({ id: d.id, athlete: d.athlete || null, summary: d.summary || null }));
  return { metadata: { aggregated_at: aggregatedAt, rowsCount: rows.length }, rows };
}

function readSnapshot(snapshot) {
  const lastAgg = snapshot.metadata && snapshot.metadata.aggregated_at ? snapshot.metadata.aggregated_at : null;
  const rows = Array.isArray(snapshot.rows) ? snapshot.rows : [];
  const normalized = rows.map(r => ({ id: r.id, athlete: r.athlete || null, summary: r.summary ? { ...r.summary, updated_at: r.summary.updated_at || lastAgg || Date.now() } : { distance: 0, count: 0, longest: 0, avg_pace: null, elev_gain: 0, updated_at: lastAgg || Date.now() } }));
  return { rows: normalized, last_aggregation_at: lastAgg };
}

// sample activities
const activities = [
  { id: 'a1', athlete: { name: 'Alice' }, summary: { distance: 11000, count: 2, longest: 7000, avg_pace: 300, elev_gain: 15 } },
  { id: 'b2', athlete: { name: 'Bob' }, summary: { distance: 34000, count: 4, longest: 10000, avg_pace: 290, elev_gain: 80, updated_at: 1690000000000 } },
];

const snapshot = makeSnapshotFromActivities(activities, 1650000000000);
console.log('Snapshot created:', JSON.stringify(snapshot, null, 2));
const read = readSnapshot(snapshot);
console.log('Read snapshot normalized:', JSON.stringify(read, null, 2));

// assertions
if (!Array.isArray(read.rows) || read.rows.length !== activities.length) {
  console.error('Snapshot read failed: wrong row count');
  process.exitCode = 2;
} else {
  console.log('Snapshot handling test passed');
}
