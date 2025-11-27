// Test that cleanup would delete duplicates even when timestamps are missing
// We'll simulate groups and run the selection logic locally.

function tsOf(d) { return Number(d && (d.created_at || d.fetched_at || d.updated_at) || Number.MAX_SAFE_INTEGER); }

function chooseKeep(list) {
  let keep = list[0];
  const hasAnyTs = list.some(item => tsOf(item.data) !== Number.MAX_SAFE_INTEGER);
  if (hasAnyTs) {
    for (const item of list) if (tsOf(item.data) < tsOf(keep.data)) keep = item;
  } else {
    const withStravaId = list.find(item => item.data && item.data.strava_id);
    if (withStravaId) keep = withStravaId;
    else {
      const withStravaSource = list.find(item => item.data && (item.data.source === 'strava_api' || (item.data.source && String(item.data.source).toLowerCase().includes('strava'))));
      if (withStravaSource) keep = withStravaSource;
      else keep = list[0];
    }
  }
  return keep;
}

// Case A: no timestamps, one doc has strava_id
const groupA = [
  { id: 'a1', data: { athlete_id: '100', name: 'Run A', distance: 5000, moving_time: 1500, elapsed_time: 1510, total_elevation_gain: 10, source: 'manual' } },
  { id: 'a2', data: { athlete_id: '100', name: 'Run A', distance: 5000, moving_time: 1500, elapsed_time: 1510, total_elevation_gain: 10, source: 'strava_api', strava_id: '999' } },
  { id: 'a3', data: { athlete_id: '100', name: 'Run A', distance: 5000, moving_time: 1500, elapsed_time: 1510, total_elevation_gain: 10, source: 'manual' } }
]

// Case B: no timestamps, none with strava hints
const groupB = [
  { id: 'b1', data: { athlete_id: '200', name: 'Run B', distance: 4000, moving_time: 1200, elapsed_time: 1210, total_elevation_gain: 5 } },
  { id: 'b2', data: { athlete_id: '200', name: 'Run B', distance: 4000, moving_time: 1200, elapsed_time: 1210, total_elevation_gain: 5 } }
]

// Case C: timestamps present -> keep oldest
const groupC = [
  { id: 'c1', data: { athlete_id: '300', name: 'Run C', distance: 3000, moving_time: 1000, elapsed_time: 1010, total_elevation_gain: 2, created_at: 2000 } },
  { id: 'c2', data: { athlete_id: '300', name: 'Run C', distance: 3000, moving_time: 1000, elapsed_time: 1010, total_elevation_gain: 2, created_at: 1000 } }
]

console.log('keep A ->', chooseKeep(groupA).id)
console.log('keep B ->', chooseKeep(groupB).id)
console.log('keep C ->', chooseKeep(groupC).id)

if (chooseKeep(groupA).id !== 'a2') process.exitCode = 2;
if (chooseKeep(groupB).id !== 'b1') process.exitCode = 2; // keep first when nothing better
if (chooseKeep(groupC).id !== 'c2') process.exitCode = 2; // keep oldest
