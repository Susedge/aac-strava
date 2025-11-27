// Quick test harness for the cleanup duplicate-key logic (exact match detection)

function buildDupKey(data) {
  if (!data) return null;
  // Athlete identity
  let fn = '';
  let ln = '';
  if (data.athlete && typeof data.athlete === 'object') {
    fn = (data.athlete.firstname || data.athlete.first_name || '').toString().trim();
    ln = (data.athlete.lastname || data.athlete.last_name || '').toString().trim();
  } else if (data.athlete_name) {
    const parts = data.athlete_name.toString().trim().split(/\s+/);
    fn = parts[0] || '';
    ln = parts.slice(1).join(' ') || '';
  }
  const activityName = (data.name || '').toString().trim();
  const athleteId = data.athlete && (data.athlete.id || data.athlete.id_str || data.athlete_id) ? String(data.athlete.id || data.athlete.id_str || data.athlete_id) : '';

  const rawDistanceMeters = Number(data.distance || data.distance_m || 0);
  const moving_time = Number(data.moving_time || 0);
  const elapsed_time = Number(data.elapsed_time || 0);
  const elev = Number(data.total_elevation_gain || data.elevation_gain || data.elev_total || 0);

  const parts = [
    athleteId || (fn + ' ' + ln).trim(),
    activityName,
    String(rawDistanceMeters),
    String(moving_time),
    String(elapsed_time),
    String(elev)
  ];

  return parts.map(s => (s || '').toString().toLowerCase().replace(/\s+/g, ' ').trim()).join('|');
}

const a = {
  athlete: { id: 100, firstname: 'k1LLu4', lastname: 'Z.' },
  name: '82 left',
  distance: 3109.5,
  moving_time: 2121,
  elapsed_time: 2639,
  total_elevation_gain: 5.5
}

const b = {
  athlete: { id: 100, firstname: 'k1LLu4', lastname: 'Z.' },
  name: '50 left',
  distance: 3170.7,
  moving_time: 2303,
  elapsed_time: 2780,
  total_elevation_gain: 3.3
}

const c = Object.assign({}, a);

console.log('key a:', buildDupKey(a));
console.log('key b:', buildDupKey(b));
console.log('key c:', buildDupKey(c));

console.log('a === c?', buildDupKey(a) === buildDupKey(c));
console.log('a === b?', buildDupKey(a) === buildDupKey(b));

if (buildDupKey(a) === buildDupKey(c) && buildDupKey(a) !== buildDupKey(b)) process.exitCode = 0; else process.exitCode = 2;
