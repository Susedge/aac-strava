// Local test script to simulate /activities aggregation logic with sample data
// Run: node scripts/test_activities_mapping.js

function simulateAggregation(rawActivities, summaryDocs) {
  // Duplicate of the aggregation logic from /activities endpoint, simplified for local testing
  console.log('Simulating aggregation with', rawActivities.length, 'raw activities and', summaryDocs.length, 'summary docs');

  // Aggregate by athlete name
  const agg = new Map();
  const normalizeNameKey = (s) => {
    if (!s) return '';
    try {
      return s.toString().trim().replace(/^0+/, '').replace(/\./g, '').replace(/[^\w\s]/g, '').replace(/\s+/g, ' ').toLowerCase();
    } catch (e) { return String(s || '').toLowerCase().trim(); }
  };
  for (const act of rawActivities) {
    const athleteNameRaw = (act.athlete_name || '').trim();
    if (!athleteNameRaw && act.athlete) {
      const fn = (act.athlete.firstname || act.athlete.first_name || '').trim();
      const ln = (act.athlete.lastname || act.athlete.last_name || '').trim();
      if (fn || ln) act.athlete_name = `${fn} ${ln}`.trim();
    }
    const athleteNameUse = (act.athlete_name || '').trim();
    if (!athleteNameUse) continue;
    const athleteName = athleteNameUse.replace(/^0+(?=[A-Za-z])/, '').trim();
    if (!athleteName) continue;

  const key = normalizeNameKey(athleteName);
    const cur = agg.get(key) || {
      name: athleteName,
      distance: 0,
      count: 0,
      longest: 0,
      total_moving_time: 0,
      elev_gain: 0
    };

    const dist = Number(act.distance || 0);
    const mt = Number(act.moving_time || 0);
    const eg = Number(act.elevation_gain || act.total_elevation_gain || 0);

    cur.distance += dist;
    cur.count += 1;
    cur.longest = Math.max(cur.longest, dist);
    cur.total_moving_time += mt;
    cur.elev_gain += eg;

    agg.set(key, cur);
  }

  // Build athleteMetadata map similar to server
  const athleteMetadata = new Map();
  for (const doc of summaryDocs) {
    const data = doc.data || {};
    const rawNameField = (data.name || '').toString().trim();
    const fromId = String(doc.id || '').startsWith('name:') ? String(doc.id).replace(/^name:/, '').trim() : null;
    const canonical = rawNameField || fromId || (data.username || '');
    const cleanCanonical = canonical.replace(/^0+(?=[A-Za-z])/, '').trim();
    const storedNick = (data.nickname || '').toString().trim();
    const cleanStoredNick = storedNick ? storedNick.replace(/^0+(?=[A-Za-z])/, '').trim() : '';
    let nicknameToUse = cleanStoredNick || null;
    if (!nicknameToUse && fromId) {
      if ((fromId || '').length > (rawNameField || '').length) {
        nicknameToUse = fromId.replace(/^0+(?=[A-Za-z])/, '').trim();
      }
    }
    const meta = {
      id: doc.id,
      nickname: nicknameToUse,
      goal: Number(data.goal || 0) || 0,
      name: canonical || ''
    };
    const normalizeNameKey = (s) => {
      if (!s) return '';
      try {
        return s.toString().trim().replace(/^0+/, '').replace(/\./g, '').replace(/[^\w\s]/g, '').replace(/\s+/g, ' ').toLowerCase();
      } catch (e) { return String(s || '').toLowerCase().trim(); }
    };
    const k1 = normalizeNameKey(cleanCanonical);
    if (k1) athleteMetadata.set(k1, meta);
    if (fromId) {
      const k2 = normalizeNameKey(fromId.replace(/^0+(?=[A-Za-z])/, '').trim());
      if (k2 && !athleteMetadata.has(k2)) athleteMetadata.set(k2, meta);
    }
    if (cleanCanonical) {
      const first = (cleanCanonical.split(' ')[0] || '').trim();
      const k3 = normalizeNameKey(first);
      if (k3 && !athleteMetadata.has(k3)) athleteMetadata.set(k3, meta);
    }
  }

  const rows = [];
  for (const [key, summary] of agg.entries()) {
    const meta = athleteMetadata.get(key);
    const avgPaceSecPerKm = summary.distance > 0 ? Math.round(summary.total_moving_time / (summary.distance / 1000)) : null;
    const rawSummaryName = (summary.name || '').toString().trim();
    const cleanSummaryName = rawSummaryName.replace(/^0+(?=[A-Za-z])/, '').trim();
    const rawNick = meta && meta.nickname ? String(meta.nickname).trim() : '';
    const cleanNick = rawNick ? rawNick.replace(/^0+(?=[A-Za-z])/, '').trim() : '';
    const displayName = cleanNick || cleanSummaryName || ('Unknown');

    rows.push({
      id: meta ? meta.id : `name:${summary.name}`,
      athlete: {
        name: displayName,
        nickname: cleanNick || null,
        firstname: (cleanSummaryName.split(' ')[0] || '') ,
        lastname: (cleanSummaryName.split(' ').slice(1).join(' ') || ''),
        goal: meta ? meta.goal : 0
      },
      summary: {
        distance: summary.distance,
        count: summary.count,
        longest: summary.longest,
        avg_pace: avgPaceSecPerKm,
        elev_gain: summary.elev_gain,
        updated_at: Date.now()
      }
    });
  }

  return rows;
}

// Sample raw activities including the user-provided record
const rawActivities = [
  // sample Strava-like activity with athlete object (no athlete_name)
  {
    athlete: { resource_state: 2, firstname: 'Jayko', lastname: 'C.' },
    name: 'Morning Run',
    distance: 4121.7,
    moving_time: 2857,
    elapsed_time: 2899,
    total_elevation_gain: 24,
    type: 'Run',
    sport_type: 'Run',
    workout_type: null,
    source: 'strava_api',
    fetched_at: Date.now()
  },
  // sample raw activity with athlete_name and leading zero
  {
    athlete_name: '0Arsel V.',
    name: 'Long Run',
    distance: 95650,
    moving_time: 36000,
    elapsed_time: 36200,
    total_elevation_gain: 120,
    type: 'Run',
    sport_type: 'Run',
    workout_type: null,
    source: 'strava_api',
    fetched_at: Date.now()
  },
  // sample manual activity matching Arsel but with different formatting
  {
    athlete_name: 'Arsel V.',
    name: 'Evening Run',
    distance: 5000,
    moving_time: 1500,
    elapsed_time: 1520,
    total_elevation_gain: 10,
    type: 'Run',
    sport_type: 'Run',
    workout_type: null,
    source: 'manual',
    created_at: Date.now() - 1000 * 60 * 60 * 24 // older
  }
];

// Sample summary_athletes docs
const summaryDocs = [
  { id: 'name:Arsel V.', data: { id: 'name:Arsel V.', name: 'Arsel', nickname: 'Arsel', goal: 100 } },
  { id: 'name:Jayko C.', data: { id: 'name:Jayko C.', name: 'Jayko C.', nickname: null, goal: 0 } }
];

const rows = simulateAggregation(rawActivities, summaryDocs);
console.log('Resulting rows:');
console.log(JSON.stringify(rows, null, 2));

// Print a summary view
console.log('\nSummary view:');
rows.forEach((r, i) => {
  console.log(`${i+1}. displayName=${r.athlete.name}, nickname=${r.athlete.nickname}, goal=${r.athlete.goal}, distance_km=${(r.summary.distance/1000).toFixed(2)}`);
});
