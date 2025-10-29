require('dotenv').config();
const express = require('express');
const cors = require('cors');
const axios = require('axios');
const admin = require('firebase-admin');
const path = require('path');
const fs = require('fs');

const app = express();
app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 4000;

// Initialize Firebase Admin if credentials are provided. For local dev, if
// GOOGLE_APPLICATION_CREDENTIALS is not set we'll try to load a
// backend/service-account.json automatically so Firestore works out-of-the-box.
let initializedFirestore = false;
const tryInitWithCred = (credObj, source) => {
  try {
    admin.initializeApp({ credential: admin.credential.cert(credObj) });
    const proj = (credObj && (credObj.project_id || credObj.projectId)) || process.env.FIREBASE_PROJECT_ID || '(unknown project)';
    console.log('Firebase Admin initialized from', source, 'project:', proj);
    initializedFirestore = true;
  } catch (e) {
    console.warn('Failed initializing Firebase Admin from', source, e && e.message || e);
  }
};

if (process.env.GOOGLE_APPLICATION_CREDENTIALS) {
  const credPath = path.isAbsolute(process.env.GOOGLE_APPLICATION_CREDENTIALS)
    ? process.env.GOOGLE_APPLICATION_CREDENTIALS
    : path.join(process.cwd(), process.env.GOOGLE_APPLICATION_CREDENTIALS);
  if (fs.existsSync(credPath)) {
    const cred = require(credPath);
    tryInitWithCred(cred, credPath);
  } else {
    console.log(`Firebase credentials not found at ${credPath}.`);
  }
}

// Fallback: look for a local service-account.json in the backend folder
if (!initializedFirestore) {
  const localPaths = [
    path.join(process.cwd(), 'service-account.json'),
    path.join(__dirname, '..', 'service-account.json'),
    path.join(__dirname, '..', 'service-account', 'service-account.json')
  ];
  for (const p of localPaths) {
    if (fs.existsSync(p)) {
      try {
        const cred = require(p);
        tryInitWithCred(cred, p);
        break;
      } catch (e) {
        console.warn('Error requiring local service account at', p, e && e.message || e);
      }
    }
  }
}

if (!initializedFirestore) console.log('Firestore not initialized. Set GOOGLE_APPLICATION_CREDENTIALS or place service-account.json in the backend folder.');

const db = admin.apps.length ? admin.firestore() : null;

// Helper: parse a date string YYYY-MM-DD as midnight in the given timezone (simple offset map)
function parseDateWithTZ(dateStr, tz) {
  if (!dateStr) return NaN;
  // Simple timezone offset map (hours). Add entries as needed.
  const tzMap = {
    'Asia/Manila': 8,
    'PH': 8,
    'PHT': 8,
    'PST': 8, // treat PST as Philippine Standard Time for this app
    'Philippines': 8,
    'UTC': 0
  };
  const offset = tzMap[tz] !== undefined ? tzMap[tz] : 0;
  // Interpret dateStr as YYYY-MM-DD at 00:00 in that timezone and convert to UTC millis
  const parsedUtcMidnight = Date.parse(dateStr + 'T00:00:00Z');
  if (Number.isNaN(parsedUtcMidnight)) return NaN;
  // Adjust by timezone offset (positive offset means local time = UTC + offset)
  return parsedUtcMidnight - offset * 3600 * 1000;
}

// Normalize athlete/profile fields from various shapes returned by Strava or stored docs
function normalizeAthleteData(obj) {
  if (!obj) return null;
  // obj may already be an athlete object, or a member record with top-level profile fields
  const a = Object.assign({}, obj);
  // common Strava fields: profile
  if (!a.profile && (a.athlete_profile || a.profile_url)) a.profile = a.athlete_profile || a.profile_url;
  // Some backend shapes may have keys using camelCase
  if (!a.profile && a.athleteProfile) a.profile = a.athleteProfile;
  // legacy camelCase profile fields handled above; profile_medium intentionally not mapped (avatars removed)
  return a;
}

// NOTE: removed remote Strava athlete enrichment. Avatars/profile_medium are no longer fetched server-side.

// Strava helper functions
async function refreshAccessTokenIfNeeded(athleteDoc) {
  // athleteDoc contains tokens from oauth exchange
  if (!athleteDoc || !athleteDoc.refresh_token) return athleteDoc;
  const expiresAt = athleteDoc.expires_at || 0;
  const now = Math.floor(Date.now() / 1000);
  if (expiresAt - now > 60) return athleteDoc; // still valid

  try {
    const resp = await axios.post('https://www.strava.com/oauth/token', {
      client_id: process.env.STRAVA_CLIENT_ID,
      client_secret: process.env.STRAVA_CLIENT_SECRET,
      grant_type: 'refresh_token',
      refresh_token: athleteDoc.refresh_token,
    });
    const data = resp.data;
    if (db) await db.collection('athletes').doc(String(data.athlete.id)).set(data, { merge: true });
    return data;
  } catch (err) {
    console.error('refresh failed', err.response ? err.response.data : err.message);
    return athleteDoc;
  }
}

// Persist admin token helper: writes admin/strava doc with club preference
async function persistAdminToken({ access_token, refresh_token, athlete_id, expires_at }) {
  console.log('persistAdminToken: called with', { athlete_id, expires_at, hasAccess: !!access_token, hasRefresh: !!refresh_token });
  if (!db) {
    console.warn('persistAdminToken: Firestore not initialized, skipping persist');
    return null;
  }
  const adminDoc = {
    access_token,
    refresh_token,
    athlete_id: athlete_id || null,
    expires_at: expires_at || null,
    updated_at: Date.now()
  };

  try {
    const ref = db.collection('admin').doc('strava');
    // First write base doc to guarantee creation
    console.log('persistAdminToken: initial write of base admin doc');
    await ref.set(adminDoc, { merge: true });
    try {
      const chk1 = await ref.get();
      console.log('persistAdminToken: base write ok, keys=', chk1.exists ? Object.keys(chk1.data()) : null);
    } catch (r) { console.warn('persistAdminToken: base readback failed', r && r.message || r); }

    // Attempt to detect club from Strava using the access token and prefer configured club id
    try {
      const clubsResp = await axios.get('https://www.strava.com/api/v3/athlete/clubs', {
        headers: { Authorization: `Bearer ${access_token}` }
      });
      const clubs = Array.isArray(clubsResp.data) ? clubsResp.data : [];
      console.log('persistAdminToken: fetched clubs count=', clubs.length, 'list=', clubs.map(c=>({ id: c.id, name: c.name })).slice(0,20));
      if (clubs.length > 0) {
        const TARGET_CLUB_ID = process.env.STRAVA_CLUB_ID ? String(process.env.STRAVA_CLUB_ID) : '1388675';
        let selected = clubs.find(c => String(c.id) === TARGET_CLUB_ID) || null;
        if (!selected) selected = clubs[0];
        if (selected) {
          adminDoc.club_id = selected.id;
          adminDoc.club_name = selected.name;
          console.log('persistAdminToken: updating admin doc with club', { id: selected.id, name: selected.name });
          await ref.set({ club_id: selected.id, club_name: selected.name, updated_at: Date.now() }, { merge: true });
        }
      }
    } catch (e) {
      console.warn('persistAdminToken: failed to fetch clubs', e && e.message || e);
    }

    // Final readback
    try {
      const chk = await ref.get();
      console.log('persistAdminToken: wrote admin/strava, exists=', !!chk.exists, 'keys=', chk.exists ? Object.keys(chk.data()) : null);
    } catch (r) { console.warn('persistAdminToken: final readback failed', r && r.message || r); }
    return adminDoc;
  } catch (we) {
    console.error('persistAdminToken: failed to persist admin token', we && we.message || we);
    return null;
  }
}

// Simple health
app.get('/health', (req, res) => res.json({ status: 'ok', timestamp: Date.now() }));

// Redirect legacy static user page requests to the SPA index so React handles the route
app.get(['/user', '/user.html'], (req, res) => {
  // If query contains id, map to /user/{id} and preserve other params
  try {
    const raw = req.url || '';
    const qs = raw.includes('?') ? raw.slice(raw.indexOf('?')) : '';
    const params = new URLSearchParams(qs);
    const id = params.get('id') || params.get('athlete_id');
    if (id) {
      params.delete('id'); params.delete('athlete_id');
      const rest = params.toString();
      return res.redirect('/user/' + encodeURIComponent(id) + (rest ? ('?' + rest) : ''));
    }
  } catch (e) { /* ignore parsing errors */ }
  // default redirect to /user
  return res.redirect('/user');
});

// Return admin/strava doc for debugging
app.get('/admin/strava', async (req, res) => {
  if (!db) return res.status(500).json({ error: 'firestore not initialized' });
  try {
    const doc = await db.collection('admin').doc('strava').get();
    if (!doc.exists) return res.status(404).json({ error: 'admin/strava doc not found' });
    return res.json({ ok: true, data: doc.data() });
  } catch (e) {
    console.error('GET /admin/strava failed', e.message || e);
    return res.status(500).json({ error: 'failed to read admin/strava', detail: e.message || String(e) });
  }
});

// Seed admin/strava doc for local testing
app.post('/admin/seed', async (req, res) => {
  if (!db) return res.status(500).json({ error: 'firestore not initialized' });
  try {
    const body = req.body || {};
    const adminDoc = {
      access_token: body.access_token || 'TEST_ACCESS_TOKEN',
      refresh_token: body.refresh_token || 'TEST_REFRESH_TOKEN',
      athlete_id: body.athlete_id || null,
      expires_at: body.expires_at || Math.floor(Date.now() / 1000) + 24 * 3600,
      club_id: body.club_id || process.env.STRAVA_CLUB_ID || '1388675',
      club_name: body.club_name || 'AAC Active Club',
      updated_at: Date.now()
    };
    const ref = db.collection('admin').doc('strava');
    await ref.set(adminDoc, { merge: true });
    const chk = await ref.get();
    return res.json({ ok: true, written: chk.exists ? chk.data() : null });
  } catch (e) {
    console.error('admin/seed failed', e.message || e);
    return res.status(500).json({ error: 'admin/seed failed', detail: e.message || String(e) });
  }
});

// Start OAuth flow - frontend will open Strava authorize URL
app.get('/auth/strava/url', (req, res) => {
  const clientId = process.env.STRAVA_CLIENT_ID;
  let redirect = process.env.STRAVA_REDIRECT_URI;
  try {
    if (redirect) {
      const u = new URL(redirect);
      // If using our default /auth/callback path, ensure we point to the static file to avoid SPA fallback
      if (u.pathname === '/auth/callback') {
        u.pathname = '/auth/callback/index.html';
        redirect = u.toString();
      }
    }
  } catch(e) { /* ignore URL parse errors */ }
  const scope = 'read,activity:read_all';
  const url = `https://www.strava.com/oauth/authorize?client_id=${clientId}&response_type=code&redirect_uri=${encodeURIComponent(redirect)}&approval_prompt=auto&scope=${scope}`;
  console.log('GET /auth/strava/url ->', url);
  res.json({ url });
});

// Exchange code for token
app.post('/auth/strava/callback', async (req, res) => {
  const { code } = req.body;
  console.log('POST /auth/strava/callback received body:', req.body ? Object.keys(req.body) : req.body);
  console.log('code:', code);
  if (!code) return res.status(400).json({ error: 'missing code' });
  try {
    const resp = await axios.post('https://www.strava.com/oauth/token', {
      client_id: process.env.STRAVA_CLIENT_ID,
      client_secret: process.env.STRAVA_CLIENT_SECRET,
      code,
      grant_type: 'authorization_code'
    });
    console.log('Strava token response status:', resp.status);
    const data = resp.data;
    // Log key parts but avoid printing full tokens in case of shared logs
    console.log('Strava token response keys:', Object.keys(data));
    console.log('Strava token response athlete.id:', data && data.athlete && data.athlete.id);
    console.log('Strava token response contains access_token:', !!(data && data.access_token), 'refresh_token:', !!(data && data.refresh_token), 'expires_at:', data && data.expires_at);

    // store athlete tokens in firestore
    if (db) {
      await db.collection('athletes').doc(String(data.athlete.id)).set(data, { merge: true });
    }

    // Also store admin token document for club aggregation so the admin doesn't need to manually copy it.
    // This will overwrite the admin token each time the admin performs OAuth.
    try {
      if (db) {
        const adminDoc = {
          access_token: data.access_token,
          refresh_token: data.refresh_token,
          athlete_id: data.athlete && data.athlete.id,
          expires_at: data.expires_at || null,
          updated_at: Date.now()
        };

        // Try to fetch the admin's clubs from Strava and persist the club id/name
        try {
          const clubsResp = await axios.get('https://www.strava.com/api/v3/athlete/clubs', {
            headers: { Authorization: `Bearer ${data.access_token}` }
          });
          const clubs = Array.isArray(clubsResp.data) ? clubsResp.data : [];
          if (clubs.length > 0) {
            // Prefer AAC Active Club (id 1388675) if present. Fall back to env configured club, then first club.
            const TARGET_CLUB_ID = '1388675';
            let selected = clubs.find(c => String(c.id) === TARGET_CLUB_ID) || null;
            if (selected) {
              console.log('Selected target AAC club from admin clubs:', TARGET_CLUB_ID);
            } else {
              const envClub = process.env.STRAVA_CLUB_ID ? String(process.env.STRAVA_CLUB_ID) : null;
              if (envClub) {
                selected = clubs.find(c => String(c.id) === envClub) || null;
                if (selected) console.log('Matched configured STRAVA_CLUB_ID to admin club:', envClub);
              }
            }
            if (!selected) selected = clubs[0];
            const c = selected;
            adminDoc.club_id = c.id;
            adminDoc.club_name = c.name;
            console.log('Found admin club from Strava:', c.id, c.name);
          } else {
            console.log('No clubs returned for admin athlete');
          }
        } catch (clubErr) {
          console.warn('Failed to fetch admin clubs from Strava', clubErr.response ? clubErr.response.data : clubErr.message);
        }

        try {
          const p = await persistAdminToken({ access_token: adminDoc.access_token, refresh_token: adminDoc.refresh_token, athlete_id: adminDoc.athlete_id, expires_at: adminDoc.expires_at });
          console.log('persistAdminToken returned:', p ? { athlete_id: p.athlete_id, club_id: p.club_id, club_name: p.club_name } : null);
        } catch (writeErr) {
          console.error('Failed to persist admin token via helper', writeErr && writeErr.message || writeErr);
        }
      }
    } catch (e) {
      console.warn('Failed to write admin token to Firestore', e.message || e);
    }

    // Optionally include minimal clubs list for debugging
    let clubsOut = null;
    try {
      const clubsResp = await axios.get('https://www.strava.com/api/v3/athlete/clubs', {
        headers: { Authorization: `Bearer ${data.access_token}` }
      });
      const clubs = Array.isArray(clubsResp.data) ? clubsResp.data : [];
      clubsOut = clubs.map(c => ({ id: c.id, name: c.name }));
    } catch (e) { /* ignore */ }
    res.json({ ok: true, data, clubs: clubsOut });
  } catch (err) {
    console.error(err.response ? err.response.data : err.message);
    res.status(500).json({ error: 'token exchange failed' });
  }
});

// A placeholder endpoint to compute leaderboard from stored athletes
app.get('/leaderboard/weekly', async (req, res) => {
  if (!db) return res.status(500).json({ error: 'firestore not initialized' });
  try {
    // Return stored athlete documents with refreshed tokens when needed
    const snaps = await db.collection('athletes').get();
    const athletes = [];
    for (const d of snaps.docs) {
      const a = d.data();
      const refreshed = await refreshAccessTokenIfNeeded(a);
      athletes.push(refreshed);
    }
    res.json({ athletes });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'failed' });
  }
});

// Basic aggregator: fetch recent activities for each athlete and store weekly total
app.post('/aggregate/weekly', async (req, res) => {
  if (!db) return res.status(500).json({ error: 'firestore not initialized' });
  try {
    const results = [];
    // Allow overriding the aggregation start date via POST body { start_date: 'YYYY-MM-DD' } or env AGGREGATION_START_DATE
    let startDateParam = null;
    try { startDateParam = req.body && req.body.start_date; } catch (e) {}
    const envDate = process.env.AGGREGATION_START_DATE || null;
    const startDateStr = startDateParam || envDate || null;
    let oneWeekAgo;
    if (startDateStr) {
      // parse YYYY-MM-DD as midnight in Asia/Manila (PHT)
      const parsedMs = parseDateWithTZ(startDateStr, process.env.AGGREGATION_TIMEZONE || 'Asia/Manila');
      if (!Number.isNaN(parsedMs)) {
        oneWeekAgo = Math.floor(parsedMs / 1000);
      } else {
        oneWeekAgo = Math.floor(Date.now() / 1000) - 7 * 24 * 60 * 60;
      }
    } else {
      oneWeekAgo = Math.floor(Date.now() / 1000) - 7 * 24 * 60 * 60;
    }

    // Prefer club-based aggregation if a club id is configured in env or stored on the admin doc
    let clubId = process.env.STRAVA_CLUB_ID || null;
    let token = process.env.STRAVA_ADMIN_TOKEN || null;
    if ((!clubId || !token) && db) {
      try {
        const doc = await db.collection('admin').doc('strava').get();
        if (doc.exists) {
          const d = doc.data();
          clubId = clubId || d && (d.club_id || d.clubId) || null;
          token = token || (d && d.access_token) || null;
        }
      } catch (e) { console.warn('failed reading admin/strava doc', e.message || e); }
    }

    if (clubId) {
      if (!token) return res.status(500).json({ error: 'STRAVA_ADMIN_TOKEN not set in env and no admin token found in Firestore. Please connect as admin.' });

      try {
        // First fetch club members so we can list members first and ensure zero-summary entries
        const members = [];
        try {
          const perPage = 200;
          let page = 1;
          while (true) {
            const r = await axios.get(`https://www.strava.com/api/v3/clubs/${clubId}/members`, {
              params: { per_page: perPage, page },
              headers: { Authorization: `Bearer ${token}` }
            });
            const chunk = r.data || [];
            members.push(...chunk);
            if (chunk.length < perPage) break; // Stop when no more members available
            page += 1;
          }
          console.log(`Fetched ${members.length} total club members across ${page} pages`);
        } catch (memErr) {
          console.warn('failed fetching club members', memErr.response ? memErr.response.data : memErr.message);
        }

        // Then fetch club activities (paged)
        const perPage = 200;
        let page = 1;
        let acts = [];
        while (true) {
          const r = await axios.get(`https://www.strava.com/api/v3/clubs/${clubId}/activities`, {
            params: { per_page: perPage, page, after: oneWeekAgo },
            headers: { Authorization: `Bearer ${token}` }
          });
          const chunk = r.data || [];
          acts = acts.concat(chunk);
          if (chunk.length < perPage) break; // Stop when no more activities available
          page += 1;
        }
        console.log(`Fetched ${acts.length} total club activities across ${page} pages`);

        // Store each activity in raw_activities collection for data preservation
        try {
          if (acts.length === 0) {
            console.log('No activities to store - skipping batch commit');
          } else {
            const batch = db.batch();
            let storedCount = 0;
            for (const act of acts) {
              // Club activities don't have individual activity IDs
              // Create a unique ID based on athlete + distance only (ignore date) so aggregation
              // can detect existing records even if start_date varies between sources
              const athleteName = act.athlete ? `${act.athlete.firstname || ''} ${act.athlete.lastname || ''}`.trim() : 'unknown';
              const athleteId = act.athlete && act.athlete.id ? String(act.athlete.id) : null;
              const distance = Number(act.distance || 0);
              
              // Generate a unique document ID based on athlete and distance (no date)
              const uniqueId = `${athleteId || athleteName}_${Math.round(distance)}`.replace(/[^a-zA-Z0-9_-]/g, '_');
              
              const activityDoc = {
                athlete_id: athleteId,
                athlete_name: athleteName,
                distance: distance,
                moving_time: Number(act.moving_time || 0),
                start_date: startDate,
                type: act.type || 'Run',
                name: act.name || 'Activity',
                elevation_gain: Number(act.total_elevation_gain || act.elev_total || 0),
                source: 'strava_api',
                fetched_at: Date.now(),
                updated_at: Date.now()
              };
              
              // Use generated ID to avoid duplicates
              batch.set(db.collection('raw_activities').doc(uniqueId), activityDoc, { merge: true });
              storedCount++;
            }
            
            if (storedCount > 0) {
              await batch.commit();
              console.log(`Stored ${storedCount} activities in raw_activities collection`);
            } else {
              console.log('No valid activities to store');
            }
          }
        } catch (storeErr) {
          console.error('Failed to store raw activities:', storeErr);
          console.error('Error details:', storeErr.message || storeErr);
        }

        // Load manual activities from raw_activities collection
        let manualActivities = [];
        try {
          const manualSnaps = await db.collection('raw_activities')
            .where('source', 'in', ['manual', 'bulk_import'])
            .get();
          manualActivities = manualSnaps.docs.map(d => d.data());
          console.log(`Loaded ${manualActivities.length} manual activities from raw_activities`);
        } catch (manualErr) {
          console.warn('Failed to load manual activities', manualErr.message || manualErr);
        }

        // Combine Strava activities + manual activities
        const allActivities = [...acts, ...manualActivities.map(ma => {
          const athleteName = ma.athlete_name || '';
          
          // Create athlete object that matches Strava format (name-based, no ID needed)
          return {
            athlete: {
              firstname: athleteName.split(' ')[0] || '',
              lastname: athleteName.split(' ').slice(1).join(' ') || ''
            },
            distance: ma.distance,
            moving_time: ma.moving_time,
            start_date: ma.start_date,
            type: ma.type,
            name: ma.name,
            total_elevation_gain: ma.elevation_gain
          };
        })];
        console.log(`Total activities (Strava + Manual): ${allActivities.length}`);

        // Aggregate activities by athlete name ONLY (since club activities don't have IDs)
        const agg = new Map();
        const makeAthleteKey = (a) => {
          if (!a) return null;
          // Use name-based key since club activities don't have athlete IDs
          const fn = (a.firstname || '').trim();
          const ln = (a.lastname || '').trim();
          const fullName = (fn + ' ' + ln).trim();
          if (fullName) return `name:${fullName}`;
          if (a.username) return `name:${a.username}`;
          if (a.name) return `name:${a.name}`;
          return null;
        };

        for (const it of allActivities) {
          if (it.start_date) {
            const ts = Date.parse(it.start_date);
            if (!Number.isNaN(ts) && ts / 1000 < oneWeekAgo) continue;
          }
          const key = makeAthleteKey(it.athlete) || null;
          if (!key) continue; // Skip activities without identifiable athlete
          
          const cur = agg.get(key) || { athlete: it.athlete || null, distance: 0, count: 0, longest: 0, total_moving_time: 0, elev_gain: 0 };
          const dist = Number(it.distance || 0);
          const mt = Number(it.moving_time || 0);
          const eg = Number(it.total_elevation_gain || it.elev_total || 0);
          cur.distance += dist;
          cur.count += 1;
          cur.longest = Math.max(cur.longest || 0, dist);
          cur.total_moving_time += mt;
          cur.elev_gain += eg;
          if (!cur.athlete && it.athlete) cur.athlete = it.athlete;
          agg.set(key, cur);
        }

        // Prepare results: members first (with zero summary if no activity), then other contributors
        const makeMemberKey = (m) => {
          if (!m) return null;
          if (m.id) return String(m.id);
          if (m.athlete) return makeAthleteKey(m.athlete);
          return makeAthleteKey(m);
        };
        const memberIds = new Set(members.map(m => makeMemberKey(m)).filter(Boolean));
        // Write weekly docs for members first
        for (const m of members) {
          const mid = makeMemberKey(m) || null;
          if (!mid) continue;
          const v = agg.get(mid) || agg.get(String(mid)) || agg.get(Number(mid)) || null;
          let summary;
          if (v) {
            const avgPaceSecPerKm = v.distance > 0 ? Math.round((v.total_moving_time / (v.distance / 1000))) : null;
            summary = { distance: v.distance, count: v.count, longest: v.longest, avg_pace: avgPaceSecPerKm, elev_gain: v.elev_gain, updated_at: Date.now() };
            let athleteObj = normalizeAthleteData(v.athlete || (m && (m.athlete || m)) || null);
            // persist a lightweight summary athlete record for quick lookups (no avatar enrichment)
            try { if (db && athleteObj) await db.collection('summary_athletes').doc(String(mid)).set({ id: mid, name: athleteObj.firstname || athleteObj.first_name || athleteObj.name || athleteObj.username || null, profile: athleteObj.profile || null, username: athleteObj.username || null, updated_at: Date.now() }, { merge: true }); } catch(e){ console.warn('failed writing summary_athletes', e.message || e); }
            await db.collection('activities').doc(String(mid)).set({ athlete: athleteObj, summary }, { merge: true });
            results.push({ id: mid, summary });
          } else {
            // no activity for this member in the period
            summary = { distance: 0, count: 0, longest: 0, avg_pace: null, elev_gain: 0, updated_at: Date.now() };
            let athleteObj = normalizeAthleteData((m && (m.athlete || m)) || { name: (m && (m.firstname || m.lastname)) || null });
            try { if (db && athleteObj) await db.collection('summary_athletes').doc(String(mid)).set({ id: mid, name: athleteObj.firstname || athleteObj.first_name || athleteObj.name || athleteObj.username || null, profile: athleteObj.profile || null, username: athleteObj.username || null, updated_at: Date.now() }, { merge: true }); } catch(e){ console.warn('failed writing summary_athletes', e.message || e); }
            await db.collection('activities').doc(String(mid)).set({ athlete: athleteObj, summary }, { merge: true });
            results.push({ id: mid, summary });
          }
        }

        // Then include any other athlete keys from the aggregated activities that were not in members
        for (const [id, v] of agg.entries()) {
          if (memberIds.has(id) || memberIds.has(String(id)) || memberIds.has(Number(id))) continue;
          const avgPaceSecPerKm = v.distance > 0 ? Math.round((v.total_moving_time / (v.distance / 1000))) : null;
          const summary = { distance: v.distance, count: v.count, longest: v.longest, avg_pace: avgPaceSecPerKm, elev_gain: v.elev_gain, updated_at: Date.now() };
          const docId = String(id).startsWith('name:') || String(id).startsWith('username:') ? String(id) : String(id);
          let athleteNorm = normalizeAthleteData(v.athlete);
          try { if (db && athleteNorm) await db.collection('summary_athletes').doc(String(docId)).set({ id: docId, name: athleteNorm.firstname || athleteNorm.first_name || athleteNorm.name || athleteNorm.username || null, profile: athleteNorm.profile || null, username: athleteNorm.username || null, updated_at: Date.now() }, { merge: true }); } catch(e){ console.warn('failed writing summary_athletes', e.message || e); }
          await db.collection('activities').doc(docId).set({ athlete: athleteNorm, summary }, { merge: true });
          results.push({ id: docId, summary });
        }

        return res.json({ ok: true, results, membersCount: members.length, activitiesCount: acts.length });
      } catch (err) {
        console.error('club activities fetch failed', err.response ? err.response.data : err.message);
        return res.status(500).json({ error: 'club aggregation failed' });
      }
    }

    // Fallback: aggregate per-athlete as before
    const snaps = await db.collection('athletes').get();
    for (const d of snaps.docs) {
      let a = d.data();
      a = await refreshAccessTokenIfNeeded(a);
      if (!a.access_token) continue;
      // fetch activities
      try {
        const r = await axios.get('https://www.strava.com/api/v3/athlete/activities', {
          params: { after: oneWeekAgo, per_page: 200 },
          headers: { Authorization: `Bearer ${a.access_token}` }
        });
        const acts = r.data || [];
        let totalMeters = 0, longest = 0, totalMoving = 0, elevGain = 0;
        for (const it of acts) {
          const dist = Number(it.distance || 0);
          totalMeters += dist;
          longest = Math.max(longest, dist);
          totalMoving += Number(it.moving_time || 0);
          elevGain += Number(it.total_elevation_gain || it.elev_total || 0);
        }
        const avgPaceSecPerKm = totalMeters > 0 ? Math.round(totalMoving / (totalMeters / 1000)) : null;
  const summary = { distance: totalMeters, count: acts.length, longest, avg_pace: avgPaceSecPerKm, elev_gain: elevGain, updated_at: Date.now() };
        let athleteNorm = normalizeAthleteData(a.athlete);
        try { if (db && athleteNorm) await db.collection('summary_athletes').doc(String(a.athlete.id)).set({ id: String(a.athlete.id), name: athleteNorm.firstname || athleteNorm.first_name || athleteNorm.name || athleteNorm.username || null, profile: athleteNorm.profile || null, username: athleteNorm.username || null, updated_at: Date.now() }, { merge: true }); } catch(e){ console.warn('failed writing summary_athletes', e.message || e); }
        await db.collection('activities').doc(String(a.athlete.id)).set({ athlete: athleteNorm, summary }, { merge: true });
    results.push({ id: a.athlete.id, summary });
      } catch (err) {
        console.error('activities fetch failed for', a.athlete && a.athlete.id, err.response ? err.response.data : err.message);
      }
    }

    res.json({ ok: true, results });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'aggregate failed' });
  }
});

// Refresh admin stored Strava token using its refresh_token
app.post('/admin/refresh', async (req, res) => {
  if (!db) return res.status(500).json({ error: 'firestore not initialized' });
  try {
    const docRef = db.collection('admin').doc('strava');
    const doc = await docRef.get();
    if (!doc.exists) return res.status(404).json({ error: 'no admin/strava doc found' });
    const data = doc.data();
    const refreshToken = data && (data.refresh_token || data.refreshToken || null);
    if (!refreshToken) return res.status(400).json({ error: 'no refresh_token available on admin/strava' });

    // Call Strava token endpoint to refresh
    const resp = await axios.post('https://www.strava.com/oauth/token', {
      client_id: process.env.STRAVA_CLIENT_ID,
      client_secret: process.env.STRAVA_CLIENT_SECRET,
      grant_type: 'refresh_token',
      refresh_token: refreshToken
    });
    const refreshed = resp.data;

    // Persist refreshed tokens using helper
    try {
      await persistAdminToken({ access_token: refreshed.access_token, refresh_token: refreshed.refresh_token, athlete_id: refreshed.athlete && refreshed.athlete.id, expires_at: refreshed.expires_at });
    } catch (we) {
      console.error('Failed to persist refreshed admin token via helper', we && we.message || we);
      return res.status(500).json({ error: 'failed to persist refreshed admin token', detail: we && we.message || String(we) });
    }

    return res.json({ ok: true, data: refreshed });
  } catch (err) {
    console.error('admin/refresh failed', err.response ? err.response.data : err.message);
    return res.status(500).json({ error: 'refresh failed', detail: err.response ? err.response.data : err.message });
  }
});

// Debug: fetch raw club activities (requires STRAVA_ADMIN_TOKEN)
// Fetches ALL club activities without any date filtering or limits
app.get('/debug/club-activities', async (req, res) => {
  let clubId = process.env.STRAVA_CLUB_ID || null;
  let token = process.env.STRAVA_ADMIN_TOKEN || null;
  if ((!clubId || !token) && db) {
    try {
      const doc = await db.collection('admin').doc('strava').get();
      if (doc.exists) {
        const d = doc.data();
        clubId = clubId || d && (d.club_id || d.clubId) || null;
        token = token || (d && d.access_token) || null;
      }
    } catch (e) { console.warn('failed reading admin/strava doc', e.message || e); }
  }
  if (!clubId) return res.status(400).json({ error: 'STRAVA_CLUB_ID not set' });
  if (!token) return res.status(400).json({ error: 'STRAVA_ADMIN_TOKEN not set' });
  try {
    console.log(`Debug: Fetching ALL club activities (no date filter) for club ${clubId}`);
    
    // Paginate through all activities with NO date filtering
    const perPage = 50;
    let page = 1;
    let allActivities = [];
    while (true) {
      const r = await axios.get(`https://www.strava.com/api/v3/clubs/${clubId}/activities`, {
        params: { per_page: perPage, page }, // No 'after' parameter = get all activities
        headers: { Authorization: `Bearer ${token}` }
      });
      const chunk = r.data || [];
      allActivities = allActivities.concat(chunk);
      console.log(`Debug: Fetched page ${page}, got ${chunk.length} activities (total so far: ${allActivities.length})`);
      if (chunk.length < perPage) break; // Stop when no more activities available
      page += 1;
      
      // Safety limit to prevent infinite loops or excessive API calls (max 50 pages = 10,000 activities)
      if (page > 50) {
        console.warn(`Debug: Hit safety limit of 50 pages (10,000 activities)`);
        break;
      }
    }
    console.log(`Debug: Total activities fetched: ${allActivities.length} across ${page} pages`);
    res.json({ ok: true, activities: allActivities, count: allActivities.length, pages: page, note: 'All club activities (no date filter)' });
  } catch (err) {
    console.error('debug club fetch failed', err.response ? err.response.data : err.message);
    res.status(500).json({ error: 'failed to fetch club activities' });
  }
});

// Debug: fetch club activities with date range support
// Use ?start_date=YYYY-MM-DD to fetch activities from that date forward
// Use ?before=YYYY-MM-DD to fetch activities before that date (for historical data)
app.get('/debug/club-activities-range', async (req, res) => {
  let clubId = process.env.STRAVA_CLUB_ID || null;
  let token = process.env.STRAVA_ADMIN_TOKEN || null;
  if ((!clubId || !token) && db) {
    try {
      const doc = await db.collection('admin').doc('strava').get();
      if (doc.exists) {
        const d = doc.data();
        clubId = clubId || d && (d.club_id || d.clubId) || null;
        token = token || (d && d.access_token) || null;
      }
    } catch (e) { console.warn('failed reading admin/strava doc', e.message || e); }
  }
  if (!clubId) return res.status(400).json({ error: 'STRAVA_CLUB_ID not set' });
  if (!token) return res.status(400).json({ error: 'STRAVA_ADMIN_TOKEN not set' });
  
  try {
    const startDateStr = req.query && req.query.start_date ? String(req.query.start_date) : '2025-09-13';
    const beforeDateStr = req.query && req.query.before ? String(req.query.before) : null;
    
    let afterTimestamp = null;
    let beforeTimestamp = null;
    
    if (startDateStr) {
      const parsedMs = parseDateWithTZ(startDateStr, 'Asia/Manila');
      afterTimestamp = Number.isNaN(parsedMs) ? null : Math.floor(parsedMs / 1000);
    }
    
    if (beforeDateStr) {
      const parsedMs = parseDateWithTZ(beforeDateStr, 'Asia/Manila');
      beforeTimestamp = Number.isNaN(parsedMs) ? null : Math.floor(parsedMs / 1000);
    }
    
    console.log(`Debug: Fetching club activities - after: ${afterTimestamp ? new Date(afterTimestamp * 1000).toISOString() : 'none'}, before: ${beforeTimestamp ? new Date(beforeTimestamp * 1000).toISOString() : 'none'}`);
    
    const perPage = 200;
    let page = 1;
    let allActivities = [];
    
    while (true) {
      const params = { per_page: perPage, page };
      if (afterTimestamp) params.after = afterTimestamp;
      if (beforeTimestamp) params.before = beforeTimestamp;
      
      const r = await axios.get(`https://www.strava.com/api/v3/clubs/${clubId}/activities`, {
        params,
        headers: { Authorization: `Bearer ${token}` }
      });
      const chunk = r.data || [];
      allActivities = allActivities.concat(chunk);
      console.log(`Debug: Fetched page ${page}, got ${chunk.length} activities (total so far: ${allActivities.length})`);
      
      if (chunk.length < perPage) break;
      page += 1;
      
      if (page > 100) {
        console.warn(`Debug: Hit safety limit of 100 pages`);
        break;
      }
    }
    
    console.log(`Debug: Total activities fetched: ${allActivities.length} across ${page} pages`);
    res.json({ 
      ok: true, 
      activities: allActivities, 
      count: allActivities.length, 
      pages: page,
      filters: {
        after: afterTimestamp ? new Date(afterTimestamp * 1000).toISOString() : null,
        before: beforeTimestamp ? new Date(beforeTimestamp * 1000).toISOString() : null
      }
    });
  } catch (err) {
    console.error('debug club activities range failed', err.response ? err.response.data : err.message);
    res.status(500).json({ error: 'failed to fetch club activities' });
  }
});

// Debug: list persisted activities docs
app.get('/debug/activities', async (req, res) => {
  if (!db) return res.status(500).json({ error: 'firestore not initialized' });
  try {
    const snaps = await db.collection('activities').get();
    const rows = snaps.docs.map(d => ({ id: d.id, ...d.data() }));
    res.json({ ok: true, rows });
  } catch (e) {
    res.status(500).json({ error: e.message || String(e) });
  }
});

// GET single activity doc by id; enrich athlete profile from Strava if missing
app.get('/activities/:id', async (req, res) => {
  if (!db) return res.status(500).json({ error: 'firestore not initialized' });
  const id = req.params && req.params.id;
  if (!id) return res.status(400).json({ error: 'id required' });
  try {
    const docRef = db.collection('activities').doc(String(id));
    const doc = await docRef.get();
    if (!doc.exists) return res.status(404).json({ error: 'not found' });
    const data = doc.data();
    let athlete = data && data.athlete ? normalizeAthleteData(data.athlete) : null;
    // If athlete has no profile_medium, try to enrich via admin token
    let adminToken = process.env.STRAVA_ADMIN_TOKEN || null;
    try {
      const adminDoc = await db.collection('admin').doc('strava').get();
      if (adminDoc.exists) {
        const d = adminDoc.data();
        adminToken = adminToken || (d && d.access_token) || null;
      }
    } catch (e) { /* ignore */ }

    // No enrichment: return stored athlete info as-is

    return res.json({ ok: true, id, athlete, summary: data.summary || null });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: e.message || String(e) });
  }
});

// (Duplicate /activities/:id handler removed) single activity endpoint above returns stored data as-is.

// Debug: fetch club members (requires STRAVA_ADMIN_TOKEN or admin token in Firestore)
app.get('/debug/club-members', async (req, res) => {
  let clubId = process.env.STRAVA_CLUB_ID || null;
  let token = process.env.STRAVA_ADMIN_TOKEN || null;
  if ((!clubId || !token) && db) {
    try {
      const doc = await db.collection('admin').doc('strava').get();
      if (doc.exists) {
        const d = doc.data();
        clubId = clubId || d && (d.club_id || d.clubId) || null;
        token = token || (d && d.access_token) || null;
      }
    } catch (e) { console.warn('failed reading admin/strava doc', e.message || e); }
  }
  if (!clubId) return res.status(400).json({ error: 'STRAVA_CLUB_ID not set' });
  if (!token) return res.status(400).json({ error: 'STRAVA_ADMIN_TOKEN not set' });
  try {
    const members = [];
    const perPage = 200;
    let page = 1;
    while (true) {
      const r = await axios.get(`https://www.strava.com/api/v3/clubs/${clubId}/members`, {
        params: { per_page: perPage, page },
        headers: { Authorization: `Bearer ${token}` }
      });
      const chunk = r.data || [];
      members.push(...chunk);
      if (chunk.length < perPage) break;
      page += 1;
    }
    res.json({ ok: true, members, count: members.length });
  } catch (err) {
    console.error('debug club members fetch failed', err.response ? err.response.data : err.message);
    res.status(500).json({ error: 'failed to fetch club members' });
  }
});

// Debug: fetch clubs for a given access token or the stored admin token
app.get('/debug/admin-clubs', async (req, res) => {
  try {
    let token = req.query && req.query.token ? String(req.query.token) : null;
    if (!token && db) {
      try {
        const doc = await db.collection('admin').doc('strava').get();
        if (doc.exists) {
          const d = doc.data();
          token = d && d.access_token || null;
        }
      } catch(e) { /* ignore */ }
    }
    if (!token) return res.status(400).json({ error: 'no token provided and no admin token stored' });
    const r = await axios.get('https://www.strava.com/api/v3/athlete/clubs', {
      headers: { Authorization: `Bearer ${token}` }
    });
    const clubs = Array.isArray(r.data) ? r.data : [];
    return res.json({ ok: true, count: clubs.length, clubs: clubs.map(c => ({ id: c.id, name: c.name })) });
  } catch (err) {
    return res.status(500).json({ error: err.response ? err.response.data : err.message });
  }
});

app.get('/activities', async (req, res) => {
  if (!db) return res.status(500).json({ error: 'firestore not initialized' });
  try {
    // Compute leaderboard from raw_activities instead of pre-aggregated summaries
    const snaps = await db.collection('raw_activities').get();
    const allActivities = snaps.docs.map(d => d.data());
    
    console.log(`Computing leaderboard from ${allActivities.length} raw activities`);
    
    // Aggregate by athlete name
    const agg = new Map();
    
    for (const act of allActivities) {
      const athleteNameRaw = (act.athlete_name || '').trim();
      if (!athleteNameRaw) continue;
      // Clean accidental leading zeros (some stored names start with '0')
      const athleteName = athleteNameRaw.replace(/^0+(?=[A-Za-z])/, '').trim();
      if (!athleteName) continue;

      const key = athleteName.toLowerCase(); // Case-insensitive grouping
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
      const eg = Number(act.elevation_gain || 0);
      
      cur.distance += dist;
      cur.count += 1;
      cur.longest = Math.max(cur.longest, dist);
      cur.total_moving_time += mt;
      cur.elev_gain += eg;
      
      agg.set(key, cur);
    }
    
    // Load athlete metadata (nickname, goal) from summary_athletes
    const athleteSnaps = await db.collection('summary_athletes').get();
    const athleteMetadata = new Map();
    athleteSnaps.docs.forEach(doc => {
      const data = doc.data();
      const rawName = (data.name || '').toString().trim();
      const cleanName = rawName.replace(/^0+(?=[A-Za-z])/, '').toLowerCase();
      athleteMetadata.set(cleanName, {
        id: doc.id,
        nickname: data.nickname || null,
        goal: data.goal || 0,
        name: data.name || ''
      });
    });
    
    // Build response rows
    const rows = [];
    for (const [key, summary] of agg.entries()) {
      const meta = athleteMetadata.get(key);
      const avgPaceSecPerKm = summary.distance > 0 ? Math.round(summary.total_moving_time / (summary.distance / 1000)) : null;

      // Prefer stored nickname for display; fallback to summary name
      const displayName = meta && meta.nickname ? meta.nickname : summary.name;

      rows.push({
        id: meta ? meta.id : `name:${summary.name}`,
        athlete: {
          name: displayName,
          nickname: meta ? meta.nickname : null,
          firstname: summary.name.split(' ')[0] || '',
          lastname: summary.name.split(' ').slice(1).join(' ') || '',
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
    
    res.json({ rows });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'failed' });
  }
});

// Debug endpoint: return admin token document stored in Firestore
app.get('/debug/admin', async (req, res) => {
  if (!db) return res.status(500).json({ error: 'firestore not initialized' });
  try {
    const doc = await db.collection('admin').doc('strava').get();
    if (!doc.exists) return res.json({ ok: true, admin: null });
    return res.json({ ok: true, admin: doc.data() });
  } catch (err) {
    console.error('debug admin failed', err);
    res.status(500).json({ error: 'failed' });
  }
});

// ============================================================================
// MANUAL ACTIVITY MANAGEMENT ENDPOINTS
// ============================================================================

// Admin: List all raw activities (individual activity records)
app.get('/admin/raw-activities', async (req, res) => {
  if (!db) return res.status(500).json({ error: 'firestore not initialized' });
  try {
    const athleteName = req.query && req.query.athlete_name ? String(req.query.athlete_name) : null;
    const sortBy = req.query && req.query.sort_by ? String(req.query.sort_by) : 'start_date';
    const sortOrder = req.query && req.query.sort_order ? String(req.query.sort_order) : 'desc';
    
    // Get all activities (we'll filter and sort in-memory to avoid index requirements)
    const snaps = await db.collection('raw_activities').limit(1000).get();
    let activities = snaps.docs.map(d => ({ id: d.id, ...d.data() }));
    
    // Filter by athlete name if provided (case-insensitive)
    if (athleteName) {
      const searchName = athleteName.toLowerCase().trim();
      activities = activities.filter(a => {
        const name = (a.athlete_name || '').toLowerCase().trim();
        return name.includes(searchName);
      });
    }
    
    // Sort in-memory
    activities.sort((a, b) => {
      let valA, valB;
      
      switch (sortBy) {
        case 'start_date':
          valA = a.start_date ? new Date(a.start_date).getTime() : 0;
          valB = b.start_date ? new Date(b.start_date).getTime() : 0;
          break;
        case 'distance':
          valA = Number(a.distance || 0);
          valB = Number(b.distance || 0);
          break;
        case 'athlete_name':
          valA = (a.athlete_name || '').toLowerCase();
          valB = (b.athlete_name || '').toLowerCase();
          break;
        case 'source':
          valA = a.source || '';
          valB = b.source || '';
          break;
        default:
          valA = a.start_date ? new Date(a.start_date).getTime() : 0;
          valB = b.start_date ? new Date(b.start_date).getTime() : 0;
      }
      
      if (sortOrder === 'asc') {
        return valA > valB ? 1 : valA < valB ? -1 : 0;
      } else {
        return valA < valB ? 1 : valA > valB ? -1 : 0;
      }
    });
    
    res.json({ ok: true, activities, count: activities.length });
  } catch (e) {
    console.error('Failed to list raw activities', e);
    res.status(500).json({ error: e.message || String(e) });
  }
});

// Admin: Cleanup duplicate raw activities (keeps latest or prefers strava_api)
app.post('/admin/cleanup-raw-activities', async (req, res) => {
  if (!db) return res.status(500).json({ error: 'firestore not initialized' });
  try {
    const snaps = await db.collection('raw_activities').get();
    const seen = new Map();
    const toDelete = [];

    snaps.docs.forEach(doc => {
      const data = doc.data();
  const name = (data.athlete_name || '').toLowerCase().trim();
  const distance = Math.round(Number(data.distance || 0));
  if (!name) return; // skip unidentifiable
  // Key without date so duplicates are determined by athlete+distance only
  const key = `${name}|${distance}`;

      if (!seen.has(key)) {
        seen.set(key, { id: doc.id, data });
        return;
      }

      // Duplicate found - decide which to keep
      const existing = seen.get(key);
      const preferCurrent = () => {
        seen.set(key, { id: doc.id, data });
        toDelete.push(existing.id);
      };

      // If either one is source 'strava_api', prefer that
      const currIsStrava = data.source === 'strava_api';
      const existingIsStrava = existing.data && existing.data.source === 'strava_api';
      if (currIsStrava && !existingIsStrava) {
        preferCurrent();
      } else if (!currIsStrava && existingIsStrava) {
        toDelete.push(doc.id);
      } else {
        // Neither or both strava_api: keep the one with newer updated_at
        const currUpdated = Number(data.updated_at || 0);
        const existUpdated = Number(existing.data.updated_at || 0);
        if (currUpdated >= existUpdated) {
          preferCurrent();
        } else {
          toDelete.push(doc.id);
        }
      }
    });

    // Perform deletions in batches of 500
    let deleted = 0;
    const batchSize = 500;
    for (let i = 0; i < toDelete.length; i += batchSize) {
      const batch = db.batch();
      const slice = toDelete.slice(i, i + batchSize);
      slice.forEach(id => batch.delete(db.collection('raw_activities').doc(id)));
      await batch.commit();
      deleted += slice.length;
    }

    const kept = snaps.size - deleted;
    console.log(`Cleanup complete. Deleted ${deleted} duplicates, kept ${kept}`);
    res.json({ ok: true, deleted, kept });
  } catch (e) {
    console.error('Cleanup failed', e);
    res.status(500).json({ error: e.message || String(e) });
  }
});

// Admin: Add a new manual activity (using athlete name)
app.post('/admin/raw-activities', async (req, res) => {
  if (!db) return res.status(500).json({ error: 'firestore not initialized' });
  try {
    const { athlete_name, distance, moving_time, start_date, type, name, elevation_gain } = req.body || {};
    
    if (!athlete_name) return res.status(400).json({ error: 'athlete_name required' });
    if (!distance && distance !== 0) return res.status(400).json({ error: 'distance required' });
    if (!start_date) return res.status(400).json({ error: 'start_date required (YYYY-MM-DD or ISO)' });
    
    // No need to lookup athlete_id - we match purely by name
    const activity = {
      athlete_id: null, // Club activities don't have IDs
      athlete_name: athlete_name,
      distance: Number(distance) || 0,
      moving_time: Number(moving_time) || 0,
      start_date: start_date,
      type: type || 'Run',
      name: name || 'Manual Activity',
      elevation_gain: Number(elevation_gain) || 0,
      source: 'manual',
      created_at: Date.now(),
      updated_at: Date.now()
    };
    
    const docRef = await db.collection('raw_activities').add(activity);
    console.log(`Created manual activity ${docRef.id} for athlete ${athlete_name}`);
    
    res.json({ ok: true, activity: { id: docRef.id, ...activity } });
  } catch (e) {
    console.error('Failed to create activity', e);
    res.status(500).json({ error: e.message || String(e) });
  }
});

// Admin: Update an existing activity
app.put('/admin/raw-activities/:id', async (req, res) => {
  if (!db) return res.status(500).json({ error: 'firestore not initialized' });
  const id = req.params && req.params.id;
  if (!id) return res.status(400).json({ error: 'id required' });
  
  try {
    const { athlete_id, athlete_name, distance, moving_time, start_date, type, name, elevation_gain } = req.body || {};
    
    const update = { updated_at: Date.now() };
    if (athlete_id !== undefined) update.athlete_id = String(athlete_id);
    if (athlete_name !== undefined) update.athlete_name = athlete_name;
    if (distance !== undefined) update.distance = Number(distance) || 0;
    if (moving_time !== undefined) update.moving_time = Number(moving_time) || 0;
    if (start_date !== undefined) update.start_date = start_date;
    if (type !== undefined) update.type = type;
    if (name !== undefined) update.name = name;
    if (elevation_gain !== undefined) update.elevation_gain = Number(elevation_gain) || 0;
    
    await db.collection('raw_activities').doc(id).set(update, { merge: true });
    const doc = await db.collection('raw_activities').doc(id).get();
    
    console.log(`Updated activity ${id}`);
    res.json({ ok: true, activity: doc.exists ? { id: doc.id, ...doc.data() } : null });
  } catch (e) {
    console.error('Failed to update activity', e);
    res.status(500).json({ error: e.message || String(e) });
  }
});

// Admin: Delete an activity
app.delete('/admin/raw-activities/:id', async (req, res) => {
  if (!db) return res.status(500).json({ error: 'firestore not initialized' });
  const id = req.params && req.params.id;
  if (!id) return res.status(400).json({ error: 'id required' });
  
  try {
    await db.collection('raw_activities').doc(id).delete();
    console.log(`Deleted activity ${id}`);
    res.json({ ok: true, deleted: id });
  } catch (e) {
    console.error('Failed to delete activity', e);
    res.status(500).json({ error: e.message || String(e) });
  }
});

// Admin: Bulk import activities from CSV/JSON
app.post('/admin/raw-activities/bulk', async (req, res) => {
  if (!db) return res.status(500).json({ error: 'firestore not initialized' });
  try {
    const { activities } = req.body || {};
    if (!Array.isArray(activities)) return res.status(400).json({ error: 'activities array required' });
    
    const batch = db.batch();
    const created = [];
    const errors = [];
    
    for (const act of activities) {
      if (!act.athlete_name || (!act.distance && act.distance !== 0) || !act.start_date) {
        errors.push({ activity: act, error: 'Missing required fields: athlete_name, distance, start_date' });
        continue;
      }
      
      // No need to lookup athlete_id - match purely by name
      const activity = {
        athlete_id: null, // Club activities don't have IDs
        athlete_name: act.athlete_name,
        distance: Number(act.distance) || 0,
        moving_time: Number(act.moving_time) || 0,
        start_date: act.start_date,
        type: act.type || 'Run',
        name: act.name || 'Manual Activity',
        elevation_gain: Number(act.elevation_gain) || 0,
        source: act.source || 'bulk_import',
        created_at: Date.now(),
        updated_at: Date.now()
      };
      
      const docRef = db.collection('raw_activities').doc();
      batch.set(docRef, activity);
      created.push({ id: docRef.id, ...activity });
    }
    
    await batch.commit();
    console.log(`Bulk imported ${created.length} activities (${errors.length} errors)`);
    res.json({ ok: true, imported: created.length, activities: created, errors });
  } catch (e) {
    console.error('Failed to bulk import', e);
    res.status(500).json({ error: e.message || String(e) });
  }
});

// Admin: Export athletes as CSV template for manual data entry
app.get('/admin/export-csv-template', async (req, res) => {
  if (!db) return res.status(500).json({ error: 'firestore not initialized' });
  try {
    const snaps = await db.collection('summary_athletes').orderBy('name').get();
    const athletes = snaps.docs.map(d => ({ id: d.id, ...d.data() }));
    
    let csv = 'athlete_name,distance,moving_time,start_date,type,name,elevation_gain\n';
    
    athletes.forEach(athlete => {
      const name = (athlete.nickname || athlete.name || 'Unknown').replace(/,/g, ' ');
      // Template with example values
      csv += `${name},5000,1800,2025-09-13,Run,September Activity,100\n`;
    });
    
    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', 'attachment; filename="activity-import-template.csv"');
    res.send(csv);
  } catch (e) {
    console.error('Failed to export CSV template', e);
    res.status(500).json({ error: e.message || String(e) });
  }
});

// Admin: list summary athletes
app.get('/admin/athletes', async (req, res) => {
  if (!db) return res.status(500).json({ error: 'firestore not initialized' });
  try {
    const snaps = await db.collection('summary_athletes').orderBy('name').get();
    const rows = snaps.docs.map(d => ({ id: d.id, ...d.data() }));
    res.json({ ok: true, rows });
  } catch (e) { res.status(500).json({ error: e.message || String(e) }); }
});

// Admin: update an athlete's nickname, goal, and optional manual strava id
app.post('/admin/athlete/:id', async (req, res) => {
  if (!db) return res.status(500).json({ error: 'firestore not initialized' });
  const id = req.params && req.params.id;
  const { nickname, goal, manual_strava_id } = req.body || {};
  if (!id) return res.status(400).json({ error: 'id required' });
  try {
    const update = {};
    if (typeof nickname !== 'undefined') update.nickname = nickname || null;
    if (typeof goal !== 'undefined') update.goal = Number(goal) || 0;
    if (typeof manual_strava_id !== 'undefined') update.manual_strava_id = manual_strava_id || null;
    update.updated_at = Date.now();
    await db.collection('summary_athletes').doc(String(id)).set(update, { merge: true });

    // We no longer auto-fetch or persist avatars; manual_strava_id is stored as metadata only

    // Also merge nickname and goal into activities doc so frontend can show it
    try {
      const activityUpdate = {};
      if (typeof nickname !== 'undefined') activityUpdate['athlete.nickname'] = nickname || null;
      if (typeof goal !== 'undefined') activityUpdate['athlete.goal'] = Number(goal) || 0;
      if (typeof manual_strava_id !== 'undefined') activityUpdate['athlete.manual_strava_id'] = manual_strava_id || null;
      if (Object.keys(activityUpdate).length) {
        const athleteUpdate = {};
        if (typeof nickname !== 'undefined') athleteUpdate.nickname = nickname || null;
        if (typeof goal !== 'undefined') athleteUpdate.goal = Number(goal) || 0;
        if (typeof manual_strava_id !== 'undefined') athleteUpdate.manual_strava_id = manual_strava_id || null;
        await db.collection('activities').doc(String(id)).set({ athlete: athleteUpdate }, { merge: true });
      }
    } catch(e){ console.warn('failed merging athlete data into activities', e && e.message || e); }

    const doc = await db.collection('summary_athletes').doc(String(id)).get();
    res.json({ ok: true, athlete: doc.exists ? doc.data() : null });
  } catch (e) { res.status(500).json({ error: e.message || String(e) }); }
});

// Admin: delete a summary athlete and its activity summary (does NOT remove raw_activities)
app.delete('/admin/athlete/:id', async (req, res) => {
  if (!db) return res.status(500).json({ error: 'firestore not initialized' });
  const id = req.params && req.params.id;
  if (!id) return res.status(400).json({ error: 'id required' });
  try {
    // Delete summary_athletes doc
    await db.collection('summary_athletes').doc(String(id)).delete();
    // Also delete aggregated activities doc to keep UI consistent
    try { await db.collection('activities').doc(String(id)).delete(); } catch(e) { /* ignore if missing */ }
    console.log(`Deleted summary athlete ${id} and activities doc if existed`);
    res.json({ ok: true, deleted: id });
  } catch (e) {
    console.error('Failed to delete athlete', e);
    res.status(500).json({ error: e.message || String(e) });
  }
});

// Allow setting the admin club manually (useful if athlete belongs to multiple clubs or detected club is incorrect)
app.post('/admin/set-club', async (req, res) => {
  if (!db) return res.status(500).json({ error: 'firestore not initialized' });
  const { club_id, club_name } = req.body || {};
  if (!club_id) return res.status(400).json({ error: 'club_id required' });
  try {
    await db.collection('admin').doc('strava').set({ club_id, club_name, updated_at: Date.now() }, { merge: true });
    return res.json({ ok: true, club_id, club_name });
  } catch (err) {
    console.error('failed to set admin club', err);
    return res.status(500).json({ error: 'failed to set club' });
  }
});

// Debug endpoint: return raw weekly docs
app.get('/debug/activities-docs', async (req, res) => {
  if (!db) return res.status(500).json({ error: 'firestore not initialized' });
  try {
    const snaps = await db.collection('activities').get();
    const docs = snaps.docs.map(d => ({ id: d.id, data: d.data() }));
    res.json({ ok: true, docs });
  } catch (err) {
    console.error('debug activities docs failed', err);
    res.status(500).json({ error: 'failed' });
  }
});

app.listen(PORT, () => console.log(`Server running on ${PORT}`));
