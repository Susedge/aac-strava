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
              // Club activities don't have individual activity IDs in the API response.
              // Try to detect an existing raw_activities doc and update it instead of creating a new one.
              const athleteName = act.athlete ? `${act.athlete.firstname || ''} ${act.athlete.lastname || ''}`.trim() : 'unknown';
              const athleteId = act.athlete && act.athlete.id ? String(act.athlete.id) : null;
              const distance = Number(act.distance || 0);
              const moving_time = Number(act.moving_time || 0);
              const start_date = act.start_date || act.start_date_local || null;

              // Always include canonical fields so documents are uniform across writes.
              // We'll set the `id` field later once we determine the final Firestore doc id
              const activityDocBase = {
                // canonical fields (defaulting to null for missing values)
                athlete_id: athleteId || null,
                athlete_name: athleteName || '',
                distance: Number(distance || 0),
                moving_time: Math.round(Number(moving_time || 0) || 0),
                // elapsed_time always set (fallback to moving_time when missing)
                elapsed_time: typeof act.elapsed_time !== 'undefined' && act.elapsed_time !== null ? Math.round(Number(act.elapsed_time || 0)) : Math.round(Number(act.moving_time || 0) || 0),
                // Strava's activity id saved explicitly too (keeps the original external id)
                strava_id: typeof act.id !== 'undefined' && act.id !== null ? String(act.id) : null,
                // Always include start_date if available
                start_date: start_date || null,
                // Ensure sport_type/workout_type are present (or null)
                sport_type: act.sport_type || null,
                workout_type: typeof act.workout_type !== 'undefined' ? act.workout_type : null,
                type: act.type || 'Run',
                name: act.name || 'Activity',
                elevation_gain: Number(act.total_elevation_gain || act.elev_total || act.elevation_gain || 0),
                source: 'strava_api',
                fetched_at: act.fetched_at || Date.now(),
                updated_at: Date.now()
              };

              let docRef = null;
              let docMatchType = null; // 'strava_id'|'start_date_strict' etc. for decisive matches

              // Generate a deterministic unique document ID. Prefer a Strava-provided activity id
              // when available since it's definitive for that activity. Otherwise include athlete,
              // rounded distance, moving_time and start_date (when available) to reduce accidental duplicates.
              const sanitize = (s) => String(s || '').replace(/[^a-zA-Z0-9_-]/g, '_').replace(/_+/g, '_');
              let uniqueId;
              if (act && act.id) {
                uniqueId = `strava_${String(act.id)}`;
              } else {
                const startToken = start_date ? sanitize(start_date) : '';
                const mtToken = moving_time ? Math.round(moving_time) : 0;
                uniqueId = sanitize(`${athleteId || athleteName}_${Math.round(distance)}_${mtToken}${startToken ? '_' + startToken : ''}`);
              }

              try {
                // If Strava provides an activity id (athlete-specific), prefer matching by it
                if (!docRef && act.id) {
                  try {
                    const qId = await db.collection('raw_activities')
                      .where('strava_id', '==', act.id)
                      .limit(1)
                      .get();
                    if (!qId.empty) { docRef = qId.docs[0].ref; docMatchType = 'strava_id'; }
                  } catch (idErr) { /* ignore */ }
                }

                // Prefer matching by athlete_id + start_date if both available
                if (athleteId && start_date) {
                  const q = await db.collection('raw_activities')
                    .where('athlete_id', '==', athleteId)
                    .where('start_date', '==', start_date)
                    .limit(1)
                    .get();
                  if (!q.empty) { docRef = q.docs[0].ref; docMatchType = 'start_date_strict'; }
                }

                // If not found, try athlete_name + start_date
                if (!docRef && athleteName && start_date) {
                  const q2 = await db.collection('raw_activities')
                    .where('athlete_name', '==', athleteName)
                    .where('start_date', '==', start_date)
                    .limit(1)
                    .get();
                  if (!q2.empty) { docRef = q2.docs[0].ref; docMatchType = 'start_date_strict'; }
                }

                // Fallback: attempt fuzzy matching instead of strict equality
                // Firestore range queries across multiple fields are limited, so fetch a small
                // set of candidate docs by athlete_id or athlete_name then compare in-app
                // using tolerances for distance and moving_time and proximity for start_date.
                const findFuzzyMatch = async ({ byAthleteId, byAthleteName, distanceVal, movingTimeVal, startDateVal, nameVal, elevationVal, elapsedVal, stravaIdVal, typeVal, sportTypeVal, workoutTypeVal, athleteResourceState }) => {
                  const MAX_CANDIDATES = 50; // keep DB query small
                  // Matching thresholds
                  const DISTANCE_TOLERANCE_REL = 0.02; // 2% relative
                  const DISTANCE_TOLERANCE_STRICT = 10; // meters strict
                  const DISTANCE_TOLERANCE_LOOSE = 50; // meters loose
                  const MT_TOLERANCE_STRICT = 10; // seconds strict
                  const MT_TOLERANCE_LOOSE = 60; // seconds loose
                  const START_DATE_TOLERANCE_MS = 2 * 60 * 1000; // 2 minutes
                  const START_DATE_TOLERANCE_LOOSE_MS = 5 * 60 * 1000; // 5 minutes

                  const withinTolerance = (a, b) => {
                    if (typeof a !== 'number' || typeof b !== 'number') return false;
                    const abs = Math.abs(a - b);
                    const rel = Math.abs(a - b) / Math.max(1, Math.max(Math.abs(a), Math.abs(b)));
                    return abs <= DISTANCE_TOLERANCE_LOOSE || rel <= DISTANCE_TOLERANCE_REL;
                  };

                  // Fetch candidate documents (small query) - if this fails, bail safely
                  let q;
                  try {
                    if (byAthleteId) {
                      q = await db.collection('raw_activities').where('athlete_id', '==', byAthleteId).limit(MAX_CANDIDATES).get();
                    } else if (byAthleteName) {
                      q = await db.collection('raw_activities').where('athlete_name', '==', byAthleteName).limit(MAX_CANDIDATES).get();
                    } else {
                      return null;
                    }
                  } catch (err) {
                    console.warn('Fuzzy match query failed', err && err.message || err);
                    return null;
                  }

                  if (!q || q.empty) return null;

                  // New: prefer a very targeted match when searching by athlete_name.
                  // If the candidate already matches athlete_name and the numeric fields
                  // distance, elapsed_time and moving_time are within strict tolerances,
                  // treat it as a definitive match (helps avoid document duplication).
                  if (byAthleteName) {
                    for (const cand of q.docs) {
                      const d = cand.data();
                      // quick sanity checks
                      const candName = (d.athlete_name || '').toString().trim().toLowerCase();
                      const wantName = String(byAthleteName || '').trim().toLowerCase();
                      if (!candName || !wantName || candName !== wantName) continue;

                      const candDist = Number(d.distance || 0);
                      const candMt = Number(d.moving_time || 0);
                      // treat elapsed_time as optional: only compare if both sides provide a value
                      const candElapsed = d.hasOwnProperty('elapsed_time') && d.elapsed_time !== null ? Number(d.elapsed_time) : null;
                      const incDist = Number(distanceVal || 0);
                      const incMt = Number(movingTimeVal || 0);
                      const incElapsedExists = typeof elapsedVal !== 'undefined' && elapsedVal !== null;

                      const distStrict = Math.abs(candDist - incDist) <= DISTANCE_TOLERANCE_STRICT || (Math.abs(candDist - incDist) / Math.max(1, Math.max(Math.abs(candDist), Math.abs(incDist)))) <= DISTANCE_TOLERANCE_REL;
                      const mtStrict = Math.abs(candMt - incMt) <= MT_TOLERANCE_STRICT;
                      let elapsedStrict = true;
                      if (incElapsedExists && candElapsed !== null) {
                        elapsedStrict = Math.abs(candElapsed - Number(elapsedVal)) <= MT_TOLERANCE_STRICT;
                      }

                      if (distStrict && mtStrict && elapsedStrict) {
                        return { ref: cand.ref, type: 'athlete_name_numeric_match' };
                      }
                    }
                  }

                  for (const cand of q.docs) {
                    const d = cand.data();

                    // Definitive match by Strava ID
                    if (stravaIdVal && d.strava_id && String(d.strava_id) === String(stravaIdVal)) return { ref: cand.ref, type: 'strava_id' };

                    // Prefer start_date as a strong indicator when available
                    if (d.start_date && startDateVal) {
                      try {
                        const candMs = new Date(String(d.start_date)).getTime();
                        const curMs = new Date(String(startDateVal)).getTime();
                        if (!Number.isNaN(candMs) && !Number.isNaN(curMs)) {
                          const delta = Math.abs(candMs - curMs);
                          if (delta <= START_DATE_TOLERANCE_MS) return { ref: cand.ref, type: 'start_date_strict' };

                          if (delta <= START_DATE_TOLERANCE_LOOSE_MS) {
                            const candDist = Number(d.distance || 0);
                            const candMt = Number(d.moving_time || 0);
                            const candElapsed = Number(d.elapsed_time || 0);
                            const incElapsed = Number(elapsedVal || 0);
                            const distLoose = Math.abs(candDist - Number(distanceVal || 0)) <= DISTANCE_TOLERANCE_LOOSE || (Math.abs(candDist - Number(distanceVal || 0)) / Math.max(1, Math.max(Math.abs(candDist), Math.abs(Number(distanceVal || 0))))) <= DISTANCE_TOLERANCE_REL;
                            const mtLoose = Math.abs(candMt - Number(movingTimeVal || 0)) <= MT_TOLERANCE_LOOSE || (incElapsed && Math.abs(candElapsed - incElapsed) <= MT_TOLERANCE_LOOSE);
                            if (distLoose && mtLoose) return { ref: cand.ref, type: 'start_date_loose' };
                          }

                          // If start dates exist on both sides but are outside the loose window,
                          // they are unlikely to be the same activity.
                          if (delta > START_DATE_TOLERANCE_LOOSE_MS) continue;
                        }
                      } catch (dateErr) {
                        // ignore bad dates and continue trying other candidates
                      }
                    }

                    // Strict numeric checks (distance + moving_time close)
                    const candDist = Number(d.distance || 0);
                    const candMt = Number(d.moving_time || 0);
                    const candElapsed = Number(d.elapsed_time || 0);
                    const incElapsed = Number(elapsedVal || 0);

                    const distStrict = Math.abs(candDist - Number(distanceVal || 0)) <= DISTANCE_TOLERANCE_STRICT || (Math.abs(candDist - Number(distanceVal || 0)) / Math.max(1, Math.max(Math.abs(candDist), Math.abs(Number(distanceVal || 0))))) <= DISTANCE_TOLERANCE_REL;
                    const mtStrict = Math.abs(candMt - Number(movingTimeVal || 0)) <= MT_TOLERANCE_STRICT || (incElapsed && Math.abs(candElapsed - incElapsed) <= MT_TOLERANCE_STRICT);

                    if (distStrict && mtStrict) {
                      // Normalize activity names before comparing. Many activity names
                      // include embedded distances or numeric tokens (e.g. "45.78 km left").
                      // Strip numeric tokens, km/m unit markers and punctuation so names
                      // like "45.78 km left" and "45.8 km left" match.
                      const normalizeActivityName = (s) => {
                        if (!s) return '';
                        try {
                          // remove numeric distance tokens like '45.78 km' or '500 m'
                          let t = String(s || '').toLowerCase();
                          t = t.replace(/\b\d+(?:[.,]\d+)?\s*(km|kilometer|kilometre|m|meter|metre)\b/gi, '');
                          // remove remaining digits, punctuation and extra whitespace
                          t = t.replace(/[\d,.:]/g, '');
                          t = t.replace(/[^a-z\s]/g, '');
                          t = t.replace(/\s+/g, ' ').trim();
                          return t;
                        } catch (e) { return String(s || '').toLowerCase().trim(); }
                      };

                      const candName = normalizeActivityName(d.name || '');
                      const incName = normalizeActivityName(nameVal || '');
                      const nameMatch = candName && incName && candName === incName;

                      const candType = (d.type || '').toString().trim().toLowerCase();
                      const candSportType = (d.sport_type || '').toString().trim().toLowerCase();
                      const candWorkoutType = typeof d.workout_type !== 'undefined' ? String(d.workout_type) : null;
                      const typeMatch = candType && typeVal && candType === String(typeVal).toLowerCase();
                      const sportTypeMatch = candSportType && sportTypeVal && candSportType === String(sportTypeVal).toLowerCase();
                      const workoutTypeMatch = (typeof d.workout_type !== 'undefined' && typeof workoutTypeVal !== 'undefined') && String(d.workout_type) === String(workoutTypeVal);

                      const candEg = Number(d.elevation_gain || d.total_elevation_gain || 0);
                      const incEg = Number(elevationVal || 0);
                      const elevMatch = Number.isFinite(candEg) && Math.abs(candEg - incEg) <= 5;

                      const elapsedMatch = incElapsed && candElapsed && Math.abs(candElapsed - incElapsed) <= MT_TOLERANCE_STRICT;

                      // Score components
                      let score = 0;
                      if (nameMatch) score += 3;
                      if (distStrict) score += 2;
                      if (mtStrict) score += 2;
                      if (typeMatch) score += 1;
                      if (sportTypeMatch) score += 1;
                      if (workoutTypeMatch) score += 1;
                      if (elevMatch) score += 1;
                      if (elapsedMatch) score += 1;

                      // Conservative match decisions — require name or elapsed to be present for numeric matches
                      if (score >= 6 && (nameMatch || elapsedMatch)) return { ref: cand.ref, type: 'strict_numeric_full' };
                      if (score >= 4 && (nameMatch || elapsedMatch)) return { ref: cand.ref, type: 'strict_numeric' };
                    }

                    // Final very cautious loose fallback: only when no start_date on either side
                    const distLoose = Math.abs(candDist - Number(distanceVal || 0)) <= DISTANCE_TOLERANCE_LOOSE || (Math.abs(candDist - Number(distanceVal || 0)) / Math.max(1, Math.max(Math.abs(candDist), Math.abs(Number(distanceVal || 0))))) <= DISTANCE_TOLERANCE_REL;
                    const mtLoose = Math.abs(candMt - Number(movingTimeVal || 0)) <= MT_TOLERANCE_LOOSE;
                    if (!d.start_date && !startDateVal && distLoose && mtLoose) {
                      if (d.source && d.source === 'strava_api') return { ref: cand.ref, type: 'loose_fallback' };
                    }
                  }

                  return null;
                };

                // Try fuzzy match by athlete_id first
                if (!docRef && athleteId) {
                  // activityDoc is created later (after match detection) so use the
                  // activityDocBase here which already contains the canonical fields
                  // for the incoming activity.
                  const found = await findFuzzyMatch({ byAthleteId: athleteId, distanceVal: distance, movingTimeVal: moving_time, startDateVal: start_date, nameVal: activityDocBase.name, elevationVal: activityDocBase.elevation_gain, elapsedVal: activityDocBase.elapsed_time, stravaIdVal: activityDocBase.strava_id, typeVal: act.type, sportTypeVal: act.sport_type, workoutTypeVal: act.workout_type, athleteResourceState: act.athlete && act.athlete.resource_state });
                  // Treat high-confidence fuzzy matches as definitive so we update instead of creating duplicate docs.
                  if (found && (found.type === 'strava_id' || found.type === 'start_date_strict' || found.type === 'start_date_loose' || found.type === 'strict_numeric_full' || found.type === 'strict_numeric' || found.type === 'athlete_name_numeric_match' || found.type === 'athlete_name_numeric_match')) { docRef = found.ref; docMatchType = found.type; }
                }

                // Then try fuzzy match by athlete_name
                if (!docRef && athleteName) {
                  const found2 = await findFuzzyMatch({ byAthleteName: athleteName, distanceVal: distance, movingTimeVal: moving_time, startDateVal: start_date, nameVal: activityDocBase.name, elevationVal: activityDocBase.elevation_gain, elapsedVal: activityDocBase.elapsed_time, stravaIdVal: activityDocBase.strava_id, typeVal: act.type, sportTypeVal: act.sport_type, workoutTypeVal: act.workout_type, athleteResourceState: act.athlete && act.athlete.resource_state });
                  // Accept highly confident fuzzy matches found by athlete name search as well
                  if (found2 && (found2.type === 'strava_id' || found2.type === 'start_date_strict' || found2.type === 'start_date_loose' || found2.type === 'strict_numeric_full' || found2.type === 'strict_numeric' || found2.type === 'athlete_name_numeric_match')) { docRef = found2.ref; docMatchType = found2.type; }
                }
              } catch (qErr) {
                console.warn('Error querying raw_activities for existing doc; will fallback to generated id', qErr && qErr.message || qErr);
              }

              // decide on final doc id (use existing docRef id or the deterministic uniqueId)
              const finalDocId = docRef ? docRef.id : uniqueId;

              // Build final activity doc with explicit `id` to match normalized shape
              const activityDoc = Object.assign({}, activityDocBase, { id: finalDocId });

              // Canonicalize payload to the same keys/shape we use in raw_activities.pruned_normalized.json
              const canonicalKeys = [
                'id',
                'distance',
                'athlete_id',
                'source',
                'elevation_gain',
                'type',
                'workout_type',
                'elapsed_time',
                'name',
                'athlete_name',
                'moving_time',
                'sport_type',
                'fetched_at',
                'updated_at'
              ];

              const canonicalPayload = canonicalKeys.reduce((acc, k) => {
                // Map fields from activityDoc or fallback to null / reasonable defaults
                if (k === 'id') acc[k] = activityDoc.id;
                else if (k === 'distance') acc[k] = Number(activityDoc.distance || 0);
                else if (k === 'athlete_id') acc[k] = typeof activityDoc.athlete_id !== 'undefined' ? activityDoc.athlete_id : null;
                else if (k === 'source') acc[k] = activityDoc.source || 'strava_api';
                else if (k === 'elevation_gain') acc[k] = Number(activityDoc.elevation_gain || activityDoc.total_elevation_gain || 0);
                else if (k === 'type') acc[k] = activityDoc.type || 'Run';
                else if (k === 'workout_type') acc[k] = typeof activityDoc.workout_type !== 'undefined' ? activityDoc.workout_type : null;
                else if (k === 'elapsed_time') acc[k] = typeof activityDoc.elapsed_time !== 'undefined' && activityDoc.elapsed_time !== null ? Number(activityDoc.elapsed_time) : null;
                else if (k === 'name') acc[k] = activityDoc.name || 'Activity';
                else if (k === 'athlete_name') acc[k] = activityDoc.athlete_name || '';
                else if (k === 'moving_time') acc[k] = typeof activityDoc.moving_time !== 'undefined' && activityDoc.moving_time !== null ? Number(activityDoc.moving_time) : 0;
                else if (k === 'sport_type') acc[k] = typeof activityDoc.sport_type !== 'undefined' ? activityDoc.sport_type : null;
                else if (k === 'fetched_at') acc[k] = activityDoc.fetched_at || Date.now();
                else if (k === 'updated_at') acc[k] = activityDoc.updated_at || Date.now();
                else acc[k] = activityDoc[k] || null;
                return acc;
              }, {});

              if (docRef) {
                // Only update existing document when the match was definitive (strava_id or exact start_date).
                // For fuzzy matches we avoid overwriting/merging and instead create a new doc to preserve duplicates.
                batch.set(docRef, canonicalPayload, { merge: true });
              } else {
                batch.set(db.collection('raw_activities').doc(uniqueId), canonicalPayload, { merge: true });
              }

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

        // Load ALL activities from raw_activities for aggregation (preserves full history)
        // This ensures we aggregate all stored activities, not just recent Strava API fetches
        let allStoredActivities = [];
        try {
          const allSnaps = await db.collection('raw_activities').get();
          allStoredActivities = allSnaps.docs.map(d => d.data());
          console.log(`Loaded ${allStoredActivities.length} total activities from raw_activities for aggregation`);
        } catch (loadErr) {
          console.error('Failed to load raw_activities for aggregation:', loadErr);
          console.error('Error details:', loadErr.message || loadErr);
        }

        // Convert all stored activities to aggregation format
        const allActivities = allStoredActivities.map(act => {
          const athleteName = act.athlete_name || '';
          
          return {
            athlete: {
              id: act.athlete_id || null,
              firstname: athleteName.split(' ')[0] || '',
              lastname: athleteName.split(' ').slice(1).join(' ') || ''
            },
            distance: act.distance || 0,
            moving_time: act.moving_time || 0,
            start_date: act.start_date,
            type: act.type || 'Run',
            name: act.name || 'Activity',
            total_elevation_gain: act.elevation_gain || act.total_elevation_gain || 0
          };
        });
        console.log(`Aggregating ${allActivities.length} total activities (all sources, full history)`);

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
          // Removed date filtering - aggregate ALL activities regardless of age
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

        // Store last aggregation timestamp in admin/strava doc
        try {
          await db.collection('admin').doc('strava').set({
            last_aggregation_at: Date.now()
          }, { merge: true });
        } catch (e) {
          console.warn('Failed to update last_aggregation_at', e.message || e);
        }

        // ALSO: write a leaderboard snapshot document (single doc) for fast reads by the frontend
        try {
          // Read current per-athlete activities docs to assemble rows
          const actSnaps = await db.collection('activities').get();
          const rows = actSnaps.docs.map(d => ({ id: d.id, ...d.data() }));
          const snapshot = {
            metadata: { aggregated_at: Date.now(), membersCount: members.length, activitiesCount: acts.length },
            rows
          };
          // latest snapshot
          await db.collection('leaderboard_snapshots').doc('latest').set(snapshot, { merge: false });
          // timestamped archive
          await db.collection('leaderboard_snapshots').doc(`snap_${Date.now()}`).set(snapshot, { merge: false });
        } catch (snapErr) {
          console.warn('Failed to write leaderboard snapshot', snapErr && snapErr.message || snapErr);
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

    // Store last aggregation timestamp in admin/strava doc
    try {
      await db.collection('admin').doc('strava').set({
        last_aggregation_at: Date.now()
      }, { merge: true });
    } catch (e) {
      console.warn('Failed to update last_aggregation_at', e.message || e);
    }

    // ALSO: write a leaderboard snapshot for fast frontend reads (activities collection contains per-athlete docs)
    try {
      const actSnaps = await db.collection('activities').get();
      const rows = actSnaps.docs.map(d => ({ id: d.id, ...d.data() }));
      const snapshot = { metadata: { aggregated_at: Date.now(), resultsCount: rows.length }, rows };
      await db.collection('leaderboard_snapshots').doc('latest').set(snapshot, { merge: false });
      await db.collection('leaderboard_snapshots').doc(`snap_${Date.now()}`).set(snapshot, { merge: false });
    } catch (snapErr) {
      console.warn('Failed to write leaderboard snapshot', snapErr && snapErr.message || snapErr);
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
    // Get last aggregation timestamp from admin/strava doc
    let lastAggregationAt = null;
    try {
      const adminDoc = await db.collection('admin').doc('strava').get();
      if (adminDoc.exists) {
        const data = adminDoc.data();
        lastAggregationAt = data && data.last_aggregation_at ? data.last_aggregation_at : null;
      }
    } catch (e) {
      console.warn('Failed to read last_aggregation_at', e.message || e);
    }

    // Prefer reading a single pre-computed leaderboard snapshot for fastest response.
    // The aggregation process writes a single snapshot to `leaderboard_snapshots/latest`.
    try {
      const latestSnapDoc = await db.collection('leaderboard_snapshots').doc('latest').get();
      if (latestSnapDoc.exists) {
        const snap = latestSnapDoc.data() || {};
        const rows = Array.isArray(snap.rows) ? snap.rows : [];
        const lastAgg = snap.metadata && snap.metadata.aggregated_at ? snap.metadata.aggregated_at : lastAggregationAt;
        return res.json({ rows: rows.map(r => ({ id: r.id, athlete: r.athlete || null, summary: r.summary ? { ...r.summary, updated_at: r.summary.updated_at || lastAgg || Date.now() } : { distance: 0, count: 0, longest: 0, avg_pace: null, elev_gain: 0, updated_at: lastAgg || Date.now() } })), last_aggregation_at: lastAgg });
      }
    } catch (e) {
      console.warn('Failed to read leaderboard snapshot', e && e.message || e);
    }

    // Fallback: Read cached, pre-computed leaderboard summaries from `activities` collection.
    // This is slower than snapshot but keeps the leaderboard UI snappy when snapshot missing.
    const snaps = await db.collection('activities').get();
    const rows = snaps.docs.map(d => ({ id: d.id, ...d.data() }));
    // Build two lookup maps:
    //  - athleteMetadataById: keyed by the Firestore doc id (e.g. 'name:Arsel V.' or numeric id)
    //  - athleteMetadata: keyed by normalized canonical names (legacy fallback)
    // Load athlete summary docs used to enrich cached activities (nickname / goals)
    const athleteSnaps = await db.collection('summary_athletes').get();

    const normalizeNameKey = (s) => {
      if (!s) return '';
      try {
        return s.toString().trim().replace(/^0+/, '').replace(/\./g, '').replace(/[^\w\s]/g, '').replace(/\s+/g, ' ').toLowerCase();
      } catch (e) { return String(s || '').toLowerCase().trim(); }
    };

    const athleteMetadataById = new Map();
    const athleteMetadataByIdLower = new Map();
    const athleteMetadata = new Map();
    athleteSnaps.docs.forEach(doc => {
      const data = doc.data() || {};
      const rawNameField = (data.name || '').toString().trim();
      const fromId = String(doc.id || '').startsWith('name:') ? String(doc.id).replace(/^name:/, '').trim() : null;
      // Determine canonical name: prefer explicit name field, else id-derived name, else username-like
      const canonical = rawNameField || fromId || (data.username || '') || '';
      const cleanCanonical = canonical.replace(/^0+(?=[A-Za-z])/, '').trim();
      // Determine nickname: prefer stored nickname; if missing but doc id has a fuller name, use that as nickname
      const storedNick = (data.nickname || '').toString().trim();
      const cleanStoredNick = storedNick ? storedNick.replace(/^0+(?=[A-Za-z])/, '').trim() : '';
      let nicknameToUse = cleanStoredNick || null;
      if (!nicknameToUse && fromId) {
        // if id-derived name is longer/more-detailed than name field, promote it to nickname
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

      // Store by doc id for direct lookup (preferred)
      const docId = String(doc.id || '');
      athleteMetadataById.set(docId, meta);
      athleteMetadataByIdLower.set(docId.toLowerCase(), meta);

      // Also keep normalized-name fallback mapping for compatibility
      const k1 = normalizeNameKey(cleanCanonical);
      if (k1 && !athleteMetadata.has(k1)) athleteMetadata.set(k1, meta);
      if (fromId) {
        const k2 = normalizeNameKey(fromId.replace(/^0+(?=[A-Za-z])/, '').trim());
        if (k2 && !athleteMetadata.has(k2)) athleteMetadata.set(k2, meta);
      }
      if (cleanCanonical) {
        const first = (cleanCanonical.split(' ')[0] || '').trim();
        const k3 = normalizeNameKey(first);
        if (k3 && !athleteMetadata.has(k3)) athleteMetadata.set(k3, meta);
      }
    });
    
    // Build response rows from the cached `activities` docs we just read.
    // Each doc is expected to contain at least { athlete, summary } and an id matching
    // the summary_athletes doc id when present. Keep last_aggregation_at in the response
    // so the frontend can show when the leaderboard was last synced.
    const findMetaForSummary = (summary, normalizedKey) => {
      // 1) If athlete object has an id, prefer that doc id
      if (summary && summary.athlete && (summary.athlete.id || summary.athlete.athlete_id)) {
        const aid = String(summary.athlete.id || summary.athlete.athlete_id);
        if (athleteMetadataById.has(aid)) return athleteMetadataById.get(aid);
        if (athleteMetadataByIdLower.has(aid.toLowerCase())) return athleteMetadataByIdLower.get(aid.toLowerCase());
      }

      // 2) Try doc id variants using the aggregated summary.name (which contains the human name)
      const rawSummaryName = (summary && summary.name) ? String(summary.name).trim() : '';
      if (rawSummaryName) {
        const candidates = [];
        const cleaned = rawSummaryName.replace(/^0+(?=[A-Za-z])/, '').trim();
        candidates.push(`name:${cleaned}`);
        candidates.push(`name:${cleaned.replace(/\./g, '')}`);
        candidates.push(`name:${cleaned.replace(/[^\w\s]/g, '')}`);
        candidates.push(`name:${cleaned.replace(/[^\w\s]/g, '').replace(/\s+/g, ' ').trim()}`);
        for (const c of candidates) {
          if (athleteMetadataById.has(c)) return athleteMetadataById.get(c);
          const low = c.toLowerCase();
          if (athleteMetadataByIdLower.has(low)) return athleteMetadataByIdLower.get(low);
        }
      }

      // 3) Fallback to normalized-name lookup (legacy behavior)
      if (normalizedKey && athleteMetadata.has(normalizedKey)) return athleteMetadata.get(normalizedKey);
      return null;
    };

    // Convert docs to the same shape the frontend expects: { id, athlete, summary }
    // Ensure we supply a stable `updated_at` for each summary (fallback to lastAggregationAt)
    const normalizedRows = rows.map(r => ({
      id: r.id,
      athlete: r.athlete || null,
      summary: r.summary ? { ...r.summary, updated_at: r.summary.updated_at || lastAggregationAt || Date.now() } : { distance: 0, count: 0, longest: 0, avg_pace: null, elev_gain: 0, updated_at: lastAggregationAt || Date.now() }
    }));

    return res.json({ rows: normalizedRows, last_aggregation_at: lastAggregationAt });
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

    // Build a duplicate key based on *exact*-match fields for immediate duplicate removal:
    // we want to remove records that are exact duplicates for distance, moving_time,
    // elapsed_time, and total_elevation_gain (optionally match name and athlete identity).
    // This is intentionally stricter than the previous fuzzy grouping — Cleanup should
    // remove immediate duplicates only (no fuzzy merging).
    const buildDupKey = (data) => {
      if (!data) return null;
      // Athlete identity: prefer structured athlete object when available, else parse athlete_name
      let fn = '';
      let ln = '';
      let athleteResourceState = '';
      if (data.athlete && typeof data.athlete === 'object') {
        fn = (data.athlete.firstname || data.athlete.first_name || '').toString().trim();
        ln = (data.athlete.lastname || data.athlete.last_name || '').toString().trim();
        athleteResourceState = (data.athlete.resource_state !== undefined && data.athlete.resource_state !== null) ? String(data.athlete.resource_state) : '';
      } else if (data.athlete_name) {
        const parts = data.athlete_name.toString().trim().split(/\s+/);
        fn = parts[0] || '';
        ln = parts.slice(1).join(' ') || '';
      }

      const activityName = (data.name || '').toString().trim();
      const athleteId = data.athlete && (data.athlete.id || data.athlete.id_str || data.athlete_id) ? String(data.athlete.id || data.athlete.id_str || data.athlete_id) : '';

      // Normalize numeric fields to avoid superficial differences in formatting
      // from preventing exact-duplicate detection. Distance & elevation are kept
      // to one decimal place; times are integers (seconds).
      const rawDistanceMeters = Number(data.distance || data.distance_m || 0);
      const distanceKey = (Number.isFinite(rawDistanceMeters) ? (Math.round(rawDistanceMeters * 10) / 10).toFixed(1) : '0.0');
      const moving_time = Math.round(Number(data.moving_time || 0));
      const elapsed_time = Math.round(Number(data.elapsed_time || 0));
      const elevRaw = Number(data.total_elevation_gain || data.elevation_gain || data.elev_total || 0);
      const elev = Number.isFinite(elevRaw) ? (Math.round(elevRaw * 10) / 10).toFixed(1) : '0.0';
      const type = (data.type || '').toString().trim();
      const sport_type = (data.sport_type || '').toString().trim();
      const workout_type = (data.workout_type !== undefined && data.workout_type !== null) ? String(data.workout_type) : '';

      // Build a strict key representing immediate-duplicate attributes. Prefer
      // the normalized athlete name for grouping when available so docs that have
      // athlete_id in one record and only athlete_name in another still match.
      const athleteNameNormalized = ((data.athlete_name || ((fn + ' ' + ln).trim())) || '').toString().trim();
      const parts = [
        athleteNameNormalized ? athleteNameNormalized : String(athleteId || ''),
        activityName,
        String(distanceKey),
        String(moving_time),
        String(elapsed_time),
        String(elev)
      ];

      // Lowercase and join into a stable key
      return parts.map(s => (s || '').toString().toLowerCase().replace(/\s+/g, ' ').trim()).join('|');
    };

    // Group docs by duplicate key first (two-pass). This ensures we only delete when a key has >1 docs.
    const groups = new Map();
    snaps.docs.forEach(doc => {
      const data = doc.data();
      const key = buildDupKey(data);
      if (!key) return; // skip unidentifiable
      if (!groups.has(key)) groups.set(key, []);
      groups.get(key).push({ id: doc.id, data });
    });

    const toDelete = [];
    const previewGroups = [];

    // Helper to compute timestamp for ordering (prefer older = smaller value)
    const tsOf = (d) => Number(d && (d.created_at || d.fetched_at || d.updated_at) || Number.MAX_SAFE_INTEGER);

    for (const [key, list] of groups.entries()) {
      if (!Array.isArray(list) || list.length <= 1) {
        // No duplicates for this key -> do not delete anything
        continue;
      }

      // Choose the doc to keep. If there are any timestamp fields we prefer the oldest
      // (smallest ts). When no timestamps are present we still want to aggressively
      // remove immediate duplicates — in that case select a preferred candidate to keep
      // using a safe heuristic: prefer records with a `strava_id`, then prefer source
      // 'strava_api', otherwise fall back to the first item.
      let keep = list[0];
      const hasAnyTs = list.some(item => tsOf(item.data) !== Number.MAX_SAFE_INTEGER);
      if (hasAnyTs) {
        for (const item of list) {
          if (tsOf(item.data) < tsOf(keep.data)) keep = item;
        }
      } else {
        // No timestamps — pick the best candidate to keep
        const withStravaId = list.find(item => item.data && item.data.strava_id);
        if (withStravaId) keep = withStravaId;
        else {
          const withStravaSource = list.find(item => item.data && (item.data.source === 'strava_api' || (item.data.source && String(item.data.source).toLowerCase().includes('strava'))));
          if (withStravaSource) keep = withStravaSource;
          else keep = list[0];
        }
      }

      // Mark all others for deletion
      const deleteForThis = [];
      for (const item of list) {
        if (item.id !== keep.id) { toDelete.push(item.id); deleteForThis.push(item.id); }
      }

      // Keep a simple preview item for dry-run reporting
      previewGroups.push({ key, keep: keep.id, deletes: deleteForThis, count: list.length });
    }

    // If dry-run requested, return preview details and DO NOT delete
    const dryRun = (req.query && (req.query.dry_run === '1' || req.query.dry_run === 'true')) || (req.body && req.body.dry_run);
    if (dryRun) {
      return res.json({ ok: true, deleted: toDelete.length, kept: snaps.size - toDelete.length, dry_run: true, preview: previewGroups });
    }

    // Perform deletions in batches of 500 (if any)
    let deleted = 0;
    if (toDelete.length === 0) {
      console.log('Cleanup complete. No duplicates found to delete.');
      return res.json({ ok: true, deleted: 0, kept: snaps.size });
    }

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

// Admin: Restore backed-up activities from raw_activities_backup collection
app.post('/admin/restore-backup', async (req, res) => {
  if (!db) return res.status(500).json({ error: 'firestore not initialized' });
  try {
    console.log('Starting backup restoration...');
    
    // Check if backup collection exists and has data
    const backupSnap = await db.collection('raw_activities_backup').get();
    
    if (backupSnap.empty) {
      console.log('No backup data found');
      return res.json({ ok: true, restored: 0, message: 'No backup data found to restore' });
    }
    
    console.log(`Found ${backupSnap.size} backed up records`);
    
    // Restore in batches
    let restored = 0;
    const batchSize = 500;
    const docs = backupSnap.docs;
    
    for (let i = 0; i < docs.length; i += batchSize) {
      const batch = db.batch();
      const slice = docs.slice(i, i + batchSize);
      
      slice.forEach(doc => {
        const data = doc.data();
        // Remove backup-specific fields
        delete data.backed_up_at;
        delete data.backup_reason;
        const originalId = data.original_id || doc.id;
        delete data.original_id;
        
        // Restore to raw_activities with original ID
        const restoreRef = db.collection('raw_activities').doc(originalId);
        batch.set(restoreRef, data, { merge: true });
      });
      
      await batch.commit();
      restored += slice.length;
      console.log(`Restored ${restored}/${docs.length} records`);
    }
    
    console.log(`Restoration complete. Restored ${restored} activities`);
    res.json({ ok: true, restored, message: `Successfully restored ${restored} activities from backup` });
  } catch (e) {
    console.error('Restore failed', e);
    res.status(500).json({ error: e.message || String(e) });
  }
});

// Admin: Normalize stored raw activities so key fields are always present
// This helps fix inconsistent documents where some fields (distance, moving_time,
// elapsed_time, elevation_gain, athlete_name, name) are missing.
app.post('/admin/normalize-raw-activities', async (req, res) => {
  if (!db) return res.status(500).json({ error: 'firestore not initialized' });
  const dryRun = (req.query && (req.query.dry_run === '1' || req.query.dry_run === 'true')) || (req.body && req.body.dry_run);
  try {
    console.log('Starting normalize raw_activities (dryRun=', !!dryRun, ')');
    const snap = await db.collection('raw_activities').get();
    if (!snap || snap.empty) return res.json({ ok: true, normalized: 0, total: 0 });

    const updates = [];
    const backups = [];
    snap.docs.forEach(d => {
      const data = d.data() || {};
      const norm = {};
      // athlete_name
      if (!data.athlete_name && data.athlete && typeof data.athlete === 'object') {
        const fn = data.athlete.firstname || data.athlete.first_name || '';
        const ln = data.athlete.lastname || data.athlete.last_name || '';
        const full = (fn + ' ' + ln).trim();
        if (full) norm.athlete_name = full;
      }
      // athlete_id
      if (!data.athlete_id && data.athlete && (data.athlete.id || data.athlete.id_str || data.athlete_id)) {
        norm.athlete_id = String(data.athlete.id || data.athlete.id_str || data.athlete_id);
      }

      // distance - prefer `distance` or `distance_m` if present, else fallback to 0
      const dCandidates = [data.distance, data.distance_m, data.distanceMeters, data.distance_meters];
      const dFound = dCandidates.find(v => typeof v !== 'undefined' && v !== null && v !== '');
      if (typeof data.distance === 'undefined' && typeof dFound !== 'undefined') {
        norm.distance = Number(dFound || 0);
      }
      if (typeof data.distance === 'undefined' && typeof dFound === 'undefined') {
        // Ensure distance exists (0 if unknown) so duplicates detection always has it
        norm.distance = 0;
      }

      // moving_time
      if (typeof data.moving_time === 'undefined') {
        const mt = typeof data.moving_time !== 'undefined' ? data.moving_time : (typeof data.movingTime !== 'undefined' ? data.movingTime : null);
        if (mt !== null && typeof mt !== 'undefined') norm.moving_time = Math.round(Number(mt || 0));
        else norm.moving_time = 0;
      }

      // elapsed_time
      if (typeof data.elapsed_time === 'undefined') {
        if (typeof data.elapsed_time !== 'undefined') norm.elapsed_time = Number(data.elapsed_time || 0);
        else if (typeof data.moving_time !== 'undefined') norm.elapsed_time = Math.round(Number(data.moving_time || 0));
        else norm.elapsed_time = 0;
      }

      // elevation_gain
      if (typeof data.elevation_gain === 'undefined') {
        const egCandidates = [data.total_elevation_gain, data.elev_total, data.elevation];
        const eg = egCandidates.find(v => typeof v !== 'undefined' && v !== null);
        norm.elevation_gain = Number(eg || 0);
      }

      // name
      if (!data.name) norm.name = data.type ? `${data.type} Activity` : 'Activity';

      // type defaults
      if (!data.type) norm.type = 'Run';

      // sport_type/workout_type for completeness
      if (typeof data.sport_type === 'undefined' && data.type) norm.sport_type = data.type;
      if (typeof data.workout_type === 'undefined') norm.workout_type = data.workout_type || null;

      // timestamps
      if (!data.updated_at) norm.updated_at = Date.now();

      // If there are keys to set, capture update. Also ensure the `id` field
      // exists inside each document to keep shape consistent.
      if (Object.keys(norm).length > 0) {
        if (typeof norm.id === 'undefined') norm.id = d.id;
        updates.push({ id: d.id, set: norm });
      }
    });

    if (dryRun) return res.json({ ok: true, total: snap.size, candidates: updates.slice(0, 200), count: updates.length });

    // Perform batched updates
    let done = 0;
    const batchSize = 500;
    for (let i = 0; i < updates.length; i += batchSize) {
      const batch = db.batch();
      const slice = updates.slice(i, i + batchSize);
      slice.forEach(u => batch.set(db.collection('raw_activities').doc(u.id), u.set, { merge: true }));
      await batch.commit();
      done += slice.length;
    }

    res.json({ ok: true, normalized: done, total: snap.size });
  } catch (e) {
    console.error('Normalization failed', e);
    res.status(500).json({ error: e.message || String(e) });
  }
});

// Admin: Prune raw_activities down to one canonical record per group.
// This is more aggressive than 'cleanup-raw-activities' and groups items by
// athlete + date + rounded distance. Use dry_run=true to preview. Defaults
// to rounding to the nearest 3.5 meters (which matches the 'canonical' snapshot).
app.post('/admin/prune-raw-to-canonical', async (req, res) => {
  if (!db) return res.status(500).json({ error: 'firestore not initialized' });
  try {
    const dryRun = (req.query && (req.query.dry_run === '1' || req.query.dry_run === 'true')) || (req.body && req.body.dry_run);
    const roundUnit = Number(req.body && req.body.round_unit) || Number(req.query && req.query.round_unit) || 3.5;
    const dateOnly = req.body && typeof req.body.date_only !== 'undefined' ? !!req.body.date_only : true;
    const createBackup = req.body && typeof req.body.create_backup !== 'undefined' ? !!req.body.create_backup : true;

    console.log('Starting prune-raw-to-canonical (dryRun=', !!dryRun, ', roundUnit=', roundUnit, ', dateOnly=', dateOnly, ')');

    const snap = await db.collection('raw_activities').get();
    if (!snap || snap.empty) return res.json({ ok: true, deleted: 0, kept: 0, total: 0 });

    // Normalize and build grouping key (athlete id/name || date-only start_date || rounded distance)
    const normalizeName = (d) => {
      if (!d) return '';
      if (d.athlete && typeof d.athlete === 'object') {
        const fn = d.athlete.firstname || d.athlete.first_name || '';
        const ln = d.athlete.lastname || d.athlete.last_name || '';
        const full = (fn + ' ' + ln).trim();
        if (full) return full.toString().trim();
      }
      if (d.athlete_name) return d.athlete_name.toString().trim();
      return d.athlete_id ? String(d.athlete_id || '') : '';
    };

    const datePart = (s) => {
      if (!s) return '';
      try { return dateOnly ? String(s).slice(0, 10) : String(s); } catch (e) { return String(s); }
    };

    const roundNearest = (n, unit) => {
      if (!Number.isFinite(Number(n))) return 0;
      return Math.round(Number(n) / unit) * unit;
    };

    const groups = new Map();
    snap.docs.forEach(doc => {
      const d = Object.assign({}, doc.data());
      // make sure distance exists in some shape
      const rawDistance = Number(d.distance || d.distance_m || d.distanceMeters || 0);
      const rounded = roundNearest(rawDistance, roundUnit);
      const name = (normalizeName(d) || '').toLowerCase();
      const keyParts = [name || '', datePart(d.start_date || d.start_date_local || d.start_date_local_time || ''), String(rounded)];
      const key = keyParts.map(s => (s || '').toString().toLowerCase().replace(/\s+/g,' ').trim()).join('|');
      if (!key) return; // skip unknowable rows
      if (!groups.has(key)) groups.set(key, []);
      groups.get(key).push({ id: doc.id, data: d });
    });

    // Decide which docs to keep (prefer strava_id, then strava_api source, else earliest updated_at)
    const toDelete = [];
    const preview = [];
    let groupCount = 0;
    for (const [key, list] of groups.entries()) {
      groupCount++;
      if (!Array.isArray(list) || list.length <= 1) continue;
      // choose keep
      let keep = list[0];
      const withStravaId = list.find(it => it.data && (it.data.strava_id || it.data.stravaId));
      if (withStravaId) keep = withStravaId;
      else {
        const withStravaSource = list.find(it => it.data && it.data.source && String(it.data.source).toLowerCase().includes('strava'));
        if (withStravaSource) keep = withStravaSource;
        else {
          // earliest updated_at or created_at
          let earliest = list[0];
          for (const it of list) {
            const ts = Number(it.data && (it.data.updated_at || it.data.fetched_at || it.data.created_at) || Number.MAX_SAFE_INTEGER);
            const bestTs = Number(earliest.data && (earliest.data.updated_at || earliest.data.fetched_at || earliest.data.created_at) || Number.MAX_SAFE_INTEGER);
            if (ts < bestTs) earliest = it;
          }
          keep = earliest;
        }
      }

      const deletes = list.filter(x => x.id !== keep.id).map(x => x.id);
      if (deletes.length) {
        toDelete.push(...deletes);
        preview.push({ key, keep: keep.id, deletes, count: list.length });
      }
    }

    // If dry run, return preview
    if (dryRun) {
      const kept = snap.size - toDelete.length;
      return res.json({ ok: true, total: snap.size, groups: groups.size, kept, deleted: toDelete.length, dry_run: true, preview: preview.slice(0, 200) });
    }

    // Non-dry run: backup (if requested) and delete
    const batchSize = 500;
    let deleted = 0;

    if (createBackup && toDelete.length > 0) {
      // Copy each doc to raw_activities_backup before deletion (preserve original id by adding suffix if exists)
      for (let i = 0; i < toDelete.length; i += batchSize) {
        const batch = db.batch();
        const slice = toDelete.slice(i, i + batchSize);
        // read documents to back them up
        const docs = await Promise.all(slice.map(id => db.collection('raw_activities').doc(id).get()));
        docs.forEach((docSnap) => {
          if (!docSnap || !docSnap.exists) return;
          const d = docSnap.data();
          const backupId = `${docSnap.id}_${Date.now()}`;
          const ref = db.collection('raw_activities_backup').doc(backupId);
          const payload = Object.assign({}, d, { original_id: docSnap.id, backed_up_at: Date.now(), backup_reason: 'prune_to_canonical' });
          batch.set(ref, payload);
        });
        await batch.commit();
      }
    }

    // Now perform deletes
    for (let i = 0; i < toDelete.length; i += batchSize) {
      const batch = db.batch();
      const slice = toDelete.slice(i, i + batchSize);
      slice.forEach(id => batch.delete(db.collection('raw_activities').doc(id)));
      await batch.commit();
      deleted += slice.length;
    }

    const kept = snap.size - deleted;
    console.log(`Prune complete. Deleted ${deleted}, kept ${kept} (groups=${groups.size}, total=${snap.size})`);
    return res.json({ ok: true, deleted, kept, total: snap.size, groups: groups.size, preview: preview.slice(0,200) });
  } catch (e) {
    console.error('Prune failed', e);
    res.status(500).json({ error: e.message || String(e) });
  }
});

// Admin: Remove all raw_activities documents that exactly match a particular
// structural signature discovered in data (the '192' group). Use dry_run=1 to preview.
app.post('/admin/prune-raw-activities-192', async (req, res) => {
  if (!db) return res.status(500).json({ error: 'firestore not initialized' });
  try {
    const dryRun = (req.query && (req.query.dry_run === '1' || req.query.dry_run === 'true')) || (req.body && req.body.dry_run);
    const createBackup = req.body && typeof req.body.create_backup !== 'undefined' ? !!req.body.create_backup : true;

    // Signature to target (sorted keys)
    const requiredKeys = ['athlete_id','athlete_name','distance','elapsed_time','elevation_gain','fetched_at','id','moving_time','name','source','sport_type','type','updated_at','workout_type'];

    console.log('Starting prune-raw-activities-192 (dryRun=', !!dryRun, ', createBackup=', createBackup, ')');

    const snap = await db.collection('raw_activities').get();
    if (!snap || snap.empty) return res.json({ ok: true, deleted: 0, kept: 0, total: 0 });

    const matches = [];
    // Find docs that contain the required key set. Use a tolerant check so
    // minor variations (ordering, extra keys, extra fields) won't prevent
    // detection. We allow documents that have at least (requiredKeys.length - 1)
    // of the required fields to match — this helps match near-identical docs
    // stored in Firestore where some fields may be null or occasionally missing.
    snap.docs.forEach(doc => {
      const data = doc.data() || {};
      const keys = Object.keys(data).map(k => String(k));
      const keySet = new Set(keys);
      const presentCount = requiredKeys.reduce((c, k) => c + (keySet.has(k) ? 1 : 0), 0);
      const matchThreshold = Math.max(0, requiredKeys.length - 1); // allow one missing
      if (presentCount >= matchThreshold) matches.push({ id: doc.id, data, presentCount });
    });

    // Provide additional visibility in dry run: how many keys each match has
    if (dryRun) return res.json({ ok: true, total: snap.size, matches: matches.length, preview: matches.slice(0, 200).map(m => ({ id: m.id, presentCount: m.presentCount })), dry_run: true });

    if (matches.length === 0) return res.json({ ok: true, deleted: 0, kept: snap.size, total: snap.size });

    const toDelete = matches.map(m => m.id);
    const batchSize = 500;
    let deleted = 0;

    // backup before deleting if requested
    if (createBackup) {
      for (let i = 0; i < toDelete.length; i += batchSize) {
        const slice = toDelete.slice(i, i + batchSize);
        const docs = await Promise.all(slice.map(id => db.collection('raw_activities').doc(id).get()));
        const batch = db.batch();
        docs.forEach(docSnap => {
          if (!docSnap || !docSnap.exists) return;
          const payload = Object.assign({}, docSnap.data(), { original_id: docSnap.id, backed_up_at: Date.now(), backup_reason: 'prune_192_signature' });
          const backupId = `${docSnap.id}_${Date.now()}`;
          batch.set(db.collection('raw_activities_backup').doc(backupId), payload);
        });
        await batch.commit();
      }
    }

    // delete in batches
    for (let i = 0; i < toDelete.length; i += batchSize) {
      const batch = db.batch();
      const slice = toDelete.slice(i, i + batchSize);
      slice.forEach(id => batch.delete(db.collection('raw_activities').doc(id)));
      await batch.commit();
      deleted += slice.length;
    }

    const kept = snap.size - deleted;
    console.log(`prune-raw-activities-192 complete. Deleted ${deleted}, kept ${kept} (total=${snap.size})`);
    return res.json({ ok: true, deleted, kept, total: snap.size });
  } catch (e) {
    console.error('prune-raw-activities-192 failed', e);
    res.status(500).json({ error: e.message || String(e) });
  }
});

// Admin: Export raw_activities + raw_activities_backup as combined JSON
app.get('/admin/export-raw-activities', async (req, res) => {
  if (!db) return res.status(500).json({ error: 'firestore not initialized' });
  try {
    const rawSnap = await db.collection('raw_activities').get();
    const backupSnap = await db.collection('raw_activities_backup').get();

    const raw = rawSnap.docs.map(d => ({ id: d.id, ...d.data() }));
    const backup = backupSnap && !backupSnap.empty ? backupSnap.docs.map(d => ({ id: d.id, ...d.data() })) : [];

    const payload = { exported_at: Date.now(), raw_activities: raw, raw_activities_backup: backup };

    // Support download response for browser (attachment)
    const wantDownload = (req.query && (req.query.download === '1' || req.query.download === 'true')) || (req.body && req.body.download);
    if (wantDownload) {
      const filename = `aac_raw_activities_export_${new Date().toISOString().replace(/[:.]/g,'-')}.json`;
      res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
      res.setHeader('Content-Type', 'application/json');
      return res.send(JSON.stringify(payload));
    }

    return res.json(payload);
  } catch (e) {
    console.error('Export failed', e);
    return res.status(500).json({ error: e.message || String(e) });
  }
});

// Admin: Export the pruned + normalized JSON file from the repo root
// (raw_activities.pruned_normalized.json). This is useful when the team
// prepared an offline pruned/normalized payload they want to download.
app.get('/admin/export-pruned-normalized', async (req, res) => {
  const fs = require('fs');
  const path = require('path');
  if (!db) return res.status(500).json({ error: 'firestore not initialized' });
  try {
    // Try backend folder first, then repo root as a fallback (script earlier writes root file)
    let filePath = path.join(__dirname, '..', 'raw_activities.pruned_normalized.json');
    if (!fs.existsSync(filePath)) {
      // fallback to repo root
      const maybeRoot = path.join(__dirname, '..', '..', 'raw_activities.pruned_normalized.json');
      if (fs.existsSync(maybeRoot)) filePath = maybeRoot;
    }
    if (!fs.existsSync(filePath)) return res.status(404).json({ error: 'pruned_normalized file not found on server' });

    const wantDownload = (req.query && (req.query.download === '1' || req.query.download === 'true')) || (req.body && req.body.download);
    const content = fs.readFileSync(filePath, 'utf8');

    if (wantDownload) {
      const filename = `raw_activities.pruned_normalized_${new Date().toISOString().replace(/[:.]/g,'-')}.json`;
      res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
      res.setHeader('Content-Type', 'application/json');
      return res.send(content);
    }

    return res.json(JSON.parse(content));
  } catch (e) {
    console.error('export-pruned-normalized failed', e);
    return res.status(500).json({ error: e.message || String(e) });
  }
});

// Admin: Replace the entire raw_activities collection from an uploaded JSON payload.
// Body should contain either { raw_activities: [ ... ] } or { activities: [ ... ] } or be the array itself.
// Supports dry_run=true to preview actions, create_backup=true to back up current docs before replacing,
// and preserve_ids=true to use incoming records' `id` fields as Firestore doc ids (if present).
app.post('/admin/replace-raw-activities', async (req, res) => {
  if (!db) return res.status(500).json({ error: 'firestore not initialized' });
  try {
    const dryRun = (req.query && (req.query.dry_run === '1' || req.query.dry_run === 'true')) || (req.body && req.body.dry_run);
    const createBackup = req.body && typeof req.body.create_backup !== 'undefined' ? !!req.body.create_backup : true;
    const preserveIds = req.body && typeof req.body.preserve_ids !== 'undefined' ? !!req.body.preserve_ids : true;

    // Accept a few shapes for incoming payload
    const body = req.body || {};
    let incoming = body.raw_activities || body.activities || body;
    // If body is an object that isn't an array but has top-level exported_at and raw_activities, use that
    if (!Array.isArray(incoming) && body && Array.isArray(body.raw_activities)) incoming = body.raw_activities;

    if (!Array.isArray(incoming)) return res.status(400).json({ error: 'No activities array found in request body' });

    console.log(`replace-raw-activities received ${incoming.length} incoming docs (dryRun=${!!dryRun}, createBackup=${createBackup}, preserveIds=${preserveIds})`);

    // Fetch current raw_activities
    const snap = await db.collection('raw_activities').get();
    const existingCount = snap ? snap.size : 0;

    // Quick analysis for preview
    const incomingIds = new Set(incoming.filter(i => i && i.id).map(i => String(i.id)));
    const existingIds = new Set(snap.docs.map(d => String(d.id)));
    const overlapping = Array.from(incomingIds).filter(id => existingIds.has(id));

    if (dryRun) {
      // Sampling for preview
      const sampleExisting = snap.docs.slice(0, 5).map(d => d.id);
      const sampleIncoming = incoming.slice(0, 5).map(i => i && (i.id || '(no id)'));
      return res.json({ ok: true, dry_run: true, existingCount, incomingCount: incoming.length, overlappingCount: overlapping.length, overlappingSample: overlapping.slice(0, 10), sampleExisting, sampleIncoming });
    }

    // Actual replacement
    // 1) backup existing docs if requested
    const batchSize = 500;
    let backedUp = 0;
    if (createBackup && snap && !snap.empty) {
      const docs = snap.docs;
      for (let i = 0; i < docs.length; i += batchSize) {
        const batch = db.batch();
        const slice = docs.slice(i, i + batchSize);
        slice.forEach(docSnap => {
          const payload = Object.assign({}, docSnap.data(), { original_id: docSnap.id, backed_up_at: Date.now(), backup_reason: 'replace_raw_activities' });
          // use new backup id to avoid collisions
          const backupId = `${docSnap.id}_${Date.now()}`;
          const ref = db.collection('raw_activities_backup').doc(backupId);
          batch.set(ref, payload);
        });
        await batch.commit();
        backedUp += slice.length;
      }
    }

    // 2) delete existing raw_activities
    let deleted = 0;
    if (snap && !snap.empty) {
      const ids = snap.docs.map(d => d.id);
      for (let i = 0; i < ids.length; i += batchSize) {
        const batch = db.batch();
        const slice = ids.slice(i, i + batchSize);
        slice.forEach(id => batch.delete(db.collection('raw_activities').doc(id)));
        await batch.commit();
        deleted += slice.length;
      }
    }

    // 3) write incoming docs
    let inserted = 0;
    for (let i = 0; i < incoming.length; i += batchSize) {
      const batch = db.batch();
      const slice = incoming.slice(i, i + batchSize);
      slice.forEach(item => {
        // Choose doc ref: preserve id if requested and provided, else let firestore generate
        let ref;
        try {
          if (preserveIds && item && item.id) ref = db.collection('raw_activities').doc(String(item.id));
          else ref = db.collection('raw_activities').doc();
        } catch (e) {
          ref = db.collection('raw_activities').doc();
        }
        // Ensure a canonical payload is written (include id/fetched_at/updated_at and canonical fields)
        const now = Date.now();
        const payload = Object.assign({}, item || {});
        // Use the document id as `id` field
        payload.id = ref.id;
        // Ensure canonical keys exist
        payload.athlete_id = typeof payload.athlete_id !== 'undefined' ? payload.athlete_id : null;
        payload.athlete_name = payload.athlete_name || '';
        payload.distance = Number(payload.distance || 0);
        payload.moving_time = Number(payload.moving_time || payload.elapsed_time || 0);
        payload.elapsed_time = typeof payload.elapsed_time !== 'undefined' && payload.elapsed_time !== null ? Number(payload.elapsed_time) : Number(payload.moving_time || 0);
        payload.elevation_gain = Number(payload.elevation_gain || payload.total_elevation_gain || 0);
        payload.source = payload.source || 'uploaded';
        payload.workout_type = typeof payload.workout_type !== 'undefined' ? payload.workout_type : null;
        payload.sport_type = typeof payload.sport_type !== 'undefined' ? payload.sport_type : null;
        payload.type = payload.type || 'Run';
        payload.name = payload.name || 'Activity';
        payload.fetched_at = payload.fetched_at || now;
        payload.updated_at = payload.updated_at || now;

        batch.set(ref, payload);
        inserted++;
      });
      await batch.commit();
    }

    return res.json({ ok: true, replaced: true, existingCount, backedUp, deleted, inserted, incomingCount: incoming.length });
  } catch (e) {
    console.error('replace-raw-activities failed', e);
    return res.status(500).json({ error: e.message || String(e) });
  }
});

// Admin: Create a leaderboard snapshot from the current `activities` collection
// (useful when you want to promote current per-athlete summaries to the fast read snapshot).
app.post('/admin/make-activities-snapshot', async (req, res) => {
  if (!db) return res.status(500).json({ error: 'firestore not initialized' });
  try {
    // No body required; optional { archive: true } to write a timestamped archive as well
    const archive = req.body && typeof req.body.archive !== 'undefined' ? !!req.body.archive : true;

    const actSnaps = await db.collection('activities').get();
    if (!actSnaps || actSnaps.empty) return res.status(400).json({ ok: false, error: 'no activities documents available to snapshot' });

    const rows = actSnaps.docs.map(d => ({ id: d.id, ...d.data() }));
    const snapshot = { metadata: { created_at: Date.now(), rowsCount: rows.length }, rows };

    // write latest snapshot
    await db.collection('leaderboard_snapshots').doc('latest').set(snapshot, { merge: false });
    if (archive) await db.collection('leaderboard_snapshots').doc(`snap_${Date.now()}`).set(snapshot, { merge: false });

    return res.json({ ok: true, rowsCount: rows.length, archived: !!archive });
  } catch (e) {
    console.error('make-activities-snapshot failed', e && e.message || e);
    return res.status(500).json({ error: e.message || String(e) });
  }
});

// Admin: Dedupe raw_activities in-place (dry_run supported)
// groupingKey: athlete_name (case-insensitive) + rounded distance + rounded moving_time/elapsed_time
app.post('/admin/dedupe-raw-activities', async (req, res) => {
  if (!db) return res.status(500).json({ error: 'firestore not initialized' });
  try {
    const dryRun = (req.query && (req.query.dry_run === '1' || req.query.dry_run === 'true')) || (req.body && req.body.dry_run);
    const createBackup = req.body && typeof req.body.create_backup !== 'undefined' ? !!req.body.create_backup : true;

    const snaps = await db.collection('raw_activities').get();
    if (!snaps || snaps.empty) return res.json({ ok: true, message: 'no raw_activities documents' });

    const docs = snaps.docs.map(d => ({ id: d.id, data: d.data() }));

    const normalizeName = s => String(s || '').trim().toLowerCase();
    const round = n => Math.round(Number(n || 0));

    // group by fuzzy signature instead of exact rounding to avoid tiny numeric
    // differences (especially for sub-1km activities) creating separate groups.
    // We'll iterate docs and try to place each doc into an existing group when
    // athlete name matches and numeric fields are within tolerances.
    const groups = new Map();
    const fuzzTolerance = {
      absMeters: 10, // absolute meters tolerance
      rel: 0.02, // relative tolerance 2%
      mtSeconds: 10 // moving_time / elapsed_time tolerance (seconds)
    };

    const findMatchingGroupKey = (d) => {
      const name = normalizeName(d.athlete_name || d.athlete || '');
      const dist = Number(d.distance || 0);
      const mt = Math.round(Number(d.moving_time || d.elapsed_time || 0));

      for (const [k, arr] of groups.entries()) {
        // Each group key stores the representative doc as the first element
        const rep = arr[0] && arr[0].data ? arr[0].data : null;
        if (!rep) continue;
        const repName = normalizeName(rep.athlete_name || rep.athlete || '');
        if (repName !== name) continue;

        const repDist = Number(rep.distance || 0);
        const repMt = Math.round(Number(rep.moving_time || rep.elapsed_time || 0));

        const absd = Math.abs(repDist - dist);
        const reld = absd / Math.max(1, Math.max(Math.abs(repDist), Math.abs(dist)));
        const absmt = Math.abs(repMt - mt);

        const distOk = (absd <= fuzzTolerance.absMeters) || (reld <= fuzzTolerance.rel);
        const mtOk = absmt <= fuzzTolerance.mtSeconds;

        if (distOk && mtOk) return k;
      }
      return null;
    };

    // Build groups using fuzzy matching
    let groupIndex = 0;
    for (const doc of docs) {
      const matchKey = findMatchingGroupKey(doc.data || {});
      if (matchKey) {
        groups.get(matchKey).push(doc);
      } else {
        const newKey = `g${++groupIndex}`;
        groups.set(newKey, [doc]);
      }
    }

    // identify duplicates
    const dupGroups = Array.from(groups.entries()).filter(([k, arr]) => arr.length > 1);

    if (dryRun) {
      // return summary and some samples
      const sample = dupGroups.slice(0, 10).map(([k, arr]) => ({ key: k, count: arr.length, ids: arr.map(a => a.id).slice(0, 5) }));
      return res.json({ ok: true, dry_run: true, totalDocs: docs.length, duplicateGroupsCount: dupGroups.length, sample });
    }

    // Not a dry-run: perform backup + merge/delete
    const batchSize = 500;
    let backedUp = 0;
    let deleted = 0;
    let merged = 0;

    // create backups for docs we will remove (use per-doc backup id)
    for (const [key, arr] of dupGroups) {
      // pick canonical keeper: prefer doc with earliest fetched_at, else first
      arr.sort((a, b) => (Number(a.data && a.data.fetched_at || 0) - Number(b.data && b.data.fetched_at || 0)));
      const keeper = arr[0];
      const toRemove = arr.slice(1);

      // merge: we will set keeper to canonical shape and keep fetched_at as earliest
      const now = Date.now();
      const canonical = {
        id: keeper.id,
        distance: Number(keeper.data.distance || 0),
        athlete_id: typeof keeper.data.athlete_id !== 'undefined' ? keeper.data.athlete_id : null,
        source: keeper.data.source || 'strava_api',
        elevation_gain: Number(keeper.data.elevation_gain || keeper.data.total_elevation_gain || 0),
        type: keeper.data.type || 'Run',
        workout_type: typeof keeper.data.workout_type !== 'undefined' ? keeper.data.workout_type : null,
        elapsed_time: typeof keeper.data.elapsed_time !== 'undefined' && keeper.data.elapsed_time !== null ? Number(keeper.data.elapsed_time) : null,
        name: keeper.data.name || 'Activity',
        athlete_name: keeper.data.athlete_name || '',
        moving_time: typeof keeper.data.moving_time !== 'undefined' && keeper.data.moving_time !== null ? Number(keeper.data.moving_time) : 0,
        sport_type: typeof keeper.data.sport_type !== 'undefined' ? keeper.data.sport_type : null,
        fetched_at: Number(keeper.data.fetched_at || now),
        updated_at: now
      };

      // backup + delete others
      for (const doc of toRemove) {
        if (createBackup) {
          const backupId = `${doc.id}_${Date.now()}`;
          await db.collection('raw_activities_backup').doc(backupId).set(Object.assign({}, doc.data, { original_id: doc.id, backup_at: Date.now(), backup_reason: 'dedupe' }));
          backedUp++;
        }
        await db.collection('raw_activities').doc(doc.id).delete();
        deleted++;
      }

      // write canonical payload to keeper doc (merge false -> replace) so structure is canonical
      await db.collection('raw_activities').doc(keeper.id).set(Object.assign({}, canonical), { merge: false });
      merged++;
    }

    return res.json({ ok: true, totalDocs: docs.length, duplicateGroups: dupGroups.length, backedUp, deleted, merged, resultingTotal: docs.length - deleted });
  } catch (e) {
    console.error('dedupe-raw-activities failed', e);
    return res.status(500).json({ error: e.message || String(e) });
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
    // Create a pre-defined doc id so we can include `id` inside the stored document
    const newDocRef = db.collection('raw_activities').doc();
    const now = Date.now();
    const activity = {
      id: newDocRef.id,
      athlete_id: null,
      athlete_name: athlete_name,
      distance: Number(distance) || 0,
      moving_time: Number(moving_time) || 0,
      start_date: start_date || null,
      type: type || 'Run',
      name: name || 'Manual Activity',
      elevation_gain: Number(elevation_gain) || 0,
      workout_type: null,
      sport_type: null,
      elapsed_time: Number(moving_time) || 0,
      source: 'manual',
      fetched_at: now,
      updated_at: now
    };

    await newDocRef.set(activity, { merge: false });
    console.log(`Created manual activity ${newDocRef.id} for athlete ${athlete_name}`);

    res.json({ ok: true, activity });
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
    // Ensure the stored document has an explicit `id` field (keeps documents normalized)
    const docRef = db.collection('raw_activities').doc(id);
    const doc = await docRef.get();
    if (doc.exists) {
      const data = doc.data() || {};
      if (typeof data.id === 'undefined') {
        await docRef.set({ id }, { merge: true });
      }
    }
    
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
      // include canonical id + timestamps
      const now = Date.now();
      const activityWithId = Object.assign({ id: docRef.id, fetched_at: now, updated_at: now, workout_type: null, sport_type: null, elapsed_time: Number(act.moving_time) || 0 }, activity);
      batch.set(docRef, activityWithId);
      created.push(activityWithId);
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

// Debug: list summary_athletes documents (id, name, nickname, goal)
app.get('/admin/summary-athletes', async (req, res) => {
  if (!db) return res.status(500).json({ error: 'firestore not initialized' });
  try {
    const snaps = await db.collection('summary_athletes').orderBy('name').get();
    const rows = snaps.docs.map(d => ({ id: d.id, name: d.data().name || null, nickname: d.data().nickname || null, goal: d.data().goal || 0 }));
    res.json({ ok: true, rows, count: rows.length });
  } catch (e) {
    console.error('Failed listing summary_athletes', e);
    res.status(500).json({ error: e.message || String(e) });
  }
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
