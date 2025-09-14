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
app.get('/health', (req, res) => res.json({ status: 'ok' }));

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
            if (chunk.length < perPage || page >= 10) break; // safety cap 10 pages for members
            page += 1;
          }
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
          if (chunk.length < perPage || page >= 5) break; // safety cap 5 pages
          page += 1;
        }

        // Aggregate activities by a robust athlete key (id preferred, fallback to username or name)
        const agg = new Map();
        const makeAthleteKey = (a) => {
          if (!a) return null;
          if (a.id) return String(a.id);
          if (a.username) return `username:${a.username}`;
          const fn = (a.firstname || '').trim();
          const ln = (a.lastname || '').trim();
          if (fn || ln) return `name:${(fn + ' ' + ln).trim()}`;
          if (a.resource_state && a.resource_state === 1 && a.id) return String(a.id);
          return null;
        };

        for (const it of acts) {
          if (it.start_date) {
            const ts = Date.parse(it.start_date);
            if (!Number.isNaN(ts) && ts / 1000 < oneWeekAgo) continue;
          }
          const key = makeAthleteKey(it.athlete) || null;
          // If no identifiable athlete info, still try to use the activity 'athlete' name fields
          if (!key) {
            // some ClubActivity responses include athlete as minimal meta; try sport_type owner name fields
            const altName = it.athlete && (it.athlete.username || (it.athlete.firstname || '') + ' ' + (it.athlete.lastname || ''));
            if (altName) {
              const k = `name:${String(altName).trim()}`;
              const curA = agg.get(k) || { athlete: { name: altName }, distance: 0, count: 0, longest: 0, total_moving_time: 0, elev_gain: 0 };
              const dist = Number(it.distance || 0);
              const mt = Number(it.moving_time || 0);
              const eg = Number(it.total_elevation_gain || it.elev_total || 0);
              curA.distance += dist;
              curA.count += 1;
              curA.longest = Math.max(curA.longest || 0, dist);
              curA.total_moving_time += mt;
              curA.elev_gain += eg;
              if (!curA.athlete && it.athlete) curA.athlete = it.athlete;
              agg.set(k, curA);
            }
            continue;
          }
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
    // Allow overriding the debug start date via query ?start_date=YYYY-MM-DD or env AGGREGATION_START_DATE
    const qDate = req.query && req.query.start_date ? String(req.query.start_date) : null;
    const envDate = process.env.AGGREGATION_START_DATE || null;
    const startDateStr = qDate || envDate || null;
    let oneWeekAgo;
    if (startDateStr) {
      const parsedMs = parseDateWithTZ(startDateStr, process.env.AGGREGATION_TIMEZONE || 'Asia/Manila');
      oneWeekAgo = Number.isNaN(parsedMs) ? Math.floor(Date.now() / 1000) - 7 * 24 * 60 * 60 : Math.floor(parsedMs / 1000);
    } else {
      oneWeekAgo = Math.floor(Date.now() / 1000) - 7 * 24 * 60 * 60;
    }
    const r = await axios.get(`https://www.strava.com/api/v3/clubs/${clubId}/activities`, {
      params: { after: oneWeekAgo, per_page: 200 },
      headers: { Authorization: `Bearer ${token}` }
    });
    res.json({ ok: true, activities: r.data });
  } catch (err) {
    console.error('debug club fetch failed', err.response ? err.response.data : err.message);
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
      if (chunk.length < perPage || page >= 10) break;
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
    const snaps = await db.collection('activities').get();
    const rows = snaps.docs.map(d => ({ id: d.id, ...d.data() }));
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

// Admin: list summary athletes
app.get('/admin/athletes', async (req, res) => {
  if (!db) return res.status(500).json({ error: 'firestore not initialized' });
  try {
    const snaps = await db.collection('summary_athletes').orderBy('name').get();
    const rows = snaps.docs.map(d => ({ id: d.id, ...d.data() }));
    res.json({ ok: true, rows });
  } catch (e) { res.status(500).json({ error: e.message || String(e) }); }
});

// Admin: update an athlete's nickname and optional manual strava id
app.post('/admin/athlete/:id', async (req, res) => {
  if (!db) return res.status(500).json({ error: 'firestore not initialized' });
  const id = req.params && req.params.id;
  const { nickname, manual_strava_id } = req.body || {};
  if (!id) return res.status(400).json({ error: 'id required' });
  try {
    const update = {};
    if (typeof nickname !== 'undefined') update.nickname = nickname || null;
    if (typeof manual_strava_id !== 'undefined') update.manual_strava_id = manual_strava_id || null;
    update.updated_at = Date.now();
    await db.collection('summary_athletes').doc(String(id)).set(update, { merge: true });

    // We no longer auto-fetch or persist avatars; manual_strava_id is stored as metadata only

    // Also merge nickname into activities doc so frontend can show it
    try {
      const activityUpdate = {};
      if (typeof nickname !== 'undefined') activityUpdate['athlete.nickname'] = nickname || null;
      if (typeof manual_strava_id !== 'undefined') activityUpdate['athlete.manual_strava_id'] = manual_strava_id || null;
      if (Object.keys(activityUpdate).length) {
        await db.collection('activities').doc(String(id)).set({ athlete: { nickname: nickname || null, manual_strava_id: manual_strava_id || null } }, { merge: true });
      }
    } catch(e){ console.warn('failed merging nickname into activities', e && e.message || e); }

    const doc = await db.collection('summary_athletes').doc(String(id)).get();
    res.json({ ok: true, athlete: doc.exists ? doc.data() : null });
  } catch (e) { res.status(500).json({ error: e.message || String(e) }); }
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
