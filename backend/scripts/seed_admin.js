#!/usr/bin/env node
require('dotenv').config();
const admin = require('firebase-admin');
const fs = require('fs');
const path = require('path');

function parseArgs() {
  const out = {};
  for (let i = 2; i < process.argv.length; i++) {
    const a = process.argv[i];
    if (a.startsWith('--')) {
      const [k, v] = a.slice(2).split('=');
      out[k] = v === undefined ? true : v;
    }
  }
  return out;
}

function tryLoadCred() {
  // Respect env var first
  if (process.env.GOOGLE_APPLICATION_CREDENTIALS) {
    const p = process.env.GOOGLE_APPLICATION_CREDENTIALS;
    const full = path.isAbsolute(p) ? p : path.join(process.cwd(), p);
    if (fs.existsSync(full)) return require(full);
  }
  // Look for common local locations
  const candidates = [
    path.join(process.cwd(), 'service-account.json'),
    path.join(__dirname, '..', 'service-account.json'),
    path.join(__dirname, '..', 'service-account', 'service-account.json')
  ];
  for (const c of candidates) {
    if (fs.existsSync(c)) return require(c);
  }
  return null;
}

async function main() {
  const args = parseArgs();
  const cred = tryLoadCred();
  if (!cred) {
    console.error('No service account JSON found. Set GOOGLE_APPLICATION_CREDENTIALS or place service-account.json in backend folder.');
    process.exit(1);
  }

  try {
    admin.initializeApp({ credential: admin.credential.cert(cred) });
  } catch (e) {
    console.error('Failed to initialize Firebase Admin:', e && e.message || e);
    process.exit(1);
  }

  const db = admin.firestore();

  // Tokens from env or CLI args
  const access_token = args.access_token || args.access || process.env.ADMIN_ACCESS_TOKEN || process.env.ACCESS_TOKEN || null;
  const refresh_token = args.refresh_token || args.refresh || process.env.ADMIN_REFRESH_TOKEN || process.env.REFRESH_TOKEN || null;
  const athlete_id = args.athlete_id || process.env.ADMIN_ATHLETE_ID || null;
  const expires_at = args.expires_at ? Number(args.expires_at) : (process.env.ADMIN_EXPIRES_AT ? Number(process.env.ADMIN_EXPIRES_AT) : Math.floor(Date.now() / 1000) + 24 * 3600);

  const adminDoc = {
    access_token: access_token || 'SEED_ACCESS_TOKEN',
    refresh_token: refresh_token || 'SEED_REFRESH_TOKEN',
    athlete_id: athlete_id || null,
    expires_at: expires_at || null,
    club_id: '1388675',
    club_name: 'AAC Active Club',
    updated_at: Date.now()
  };

  try {
    const ref = db.collection('admin').doc('strava');
    await ref.set(adminDoc, { merge: true });
    const chk = await ref.get();
    console.log('Wrote admin/strava doc:');
    console.log(JSON.stringify(chk.exists ? chk.data() : null, null, 2));
    process.exit(0);
  } catch (e) {
    console.error('Failed writing admin/strava doc:', e && e.message || e);
    process.exit(1);
  }
}

main();
