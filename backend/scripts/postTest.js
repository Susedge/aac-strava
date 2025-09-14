const path = require('path');
const fs = require('fs');
const admin = require('firebase-admin');

// Simple CLI: node postTest.js --collection=weekly --doc=test-doc --data='{"foo": "bar"}'
const argv = require('minimist')(process.argv.slice(2));

// Resolve credentials: prefer env var, otherwise use backend/service-account.json
let credPath;
if (process.env.GOOGLE_APPLICATION_CREDENTIALS) {
  const envPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;
  credPath = path.isAbsolute(envPath) ? envPath : path.resolve(process.cwd(), envPath);
} else {
  // backend/scripts -> backend/service-account.json
  credPath = path.resolve(__dirname, '..', 'service-account.json');
}

if (!fs.existsSync(credPath)) {
  console.error('Service account JSON not found at', credPath);
  process.exit(1);
}

const cred = require(credPath);
admin.initializeApp({
  credential: admin.credential.cert(cred),
});

const db = admin.firestore();

const collection = argv.collection || 'test';
const docId = argv.doc || `test-${Date.now()}`;
let data = argv.data || JSON.stringify({ createdAt: new Date().toISOString(), note: 'test post' });

try {
  if (typeof data === 'string') data = JSON.parse(data);
} catch (err) {
  console.error('Failed to parse --data JSON:', err.message);
  process.exit(1);
}

async function run() {
  try {
    await db.collection(collection).doc(docId).set(data, { merge: true });
    console.log('Wrote test doc to', `${collection}/${docId}`);
    console.log('Data:', JSON.stringify(data));
    process.exit(0);
  } catch (err) {
    console.error('Write failed:', err.message || err);
    process.exit(1);
  }
}

run();
