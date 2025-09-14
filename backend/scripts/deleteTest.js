const path = require('path');
const fs = require('fs');
const admin = require('firebase-admin');

// Simple CLI: node deleteTest.js --collection=weekly --doc=test-doc
const argv = require('minimist')(process.argv.slice(2));

// Resolve credentials: prefer env var, otherwise use backend/service-account.json
let credPath;
if (process.env.GOOGLE_APPLICATION_CREDENTIALS) {
  const envPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;
  credPath = path.isAbsolute(envPath) ? envPath : path.resolve(process.cwd(), envPath);
} else {
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
const docId = argv.doc;

if (!docId) {
  console.error('--doc is required');
  process.exit(1);
}

async function run() {
  try {
    await db.collection(collection).doc(docId).delete();
    console.log('Deleted', `${collection}/${docId}`);
    process.exit(0);
  } catch (err) {
    console.error('Delete failed:', err.message || err);
    process.exit(1);
  }
}

run();
