#!/usr/bin/env node
const fs = require('fs');
const path = require('path');

const IN = path.join(__dirname, '..', 'raw_activities_export_2025-11-27T14-30-53-547Z.json');
const OUT = path.join(__dirname, '..', 'raw_activities.deduped.pruned_normalized.json');

if (!fs.existsSync(IN)) {
  console.error('Export file not found:', IN);
  process.exit(2);
}

const raw = JSON.parse(fs.readFileSync(IN, 'utf8'));
const arr = raw && Array.isArray(raw.raw_activities) ? raw.raw_activities : [];

// We'll pick a canonical representative for each signature:
// signature = athlete_name|rounded_distance|rounded_moving_time|name
const sig = a => `${String(a.athlete_name||'').trim().toLowerCase()}|${Math.round(Number(a.distance||0))}|${Math.round(Number(a.moving_time||a.elapsed_time||0))}|${String(a.name||'').trim().toLowerCase()}`;

// For each signature group, prefer the doc with the earliest fetched_at (if present), otherwise the first encountered
const groups = new Map();
arr.forEach(item => {
  const s = sig(item);
  if (!groups.has(s)) groups.set(s, []);
  groups.get(s).push(item);
});

const deduped = [];
for (const [s, items] of groups.entries()) {
  if (items.length === 1) {
    deduped.push(items[0]);
    continue;
  }

  // choose by earliest fetched_at (numeric), then smallest id as tie-breaker
  items.sort((a,b)=>{
    const fa = Number(a.fetched_at||0) || 0;
    const fb = Number(b.fetched_at||0) || 0;
    if (fa && fb) return fa - fb;
    if (fa && !fb) return -1;
    if (!fa && fb) return 1;
    return String(a.id||'').localeCompare(String(b.id||''));
  });
  deduped.push(items[0]);
}

const outPayload = { exported_at: Date.now(), raw_activities: deduped };
fs.writeFileSync(OUT, JSON.stringify(outPayload, null, 2));
console.log('Processed', arr.length, 'items -> deduped to', deduped.length, 'items. Wrote:', OUT);

process.exit(0);
