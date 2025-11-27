const fs = require('fs');
const path = require('path');

const EXPORT = path.join(__dirname, '..', 'raw_activities_export_2025-11-27T16-27-38-558Z.json');
if (!fs.existsSync(EXPORT)) {
  console.error('Export file not found:', EXPORT);
  process.exit(2);
}

const data = JSON.parse(fs.readFileSync(EXPORT, 'utf8'));
const list = data.raw_activities || data.raw || data;
if (!Array.isArray(list)) {
  console.error('No activities array found in export');
  process.exit(3);
}

const exel = list.filter(a => (a.athlete_name || '').toLowerCase().includes('exel'));
console.log('Found', exel.length, 'activities for Exel');
// group by start_date + rounded distance with 0.1m precision
const groups = new Map();
for (const a of exel) {
  const d = Number(a.distance || 0);
  const mt = Number(a.moving_time || a.elapsed_time || 0);
  // represent distance in meters with 1 decimal
  const rd5 = Math.round(d * 10) / 10;
  const key = `${(a.athlete_name||'').toLowerCase()}|${a.start_date||''}|${rd5}|${Math.round(mt)}|${String(a.name||'').slice(0,50)}`;
  if (!groups.has(key)) groups.set(key, []);
  groups.get(key).push(a);
}

// Print grouped summary and samples
let groupCount = 0;
for (const [k, arr] of groups.entries()) {
  groupCount++;
}
console.log('Grouped into', groupCount, 'unique keys with rounding *0.1m*');

// Now simulate dedupe grouping (server uses Math.round to nearest meter)
const dedupeGroups = new Map();
for (const a of exel) {
  const name = (a.athlete_name || '').toLowerCase().trim();
  const dist = Math.round(Number(a.distance || 0));
  const mt = Math.round(Number(a.moving_time || a.elapsed_time || 0));
  const key = `${name}|${dist}|${mt}`;
  if (!dedupeGroups.has(key)) dedupeGroups.set(key, []);
  dedupeGroups.get(key).push(a);
}

const dupDedupeGroups = Array.from(dedupeGroups.entries()).filter(([k, arr]) => arr.length > 1);
console.log('Dedupe groups (Math.round distance, moving_time) count:', dedupeGroups.size, 'duplicate groups:', dupDedupeGroups.length);
if (dupDedupeGroups.length>0) {
  console.log('Sample dedupe-duplicate keys (first 10):');
  dupDedupeGroups.slice(0,10).forEach(([k,arr]) => console.log(k, '->', arr.map(x=>x.id).join(', ')));
}

// Now print details of small distances (<1000m) and where there are nearby duplicates
const small = exel.filter(a => Number(a.distance || 0) < 1000);
console.log('Small distance activities (<1000m):', small.length);
small.slice(0,50).forEach(a => {
  console.log(`${a.id || ''} | ${a.athlete_name || ''} | ${a.distance || 0} | mt:${a.moving_time||a.elapsed_time||0} | start:${a.start_date||''} | name:${a.name||''} | source:${a.source||''}`);
});

// print any close matches based on distance within 2% or 10m and mt within 10s
function close(a,b){
  const da = Number(a.distance||0), db = Number(b.distance||0);
  const mta = Number(a.moving_time||a.elapsed_time||0), mtb = Number(b.moving_time||b.elapsed_time||0);
  const absd = Math.abs(da-db);
  const reld = absd / Math.max(1, Math.max(Math.abs(da), Math.abs(db)));
  const absmt = Math.abs(mta-mtb);
  return (absd <= 10 || reld <= 0.02) && (absmt <= 10);
}

const pairs = [];
for (let i=0;i<exel.length;i++){
  for (let j=i+1;j<exel.length;j++){
    if (close(exel[i], exel[j])){
      pairs.push([exel[i], exel[j]]);
    }
  }
}
console.log('Close pairs (distance <=10m OR <=2% AND mt<=10s):', pairs.length);
if (pairs.length>0){
  console.log('Sample pairs:')
  pairs.slice(0,10).forEach(([a,b])=>{
    console.log(`- ${a.id||''} ${a.distance}m mt:${a.moving_time||a.elapsed_time}    <-->    ${b.id||''} ${b.distance}m mt:${b.moving_time||b.elapsed_time}`)
  })
}

// Check normalized activity name equality for close pairs
const normalizeActivityName = (s) => {
  if (!s) return '';
  let t = String(s || '').toLowerCase();
  t = t.replace(/\b\d+(?:[.,]\d+)?\s*(km|kilometer|kilometre|m|meter|metre)\b/gi, '');
  t = t.replace(/[\d,.:]/g, '');
  t = t.replace(/[^a-z\s]/g, '');
  t = t.replace(/\s+/g, ' ').trim();
  return t;
};

const nameNormPairs = pairs.filter(([a,b]) => normalizeActivityName(a.name || '') === normalizeActivityName(b.name || ''));
console.log('Close pairs with NORMALIZED name equality:', nameNormPairs.length);
if (nameNormPairs.length > 0) {
  nameNormPairs.slice(0,10).forEach(([a,b]) => console.log(`- ${a.id} "${a.name}" -> "${normalizeActivityName(a.name)}"  <-->  ${b.id} "${b.name}" -> "${normalizeActivityName(b.name)}"`));
}

// Find close pairs that currently would be in different dedupe groups (Math.round produced different dist or mt)
const crossGroup = [];
for (const [a,b] of pairs) {
  const ad = Math.round(Number(a.distance || 0));
  const bd = Math.round(Number(b.distance || 0));
  const amt = Math.round(Number(a.moving_time || a.elapsed_time || 0));
  const bmt = Math.round(Number(b.moving_time || b.elapsed_time || 0));
  if (ad !== bd || amt !== bmt) crossGroup.push([a,b]);
}
console.log('Close pairs that are in DIFFERENT dedupe groups (rounding causes mismatch):', crossGroup.length);
if (crossGroup.length>0) {
  console.log('Sample cross-group pairs:');
  crossGroup.slice(0,10).forEach(([a,b]) => console.log(`- ${a.id} dist:${a.distance} -> ${Math.round(Number(a.distance||0))} | ${b.id} dist:${b.distance} -> ${Math.round(Number(b.distance||0))} | mt:${a.moving_time||a.elapsed_time} -> ${Math.round(Number(a.moving_time||a.elapsed_time||0))} vs ${Math.round(Number(b.moving_time||b.elapsed_time||0))}`));
}

process.exit(0);
