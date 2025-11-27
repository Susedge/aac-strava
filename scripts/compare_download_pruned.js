const fs = require('fs');
const path = require('path');

const PR = path.join('C:', 'Users', 'Susedge', 'Downloads', 'raw_activities.pruned_normalized_2025-11-27T14-52-19-581Z.json');
const EX = path.join(__dirname, '..', 'raw_activities_export_2025-11-27T14-30-53-547Z.json');

if (!fs.existsSync(PR)) { console.error('Pruned file not found:', PR); process.exit(2);}    
if (!fs.existsSync(EX)) { console.error('Export file not found:', EX); process.exit(2);}    

const pr = JSON.parse(fs.readFileSync(PR, 'utf8')).raw_activities || [];
const ex = JSON.parse(fs.readFileSync(EX, 'utf8')).raw_activities || [];

console.log('pruned file count', pr.length);
console.log('export count', ex.length);

const sig = (a) => `${String(a.athlete_name||'').trim().toLowerCase()}|${Math.round(Number(a.distance||0))}|${Math.round(Number(a.moving_time||a.elapsed_time||0))}|${String(a.name||'').trim().toLowerCase()}`;

const mapSig = new Map();
ex.forEach(e => { const s = sig(e); if (!mapSig.has(s)) mapSig.set(s, []); mapSig.get(s).push(e); });
const dupBySig = Array.from(mapSig.values()).filter(arr => arr.length > 1).length;
console.log('export duplicate signature groups', dupBySig);

const missingFromExport = pr.filter(p => !ex.some(e => String(e.id) === String(p.id)));
console.log('pruned ids missing from export', missingFromExport.length);
console.log('sample missing ids', missingFromExport.slice(0, 10).map(x => x.id));

// show an example pair where ids differ but numeric fields match
const sample = pr.slice(0,5);
sample.forEach(p => {
  const candidate = ex.find(e => e.athlete_name === p.athlete_name && Math.round(Number(e.distance||0)) === Math.round(Number(p.distance||0)) && Math.round(Number(e.moving_time||e.elapsed_time||0)) === Math.round(Number(p.moving_time||p.elapsed_time||0)));
  if (candidate) {
    console.log('FOUND pair: pruned id', p.id, 'export id', candidate.id, 'athlete', p.athlete_name, 'dist', p.distance, 'm_time', p.moving_time || p.elapsed_time);
  } else {
    console.log('NO pair found for pruned id', p.id, 'athlete', p.athlete_name);
  }
});

process.exit(0);
