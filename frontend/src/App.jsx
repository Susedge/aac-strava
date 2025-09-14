import React, {useState, useEffect} from 'react'
import axios from 'axios'
import Admin from './Admin'
import { motion, AnimatePresence } from 'framer-motion'
// lightweight theme toggler (avoids use-dark-mode peer dependency issues)

const API = import.meta.env.VITE_API_BASE || 'http://localhost:4000'

function AuthCallbackView(){
  const [status, setStatus] = useState('Waiting for Strava…');
  const [clubs, setClubs] = useState(null);
  const [error, setError] = useState(null);

  useEffect(() => {
    const run = async () => {
      try {
        const params = new URLSearchParams(window.location.search);
        const code = params.get('code');
        const err = params.get('error');
        if (err) {
          setError(err);
          setStatus('Authorization failed');
          return;
        }
        if (!code) {
          setError('No authorization code found.');
          setStatus('No code found');
          return;
        }
        setStatus('Exchanging code for tokens…');
        const res = await fetch(`${API}/auth/strava/callback`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ code })
        });
        const json = await res.json();
        if (!res.ok) {
          setError(JSON.stringify(json));
          setStatus('Exchange failed');
          return;
        }
        try { if (json && json.data && json.data.athlete) localStorage.setItem('strava_athlete', JSON.stringify(json.data.athlete)); } catch {}
        setStatus('Authorized');
        setClubs(json && json.clubs ? json.clubs : null);
        // Optionally navigate back after a short delay
        setTimeout(() => { window.location.href = '/'; }, 1200);
      } catch (e) {
        setError(e.message || String(e));
        setStatus('Network error');
      }
    };
    run();
  }, []);

  return (
    <div className="app admin-card" style={{padding:16}}>
      <h2>{status}</h2>
      {error && <p style={{color:'#f88'}}>{error}</p>}
      <div style={{textAlign:'left',marginTop:10,fontSize:'0.95rem',color:'#cfe8ff'}}>
        <div><strong>Path:</strong> {window.location.pathname}</div>
        <div><strong>Query:</strong> {window.location.search}</div>
        <div><strong>Clubs (from server):</strong></div>
        <pre style={{whiteSpace:'pre-wrap',background:'rgba(255,255,255,0.04)',padding:8,borderRadius:8,maxHeight:200,overflow:'auto'}}>{clubs ? JSON.stringify(clubs, null, 2) : '(none)'}</pre>
      </div>
    </div>
  );
}

function Leaderboard({ items, onRowClick, query }) {
  const q = (query || '').toLowerCase();
  const filtered = items.filter(it => {
    if (!q) return true;
    const name = (it.athlete && (it.athlete.firstname || it.athlete.first_name || it.athlete.name || it.athlete.username || '')) || '';
    const nick = (it.athlete && (it.athlete.nickname || '')) || '';
    return String(name).toLowerCase().includes(q) || String(nick).toLowerCase().includes(q);
  });

  const columns = [
    { name: '#', selector: (row, idx) => idx + 1, width: '64px' },
    { name: 'Athlete', selector: row => formatName(row.athlete), sortable: true, grow: 2 },
    { name: 'Distance', selector: row => ((row.distance||0)/1000).toFixed(1) + ' km', sortable: true, right: true },
    { name: 'Runs', selector: row => row.count || 0, sortable: true, right: true },
    { name: 'Longest', selector: row => ((row.longest||0)/1000).toFixed(1) + ' km', right: true },
    { name: 'Avg. Pace', selector: row => (formatPace(row.avg_pace) || '-'), right: true },
    { name: 'Elev. Gain', selector: row => (row.elev_gain || 0) + ' m', right: true }
  ]

  return (
    <div className="leaderboard compact">
      <table className="simple-table" role="table">
        <thead>
          <tr>
            <th>#</th>
            <th>Athlete</th>
            <th>Distance</th>
            <th>Runs</th>
            <th>Longest</th>
            <th>Avg. Pace</th>
            <th>Elev. Gain</th>
          </tr>
        </thead>
        <tbody>
            <AnimatePresence>
            {filtered.length === 0 ? (
              <motion.tr initial={{opacity:0}} animate={{opacity:1}} exit={{opacity:0}}>
                <td colSpan={7} style={{padding:20,textAlign:'center',color:'var(--muted)'}}>No matching records</td>
              </motion.tr>
            ) : (
              filtered.map((row, idx) => (
                <motion.tr key={row.id || idx} initial={{opacity:0, y:6}} animate={{opacity:1, y:0}} exit={{opacity:0, y:-6}} onClick={() => onRowClick && onRowClick(row)} style={{cursor:'pointer'}}>
                  <td>{idx + 1}</td>
                  <td>{formatName(row.athlete)}</td>
                  <td className="col-right">{((row.distance||0)/1000).toFixed(1) + ' km'}</td>
                  <td className="col-center">{row.count || 0}</td>
                  <td className="col-right">{((row.longest||0)/1000).toFixed(1) + ' km'}</td>
                  <td className="col-right">{(formatPace(row.avg_pace) || '-')}</td>
                  <td className="col-right">{(row.elev_gain || 0) + ' m'}</td>
                </motion.tr>
              ))
            )}
          </AnimatePresence>
        </tbody>
      </table>
    </div>
  )
}

// Avatars removed: UI no longer shows avatar images

function formatPace(secPerKm) {
  if (!secPerKm && secPerKm !== 0) return null;
  const s = Number(secPerKm || 0);
  const m = Math.floor(s / 60);
  const sec = Math.round(s % 60).toString().padStart(2, '0');
  return `${m}:${sec}`;
}

function formatName(athlete) {
  if (!athlete) return 'Unknown';
  if (athlete.nickname) return athlete.nickname;
  const f = athlete.firstname || athlete.first_name || '';
  const l = athlete.lastname || athlete.last_name || '';
  const n = `${f} ${l}`.trim();
  return n || athlete.username || athlete.name || 'Unknown';
}

export default function App(){
  const [items, setItems] = useState([])
  const [query, setQuery] = useState('')
  const [loading, setLoading] = useState(true)

  useEffect(()=>{ fetchData() }, [])

  async function fetchData(){
    try{
      setLoading(true)
      const r = await axios.get(`${API}/activities`)
      const rows = r.data.rows || []
      const prepared = rows.map(a=>{
        const athlete = a.athlete ? {...a.athlete} : {};
        if (!athlete.profile && a.athlete_profile) athlete.profile = a.athlete_profile;
        if (!athlete.profile && a.athleteProfile) athlete.profile = a.athleteProfile;
        return {
          id: a.id,
          athlete,
          distance: a.summary ? a.summary.distance : 0,
          count: a.summary ? a.summary.count : 0,
          longest: a.summary ? a.summary.longest : 0,
          avg_pace: a.summary ? a.summary.avg_pace : null,
          elev_gain: a.summary ? a.summary.elev_gain : 0
        }
      })
      prepared.sort((a,b)=> (b.distance||0) - (a.distance||0))
      setItems(prepared)
    }catch(e){
      console.error(e)
    }finally{
      setLoading(false)
    }
  }

  function openUser(it){
    const id = encodeURIComponent(it.id);
    window.location.href = `/user.html?id=${id}`;
  }

  // Simple path-based routing: render Admin when path is /admin or /admin.html
  const path = (typeof window !== 'undefined' && window.location && window.location.pathname) ? window.location.pathname : '/'
  // Handle OAuth callback route inside SPA as requested (no popup needed)
  if (path === '/auth/callback' || path === '/auth/callback/index.html') {
    return <AuthCallbackView />
  }
  if (path === '/admin' || path === '/admin.html') {
    return <Admin />
  }

  return (
    <div className="app admin-card">
      <a className="admin-fab" href="/admin">Admin</a>
      <header className="admin-header" style={{padding:12}}>
        <div className="header-actions">
          {/* search moved to table top-right */}
        </div>
      </header>

      <main style={{padding:12}}>
          <div className="table-top" style={{marginTop:12}}>
            <div style={{display:'flex',justifyContent:'flex-end',marginBottom:8}}>
              <input placeholder="Search members" value={query} onChange={e=>setQuery(e.target.value)} className="search-small" />
            </div>
            <Leaderboard items={items} onRowClick={openUser} query={query} />
          </div>
        {items.length===0 && (
          <div className="empty-table">
            <div className="empty-cell">No records found — try running the weekly aggregation or adjust the date range.</div>
          </div>
        )}
      </main>
      {loading && (
        <div className="page-loader">
          <div className="loader-box"><div className="spinner"/> <div>Loading leaderboard…</div></div>
        </div>
      )}
    </div>
  )
}
