import React, {useState, useEffect} from 'react'
import axios from 'axios'
import Admin from './Admin'
import User from './User'
import { CompactTable } from '@table-library/react-table-library/compact'
import { useTheme } from '@table-library/react-table-library/theme'
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

function Leaderboard({ items, onRowClick }) {
  const data = { 
    nodes: items.map((r, i) => ({
      id: r.id,
      idx: i + 1,
      name: formatName(r.athlete),
      distance: formatNumber((r.distance || 0) / 1000, 2),
      distanceUnit: 'km',
      runs: r.count || 0,
      longest: formatNumber((r.longest || 0) / 1000, 2),
      longestUnit: 'km',
      goal: r.goal ? formatNumber(r.goal, 0) : null,
      remaining: r.goal ? formatNumber(Math.max(0, r.goal - ((r.distance || 0) / 1000)), 1) : null,
      goalCompleted: r.goal && ((r.distance || 0) / 1000) >= r.goal,
      pace: (formatPace(r.avg_pace) || '-'),
      elev: formatNumber(r.elev_gain || 0, 2),
      elevUnit: 'm',
      // Add highlighted class for specific row (6th row in your image)
      className: i === 5 ? 'highlighted' : ''
    }))
  };

  const COLUMNS = [
    { label: '#', renderCell: (item) => <span style={{fontWeight: 700}}>{item.idx}</span> },
    { label: 'Name', renderCell: (item) => (
      <span style={{fontWeight: 700}}>
        {item.goalCompleted && <span>🏆 </span>}{item.name}
      </span>
    )},
    { label: 'Distance', renderCell: (item) => (
      <span>{item.distance} <span className="unit">{item.distanceUnit}</span></span>
    )},
    { label: 'Goal', renderCell: (item) => (
      <span>{item.goal || '-'} <span className="unit">km</span></span>
    )},
    { label: 'Remaining', renderCell: (item) => (
      <div style={item.goalCompleted ? {
        backgroundColor: '#fef3c7', 
        margin: '-12px -16px', 
        padding: '12px 16px', 
        fontWeight: 'bold'
      } : {}}>
        {item.goalCompleted ? '🎉Completed!🎉' : (item.remaining || '-')} {!item.goalCompleted && item.remaining && <span className="unit">km</span>}
      </div>
    )},
    { label: 'Runs', renderCell: (item) => item.runs },
    { label: 'Longest', renderCell: (item) => (
      <span>{item.longest} <span className="unit">{item.longestUnit}</span></span>
    )},
    { label: 'Avg. Pace', renderCell: (item) => (
      <span>{item.pace} <span className="unit">{item.pace !== '-' ? '/km' : ''}</span></span>
    )},
    { label: 'Elev. Gain', renderCell: (item) => (
      <span>{item.elev} <span className="unit">{item.elevUnit}</span></span>
    )},
  ];

  const theme = useTheme({
    Table: `
      --data-table-library-grid-template-columns: 30px 1fr 120px 120px 120px 80px 120px 120px 120px;
    `,
    Row: `
      cursor: pointer;
      &:hover {
        background-color: #f8fafc !important;
      }
    `,
  });

  return (
    <div className="leaderboard compact"> 
      <CompactTable 
        columns={COLUMNS} 
        data={data}
        theme={theme}
        onSelectChange={(action, state) => {
          if (action.type === 'SELECT_ROW' && onRowClick) {
            const item = items[action.payload.rowIndex];
            onRowClick(item);
          }
        }}
      />
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

// Format numbers with up to `decimals` decimal places and trim trailing zeros
function formatNumber(n, decimals = 2){
  if (n === null || n === undefined || n === '') return '';
  const num = Number(n || 0);
  if (!isFinite(num)) return String(n);
  return num.toFixed(decimals).replace(/\.0+$|(?<=\.[0-9]*?)0+$/,'').replace(/\.$/,'');
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
  const [loading, setLoading] = useState(true)
  const [lastUpdated, setLastUpdated] = useState(null)

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
          elev_gain: a.summary ? a.summary.elev_gain : 0,
          goal: athlete.goal || 0
        }
      })
      prepared.sort((a,b)=> (b.distance||0) - (a.distance||0))
      setItems(prepared)
      // Compute last updated timestamp from activities summary.updated_at if available
      try {
        const timestamps = rows.map(r => (r.summary && r.summary.updated_at) || null).filter(Boolean);
        if (timestamps.length) {
          const maxTs = Math.max(...timestamps);
          setLastUpdated(new Date(maxTs).toISOString());
        } else {
          setLastUpdated(null);
        }
      } catch (e) { setLastUpdated(null); }
    }catch(e){
      console.error(e)
    }finally{
      setLoading(false)
    }
  }

  function openUser(it){
    const id = encodeURIComponent(it.id);
    window.location.href = `/user/${id}`;
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
  // match /user or /user/{id}
  if (path === '/user' || path === '/user.html' || path.startsWith('/user/')) {
    return <User />
  }

  return (
    <div className="app">
      <div className="challenge-title">
        AAC COMMIT TO RUN CHALLENGE 2025
        {lastUpdated && (
          <div 
            style={{
              fontSize: '11px',
              fontFamily: 'Arial, sans-serif',
              fontWeight: 'normal',
              color: 'white',
              textAlign: 'center',
              marginTop: '5px',
              textTransform: 'none',
              letterSpacing: 'normal',
              lineHeight: '1',
              fontStyle: 'normal'
            }}
          >
            Last updated: {new Date(lastUpdated).toLocaleDateString('en-US', {
              month: '2-digit',
              day: '2-digit', 
              year: 'numeric'
            })}, {new Date(lastUpdated).toLocaleTimeString('en-US', {
              hour: 'numeric',
              minute: '2-digit',
              hour12: true
            })}
          </div>
        )}
      </div>

      <main>
          <div className="table-top">
            <Leaderboard items={items} onRowClick={openUser} />
          </div>
        {/* DataTable will render a noDataComponent when there are no items; avoid rendering an extra empty block that collapses layout */}
      </main>
      {loading && (
        <div className="page-loader">
          <div className="loader-box"><div className="spinner"/> <div>Loading leaderboard…</div></div>
        </div>
      )}
    </div>
  )
}
