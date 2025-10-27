import React, { useEffect, useState, useMemo } from 'react'
import { CompactTable } from '@table-library/react-table-library/compact'
import { useTheme } from '@table-library/react-table-library/theme'
import { useSort } from '@table-library/react-table-library/sort'

const API = import.meta.env.VITE_API_BASE || 'https://aac-strava-backend.onrender.com'

export default function Admin(){
  const [search, setSearch] = useState('')
  const [rows, setRows] = useState([])
  const [loading, setLoading] = useState(true)
  const [authed, setAuthed] = useState(false)
  const [passInput, setPassInput] = useState('')
  const [editing, setEditing] = useState({})
  const [goalEditing, setGoalEditing] = useState({})
  const [status, setStatus] = useState({})
  const [dirty, setDirty] = useState({})
  const [goalDirty, setGoalDirty] = useState({})
  const [savingAll, setSavingAll] = useState(false)

  useEffect(()=>{
    const ok = sessionStorage.getItem('aac_admin_authed') === '1'
    setAuthed(!!ok)
    if (ok) load()
  }, [])

  async function load(){
    setLoading(true)
    try{
      const res = await fetch(API + '/admin/athletes')
      const j = await res.json()
      // Normalize various possible response shapes
      let raw = []
      if (Array.isArray(j)) raw = j
      else if (j && Array.isArray(j.rows)) raw = j.rows
      else if (j && Array.isArray(j.data)) raw = j.data
      else if (j && Array.isArray(j.rows && j.rows.rows)) raw = j.rows.rows
      else if (j && typeof j === 'object') {
        // If object that isn't an array, try to guess at common fields
        if (j.rows) raw = Array.isArray(j.rows) ? j.rows : []
        else if (j.data) raw = Array.isArray(j.data) ? j.data : []
        else raw = []
      }

      // Map incoming rows to a normalized shape
      const normalized = raw.map(item => {
        // Some endpoints return athlete under `athlete` or `member`
        const athlete = item.athlete || item.member || item.user || null
        const id = item.id || (athlete && (athlete.id || athlete.id_str)) || item._id || null
        // derive a display name
        let name = item.name || item.username || item.full_name || ''
        if (!name && athlete) {
          name = athlete.nickname || athlete.firstname || athlete.first_name || ''
          if (athlete.lastname || athlete.last_name) name = (name + ' ' + (athlete.lastname || athlete.last_name)).trim()
          if (!name) name = athlete.username || athlete.name || ''
        }
        const nickname = item.nickname || (athlete && athlete.nickname) || ''
        const goal = item.goal || 0
        const distance_display = item.distance_display || item.distance || (item.summary && item.summary.distance) || ''
        const runs = item.runs || item.count || (item.summary && item.summary.count) || 0
        return { id, name, nickname, goal, distance_display, distance: item.distance, runs, raw: item }
      }).filter(r => r.id !== null)

      if (normalized.length === 0 && raw.length > 0) {
        console.warn('Admin.load: response rows found but could not normalize items, inspect payload', j)
      }

        setRows(normalized)
        // reset editing state
        const map = {}
        const goalMap = {}
        normalized.forEach(r => { 
          map[r.id] = r.nickname || ''
          goalMap[r.id] = r.goal || 0
        })
        setEditing(map)
        setGoalEditing(goalMap)
        setDirty({})
        setGoalDirty({})
    }catch(e){
      console.error('failed to load admin athletes', e)
      setRows([])
    }finally{ setLoading(false) }
  }

  function handleAuthSubmit(e){
    e && e.preventDefault()
    if (passInput === 'susedge'){
      sessionStorage.setItem('aac_admin_authed','1')
      setAuthed(true)
      load()
    }else{
      alert('Incorrect password')
      setPassInput('')
    }
  }

  // mark a row as dirty when editing changes from original
  function handleEdit(id, value){
    setEditing(s=>({...s,[id]:value}))
    const orig = rows.find(r=>r.id===id)
    const was = orig ? (orig.nickname || '') : ''
    setDirty(d=>({...d,[id]: value !== was}))
  }

  // mark a row as dirty when goal editing changes from original
  function handleGoalEdit(id, value){
    setGoalEditing(s=>({...s,[id]:Number(value) || 0}))
    const orig = rows.find(r=>r.id===id)
    const was = orig ? (orig.goal || 0) : 0
    setGoalDirty(d=>({...d,[id]: Number(value) !== was}))
  }

  async function saveAll(){
    const nicknameToSave = Object.keys(dirty).filter(id => dirty[id])
    const goalToSave = Object.keys(goalDirty).filter(id => goalDirty[id])
    const allToSave = [...new Set([...nicknameToSave, ...goalToSave])]
    
    if (allToSave.length === 0) return alert('No changes to save')
    setSavingAll(true)
    setStatus({})

    for (const id of allToSave){
      const nickname = editing[id]
      const goal = goalEditing[id]
      const payload = {}
      if (dirty[id]) payload.nickname = nickname
      if (goalDirty[id]) payload.goal = goal
      
      setStatus(s=>({...s,[id]:'saving'}))
      console.log('Saving athlete', id, 'with payload:', payload)
      try{
        
        const res = await fetch(`${API}/admin/athlete/${id}`, {
          method: 'POST', headers: {'content-type':'application/json'}, body: JSON.stringify(payload)
        })
        if (!res.ok) {
          const errorText = await res.text()
          console.error('Save failed with status:', res.status, errorText)
          throw new Error(`Save failed: ${res.status} ${errorText}`)
        }
        
        // Parse response to confirm what was actually saved
        const savedData = await res.json()
        console.log('Save response:', savedData)
        
        // Extract athlete data from response
        const athlete = savedData.athlete || {}
        
        // Only update UI with data that was confirmed saved by backend
        const updates = {}
        if (dirty[id] && athlete.nickname !== undefined) updates.nickname = athlete.nickname
        if (goalDirty[id] && athlete.goal !== undefined) updates.goal = athlete.goal
        
        // on success update rows with confirmed data
        setRows(prev=> prev.map(r=> r.id===id ? {...r, ...updates} : r))
        setStatus(s=>({...s,[id]:'success'}))
        
        // Only clear dirty flags for fields that were actually saved
        if (dirty[id] && athlete.nickname !== undefined) {
          setDirty(d=>{ const n={...d}; delete n[id]; return n })
        }
        if (goalDirty[id] && athlete.goal !== undefined) {
          setGoalDirty(d=>{ const n={...d}; delete n[id]; return n })
        }
      }catch(e){
        console.error('saveAll: failed saving', id, e)
        setStatus(s=>({...s,[id]:'error'}))
      }
    }

    // hide saving state after a short pause
    setTimeout(()=> setStatus({}), 1500)
    setSavingAll(false)
  }

  // apply search filtering so CompactTable only shows matching rows
  const filtered = rows.filter(r => {
    if (!search) return true
    const s = search.toLowerCase()
    return (r.name||'').toLowerCase().includes(s) || (r.nickname||'').toLowerCase().includes(s) || (editing[r.id] || '').toLowerCase().includes(s)
  })

  // Build CompactTable data and columns
  const data = { 
    nodes: filtered.map((r, i) => ({
      id: r.id,
      idx: i + 1,
      name: r.name,
      nickname: r.nickname || '',
      goal: r.goal || 0,
      distance_display: r.distance_display,
      runs: r.runs || 0,
      raw: r
    }))
  };

  const ADMIN_COLUMNS = [
    { label: '#', renderCell: (item) => <span style={{fontWeight: 700}}>{item.idx}</span> },
    { label: 'Name', renderCell: (item) => (
      <div className="col-truncate" style={{fontWeight: 700}}>{item.name}</div>
    ), sort: { sortKey: 'name' }},
    { label: 'Nickname', renderCell: (item) => (
      <input 
        className="admin-input" 
        value={editing[item.id] ?? ''} 
        onChange={e=>handleEdit(item.id, e.target.value)} 
      />
    ), sort: { sortKey: 'nickname' }},
    { label: 'Goal (km)', renderCell: (item) => (
      <input 
        className="admin-input admin-input-number" 
        type="number" 
        value={goalEditing[item.id] ?? 0} 
        onChange={e=>handleGoalEdit(item.id, e.target.value)}
      />
    ), sort: { sortKey: 'goal' }}
  ];

  const theme = useTheme({
    Table: `
      --data-table-library-grid-template-columns: 40px 1fr 180px 120px;
    `,
    Row: `
      cursor: default;
      &:hover {
        background-color: #f8fafc !important;
      }
    `,
  });

  const sort = useSort(data, {
    onChange: () => {},
  }, {
    sortFns: {
      name: (array) => array.sort((a, b) => a.name.localeCompare(b.name)),
      nickname: (array) => array.sort((a, b) => (a.nickname || '').localeCompare(b.nickname || '')),
      goal: (array) => array.sort((a, b) => (a.goal || 0) - (b.goal || 0)),
    },
  });

  return (
    <div className="admin-page admin-card">
      {!authed && (
        <div className="admin-overlay">
          <div className="admin-lock">
            <h2>AAC Admin</h2>
            <form onSubmit={handleAuthSubmit} className="lock-form">
              <label>Enter password to continue</label>
              <input id="adminPwd" autoFocus type="password" value={passInput} onChange={e=>setPassInput(e.target.value)} className="admin-input" />
              <div className="hint" style={{marginTop:6}}>This is a client-side gate for local admin use.</div>
              <div style={{display:'flex',gap:8,marginTop:12,justifyContent:'flex-start',alignItems:'center'}}>
                <button className="btn unlock-btn" type="submit">Unlock</button>
                <a className="btn-ghost" href="/" style={{marginLeft:8}}>Back to site</a>
              </div>
            </form>
          </div>
        </div>
      )}

      {authed && (
        <>
          <header className="admin-header">
            <h2 style={{margin:0}}>AAC Admin — Members</h2>
            <div className="header-actions">
              <a className="btn btn-ghost" href="/">Back to main</a>
              <a className="btn btn-ghost" href="/connect.html" target="_blank" rel="noopener noreferrer" style={{marginLeft:8}}>Connect</a>
              <button className="btn btn-ghost" onClick={load} style={{marginLeft:8}}>Reload</button>
            </div>
          </header>

          <main style={{marginTop:20}}>
            {loading && <div className="loading">Loading…</div>}
            {!loading && rows.length===0 && <div className="empty">No athletes found</div>}

            {!loading && (
              <div className="admin-table-wrap compact">
                <div style={{display:'flex',justifyContent:'flex-end',marginBottom:8,marginTop:16,gap:8}}>
                  <input placeholder="Search members" value={search} onChange={e=>setSearch(e.target.value)} className="search-small" />
                </div>
                <CompactTable
                  columns={ADMIN_COLUMNS}
                  data={data}
                  theme={theme}
                  sort={sort}
                />
                {filtered.length === 0 && <div className="no-results-row">No matching members</div>}
              </div>
            )}
          </main>
          <div style={{display:'flex',justifyContent:'flex-end',gap:8,marginTop:12}}>
            <button className="save-all" onClick={saveAll} disabled={savingAll}>{savingAll ? 'Saving…' : 'Save All'}</button>
          </div>
          {(loading || savingAll) && (
            <div className="page-loader">
              <div className="loader-box"><div className="spinner"/> <div>{savingAll ? 'Saving changes…' : 'Loading members…'}</div></div>
            </div>
          )}
        </>
      )}
    </div>
  )
}
