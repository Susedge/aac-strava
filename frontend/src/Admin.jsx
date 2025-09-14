import React, { useEffect, useState, useMemo } from 'react'
import DataTable from 'react-data-table-component'

const API = import.meta.env.VITE_API_BASE || 'http://localhost:4000'

export default function Admin(){
  const [search, setSearch] = useState('')
  const [rows, setRows] = useState([])
  const [loading, setLoading] = useState(true)
  const [authed, setAuthed] = useState(false)
  // default to 15 rows per page for consistency with main leaderboard
  const [rowsPerPage, setRowsPerPage] = useState(15)
  const [passInput, setPassInput] = useState('')
  const [editing, setEditing] = useState({})
  const [status, setStatus] = useState({})
  const [dirty, setDirty] = useState({})
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
        const distance_display = item.distance_display || item.distance || (item.summary && item.summary.distance) || ''
        const runs = item.runs || item.count || (item.summary && item.summary.count) || 0
        return { id, name, nickname, distance_display, distance: item.distance, runs, raw: item }
      }).filter(r => r.id !== null)

      if (normalized.length === 0 && raw.length > 0) {
        console.warn('Admin.load: response rows found but could not normalize items, inspect payload', j)
      }

        setRows(normalized)
        // reset editing state
        const map = {}
        normalized.forEach(r => { map[r.id] = r.nickname || '' })
        setEditing(map)
        setDirty({})
    }catch(e){
      console.error('failed to load admin athletes', e)
      setRows([])
    }finally{ setLoading(false) }
  }

  const columns = useMemo(()=>[
    { name: '#', cell: (row, index) => index + 1, width: '72px' },
    { name: 'Name', selector: r => r.name || '', sortable: true, wrap: true },
    { name: 'Nickname', selector: r => r.nickname || '', sortable: true },
    { name: 'Distance', selector: r => r.distance_display || r.distance || '', right: true },
    { name: 'Runs', selector: r => r.runs || 0, right: true }
  ], [])

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

  async function saveNickname(id){
    // Deprecated: per-row save removed in favor of batch Save All
    return
  }

  // mark a row as dirty when editing changes from original
  function handleEdit(id, value){
    setEditing(s=>({...s,[id]:value}))
    const orig = rows.find(r=>r.id===id)
    const was = orig ? (orig.nickname || '') : ''
    setDirty(d=>({...d,[id]: value !== was}))
  }

  async function saveAll(){
    const toSave = Object.keys(dirty).filter(id => dirty[id])
    if (toSave.length === 0) return alert('No changes to save')
    setSavingAll(true)
    setStatus({})

    for (const id of toSave){
      const nickname = editing[id]
      setStatus(s=>({...s,[id]:'saving'}))
      try{
        const res = await fetch(`${API}/admin/athlete/${id}`, {
          method: 'POST', headers: {'content-type':'application/json'}, body: JSON.stringify({ nickname })
        })
        if (!res.ok) throw new Error('save failed')
        // on success update rows
        setRows(prev=> prev.map(r=> r.id===id ? {...r, nickname} : r))
        setStatus(s=>({...s,[id]:'success'}))
        setDirty(d=>{ const n={...d}; delete n[id]; return n })
      }catch(e){
        console.error('saveAll: failed saving', id, e)
        setStatus(s=>({...s,[id]:'error'}))
      }
    }

    // hide saving state after a short pause
    setTimeout(()=> setStatus({}), 1500)
    setSavingAll(false)
  }

  // Build DataTable columns including editable nickname and status
  const dtColumns = [
    { name: '#', selector: (row) => row._index, width: '64px' },
    { name: 'Name', selector: row => row.name, sortable: true, grow: 2, minWidth: '220px' },
    { name: 'Nickname', cell: row => (
        <input className="admin-input" value={editing[row.id] ?? ''} onChange={e=>handleEdit(row.id, e.target.value)} />
      ), minWidth: '200px', grow: 1
    },
    { name: '', cell: row => (
        <div style={{display:'flex',alignItems:'center',justifyContent:'flex-end',gap:8}}>
          {status[row.id] === 'success' && <span className="save-status save-success">Saved</span>}
          {status[row.id] === 'error' && <span className="save-status save-error">Failed</span>}
        </div>
      ), width: '140px'
    }
  ]

  // short search animation state (mirrors Leaderboard behavior)
  const [isSearching, setIsSearching] = useState(false)
  useEffect(()=>{
    if (!search) return setIsSearching(false)
    setIsSearching(true)
    const t = setTimeout(()=> setIsSearching(false), 280)
    return ()=> clearTimeout(t)
  },[search])

  // apply search filtering so DataTable only shows matching rows (keeps previous behavior)
  const filtered = rows.filter(r => {
    if (!search) return true
    const s = search.toLowerCase()
    return (r.name||'').toLowerCase().includes(s) || (r.nickname||'').toLowerCase().includes(s) || (editing[r.id] || '').toLowerCase().includes(s)
  })
  const data = filtered.map((r,i)=> ({...r,_index:i+1}))

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

          <main style={{marginTop:12}}>
            {loading && <div className="loading">Loading…</div>}
            {!loading && rows.length===0 && <div className="empty">No athletes found</div>}

            {!loading && (
              <div className="admin-table-wrap compact">
                <div style={{display:'flex',justifyContent:'flex-end',marginBottom:8,gap:8}}>
                  <input placeholder="Search members" value={search} onChange={e=>setSearch(e.target.value)} className="search-small" />
                </div>
                <DataTable
                  columns={dtColumns}
                  data={data}
                  noHeader
                  pagination
                  paginationPerPage={rowsPerPage}
                  paginationRowsPerPageOptions={[5,10,15,20,50]}
                  noDataComponent={<div className="no-results-row">No matching members</div>}
                />
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
