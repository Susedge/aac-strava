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
  const [activeTab, setActiveTab] = useState('members') // 'members' or 'activities'
  
  // Activity management state
  const [activities, setActivities] = useState([])
  const [activitiesLoading, setActivitiesLoading] = useState(false)
  const [adminBusy, setAdminBusy] = useState(false)
  const [adminBusyMsg, setAdminBusyMsg] = useState('')
  const [showAddActivity, setShowAddActivity] = useState(false)
  const [activityFilter, setActivityFilter] = useState('') // Filter by athlete name
  const [activitySort, setActivitySort] = useState('start_date') // Sort field  
  const [activitySortOrder, setActivitySortOrder] = useState('desc') // asc or desc
  const [newActivity, setNewActivity] = useState({
    athlete_name: '',
    distance: '',
    moving_time: '',
    start_date: '',
    type: 'Run',
    name: '',
    elevation_gain: ''
  })
  const [athleteNames, setAthleteNames] = useState([]) // For dropdown

  useEffect(()=>{
    const ok = sessionStorage.getItem('aac_admin_authed') === '1'
    setAuthed(!!ok)
    if (ok) {
      load()
      loadActivities()
    }
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
        // derive a display name (prefer real name fields over nickname for dropdown)
        let realName = ''
        if (athlete) {
          // Prefer explicit name field, else firstname+lastname
          realName = athlete.name || `${athlete.firstname || athlete.first_name || ''} ${athlete.lastname || athlete.last_name || ''}`.trim()
        }
        if (!realName) realName = item.name || item.full_name || item.username || ''
        // If the summary_athletes document used a name-based doc id (e.g. 'name:Arvin T.')
        // prefer the id-derived full name when it contains more detail (middle initials)
        try {
          if (item.id && String(item.id).startsWith('name:')) {
            const fromId = String(item.id).replace(/^name:/, '').trim()
            if (fromId && fromId.length > (realName || '').trim().length) realName = fromId
          }
        } catch (e) { /* ignore */ }

        // nickname stored separately; may be used for UI display elsewhere
        const nickname = item.nickname || (athlete && athlete.nickname) || ''
        const goal = item.goal || 0
        const distance_display = item.distance_display || item.distance || (item.summary && item.summary.distance) || ''
        const runs = item.runs || item.count || (item.summary && item.summary.count) || 0
        return { id, name: realName, nickname, goal, distance_display, distance: item.distance, runs, raw: item }
      }).filter(r => r.id !== null)

      if (normalized.length === 0 && raw.length > 0) {
        console.warn('Admin.load: response rows found but could not normalize items, inspect payload', j)
      }

        setRows(normalized)
        
        // Build athlete names list for dropdown - use real Strava name, NOT nickname
        const nameSet = new Map()
        normalized.forEach(r => {
          const n = (r.name || '').trim()
          if (n && !nameSet.has(n.toLowerCase())) nameSet.set(n.toLowerCase(), n)
        })
        const names = Array.from(nameSet.values()).sort((a,b)=> a.localeCompare(b))
        setAthleteNames(names)
        
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

  async function handleDeleteAthlete(id){
    if (!id) return alert('No id to delete')
    if (!confirm('Delete this athlete summary? This will remove the member row and aggregated summary (raw activities will not be deleted).')) return
    try{
      const res = await fetch(`${API}/admin/athlete/${encodeURIComponent(id)}`, { method: 'DELETE' })
      if (!res.ok) throw new Error('Delete failed')
      alert('Athlete deleted')
      load()
      loadActivities()
    }catch(e){
      console.error('Failed to delete athlete', e)
      alert('Error deleting athlete: ' + e.message)
    }
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
    ,{ label: 'Actions', renderCell: (item) => (
      <div style={{display:'flex',gap:8}}>
        <button
          onClick={() => handleDeleteAthlete(item.raw && item.raw.id ? item.raw.id : item.id)}
          style={{background:'#fee2e2',color:'#991b1b',border:'none',padding:'6px 10px',borderRadius:4,cursor:'pointer'}}
        >Delete</button>
      </div>
    ) }
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

  // ============================================================================
  // ACTIVITY MANAGEMENT FUNCTIONS
  // ============================================================================
  
  async function loadActivities(){
    setActivitiesLoading(true)
    try{
      const params = new URLSearchParams()
      if (activityFilter) params.append('athlete_name', activityFilter)
      if (activitySort) params.append('sort_by', activitySort)
      if (activitySortOrder) params.append('sort_order', activitySortOrder)
      
      const url = `${API}/admin/raw-activities${params.toString() ? '?' + params.toString() : ''}`
      const res = await fetch(url)
      const j = await res.json()
      setActivities(j.activities || [])
    }catch(e){
      console.error('Failed to load activities', e)
      setActivities([])
    }finally{
      setActivitiesLoading(false)
    }
  }

  // Real-time filter: when activityFilter/sort options change, reload activities (debounced)
  useEffect(() => {
    const t = setTimeout(() => {
      loadActivities()
    }, 350)
    return () => clearTimeout(t)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activityFilter, activitySort, activitySortOrder])

  async function handleAddActivity(e){
    e && e.preventDefault()
    if (!newActivity.athlete_name || !newActivity.distance || !newActivity.start_date) {
      return alert('Please fill in: Athlete Name, Distance, and Start Date')
    }
    
    try{
      const payload = {
        athlete_name: newActivity.athlete_name,
        distance: Number(newActivity.distance) || 0,
        moving_time: Number(newActivity.moving_time) || 0,
        start_date: newActivity.start_date,
        type: newActivity.type || 'Run',
        name: newActivity.name || 'Manual Activity',
        elevation_gain: Number(newActivity.elevation_gain) || 0
      }
      
      const res = await fetch(`${API}/admin/raw-activities`, {
        method: 'POST',
        headers: {'content-type': 'application/json'},
        body: JSON.stringify(payload)
      })
      
      if (!res.ok) throw new Error('Failed to create activity')
      
      const result = await res.json()
      alert('Activity added successfully!')
      setShowAddActivity(false)
      setNewActivity({ athlete_name: '', distance: '', moving_time: '', start_date: '', type: 'Run', name: '', elevation_gain: '' })
      loadActivities()
    }catch(e){
      console.error('Failed to add activity', e)
      alert('Error adding activity: ' + e.message)
    }
  }

  async function handleDeleteActivity(id){
    if (!confirm('Delete this activity?')) return
    
    try{
      const res = await fetch(`${API}/admin/raw-activities/${id}`, { method: 'DELETE' })
      if (!res.ok) throw new Error('Failed to delete')
      
      alert('Activity deleted')
      loadActivities()
    }catch(e){
      console.error('Failed to delete activity', e)
      alert('Error deleting activity: ' + e.message)
    }
  }

  async function handleBulkImport(){
    const csvText = prompt('Paste CSV data (format: athlete_name,distance,moving_time,start_date,type,name,elevation_gain)')
    if (!csvText) return
    
    try{
      const lines = csvText.trim().split('\n')
      const activities = []
      
      for (let i = 0; i < lines.length; i++){
        const line = lines[i].trim()
        if (!line || line.startsWith('athlete_name')) continue // skip header
        
        const parts = line.split(',')
        if (parts.length < 4) continue
        
        activities.push({
          athlete_name: parts[0].trim(),
          distance: Number(parts[1]) || 0,
          moving_time: Number(parts[2]) || 0,
          start_date: parts[3]?.trim() || '',
          type: parts[4]?.trim() || 'Run',
          name: parts[5]?.trim() || 'Imported Activity',
          elevation_gain: Number(parts[6]) || 0
        })
      }
      
      if (activities.length === 0) return alert('No valid activities found in CSV')
      
      const res = await fetch(`${API}/admin/raw-activities/bulk`, {
        method: 'POST',
        headers: {'content-type': 'application/json'},
        body: JSON.stringify({ activities })
      })
      
      if (!res.ok) throw new Error('Bulk import failed')
      
      const result = await res.json()
      alert(`Imported ${result.imported} activities!${result.errors?.length ? ` (${result.errors.length} errors)` : ''}`)
      loadActivities()
    }catch(e){
      console.error('Bulk import failed', e)
      alert('Error importing: ' + e.message)
    }
  }

  async function runAggregation(){
    if (!confirm('Run aggregation to recalculate all summaries?')) return
    try{
      setAdminBusy(true)
      setAdminBusyMsg('Running aggregation…')
      const res = await fetch(`${API}/aggregate/weekly`, { method: 'POST' })
      if (!res.ok) throw new Error('Aggregation failed')
      const result = await res.json()
      alert(`Aggregation complete! Processed ${result.results?.length || 0} athletes`)
      // refresh lists
      await load()
      await loadActivities()
    }catch(e){
      console.error('Aggregation failed', e)
      alert('Error: ' + e.message)
    }finally{
      setAdminBusy(false)
      setAdminBusyMsg('')
    }
  }

  async function handleCleanup(){
    if (!confirm('This will remove duplicate raw activity records. Continue?')) return
    try{
      setAdminBusy(true)
      setAdminBusyMsg('Cleaning up duplicates…')
      const res = await fetch(`${API}/admin/cleanup-raw-activities`, { method: 'POST' })
      if (!res.ok) throw new Error('Cleanup failed')
      const j = await res.json()
      alert(`Cleanup complete. Deleted ${j.deleted || 0} duplicate activities, kept ${j.kept || 0}.`)
      await loadActivities()
    }catch(e){
      console.error('Cleanup failed', e)
      alert('Error: ' + e.message)
    }finally{
      setAdminBusy(false)
      setAdminBusyMsg('')
    }
  }


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
            <h2 style={{margin:0}}>AAC Admin</h2>
            <div className="header-actions">
              <a className="btn btn-ghost" href="/">Back to main</a>
              <a className="btn btn-ghost" href="/connect.html" target="_blank" rel="noopener noreferrer" style={{marginLeft:8}}>Connect</a>
              <button className="btn btn-ghost" onClick={activeTab === 'members' ? load : loadActivities} style={{marginLeft:8}}>Reload</button>
            </div>
          </header>

          {/* Tabs */}
          <div style={{display:'flex',gap:16,marginTop:20,borderBottom:'1px solid #e2e8f0'}}>
            <button 
              className={`tab-button ${activeTab === 'members' ? 'active' : ''}`}
              onClick={() => setActiveTab('members')}
              style={{
                padding:'12px 24px',
                background:'none',
                border:'none',
                borderBottom: activeTab === 'members' ? '2px solid #2563eb' : '2px solid transparent',
                color: activeTab === 'members' ? '#2563eb' : '#64748b',
                fontWeight: activeTab === 'members' ? 600 : 400,
                cursor:'pointer'
              }}
            >
              Members
            </button>
            <button 
              className={`tab-button ${activeTab === 'activities' ? 'active' : ''}`}
              onClick={() => setActiveTab('activities')}
              style={{
                padding:'12px 24px',
                background:'none',
                border:'none',
                borderBottom: activeTab === 'activities' ? '2px solid #2563eb' : '2px solid transparent',
                color: activeTab === 'activities' ? '#2563eb' : '#64748b',
                fontWeight: activeTab === 'activities' ? 600 : 400,
                cursor:'pointer'
              }}
            >
              Activities ({activities.length})
            </button>
          </div>

          {/* Members Tab */}
          {activeTab === 'members' && (
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
              <div style={{display:'flex',justifyContent:'flex-end',gap:8,marginTop:12}}>
                <button className="save-all" onClick={saveAll} disabled={savingAll}>{savingAll ? 'Saving…' : 'Save All'}</button>
              </div>
            </main>
          )}

          {/* Activities Tab */}
          {activeTab === 'activities' && (
            <main style={{marginTop:20}}>
              <div style={{display:'flex',justifyContent:'space-between',alignItems:'center',marginBottom:16}}>
                <h3 style={{margin:0}}>Activity Records ({activities.length})</h3>
                <div style={{display:'flex',gap:8}}>
                  <button className="btn" onClick={() => setShowAddActivity(!showAddActivity)}>
                    {showAddActivity ? 'Cancel' : '+ Add Activity'}
                  </button>
                  <button className="btn btn-ghost" onClick={handleBulkImport}>Import CSV</button>
                  <button className="btn btn-ghost" onClick={runAggregation}>Run Aggregation</button>
                  <button className="btn btn-ghost" onClick={handleCleanup}>Cleanup Duplicates</button>
                </div>
              </div>

              {/* Filters */}
              <div style={{display:'flex',gap:12,marginBottom:16,alignItems:'flex-end'}}>
                <div style={{flex:1}}>
                  <label style={{display:'block',fontSize:12,marginBottom:4}}>Filter by Athlete</label>
                  <input 
                    type="text"
                    className="admin-input" 
                    value={activityFilter}
                    onChange={e => setActivityFilter(e.target.value)}
                    placeholder="Search athlete name..."
                  />
                </div>
                <div>
                  <label style={{display:'block',fontSize:12,marginBottom:4}}>Sort By</label>
                  <select 
                    className="admin-input" 
                    value={activitySort}
                    onChange={e => setActivitySort(e.target.value)}
                  >
                    <option value="start_date">Date</option>
                    <option value="distance">Distance</option>
                    <option value="athlete_name">Athlete</option>
                    <option value="source">Source</option>
                  </select>
                </div>
                <div>
                  <label style={{display:'block',fontSize:12,marginBottom:4}}>Order</label>
                  <select 
                    className="admin-input" 
                    value={activitySortOrder}
                    onChange={e => setActivitySortOrder(e.target.value)}
                  >
                    <option value="desc">Descending</option>
                    <option value="asc">Ascending</option>
                  </select>
                </div>
                <button className="btn" onClick={loadActivities}>Apply</button>
              </div>

              {/* Add Activity Form */}
              {showAddActivity && (
                <div style={{background:'#f8fafc',padding:16,borderRadius:8,marginBottom:16}}>
                  <h4 style={{marginTop:0}}>Add Manual Activity</h4>
                  <form onSubmit={handleAddActivity} style={{display:'grid',gridTemplateColumns:'1fr 1fr',gap:12}}>
                    <div>
                      <label style={{display:'block',fontSize:12,marginBottom:4}}>Athlete Name *</label>
                      <select 
                        className="admin-input" 
                        value={newActivity.athlete_name} 
                        onChange={e => setNewActivity({...newActivity, athlete_name: e.target.value})}
                        required
                      >
                        <option value="">Select Athlete</option>
                        {athleteNames.map(name => (
                          <option key={name} value={name}>{name}</option>
                        ))}
                      </select>
                    </div>
                    <div>
                      <label style={{display:'block',fontSize:12,marginBottom:4}}>Activity Name</label>
                      <input 
                        className="admin-input" 
                        value={newActivity.name} 
                        onChange={e => setNewActivity({...newActivity, name: e.target.value})}
                        placeholder="e.g., John Doe"
                      />
                    </div>
                    <div>
                      <label style={{display:'block',fontSize:12,marginBottom:4}}>Distance (meters) *</label>
                      <input 
                        type="number" 
                        className="admin-input" 
                        value={newActivity.distance} 
                        onChange={e => setNewActivity({...newActivity, distance: e.target.value})}
                        placeholder="e.g., 5000"
                        required
                      />
                    </div>
                    <div>
                      <label style={{display:'block',fontSize:12,marginBottom:4}}>Moving Time (seconds)</label>
                      <input 
                        type="number" 
                        className="admin-input" 
                        value={newActivity.moving_time} 
                        onChange={e => setNewActivity({...newActivity, moving_time: e.target.value})}
                        placeholder="e.g., 1800"
                      />
                    </div>
                    <div>
                      <label style={{display:'block',fontSize:12,marginBottom:4}}>Start Date *</label>
                      <input 
                        type="date" 
                        className="admin-input" 
                        value={newActivity.start_date} 
                        onChange={e => setNewActivity({...newActivity, start_date: e.target.value})}
                        required
                      />
                    </div>
                    <div>
                      <label style={{display:'block',fontSize:12,marginBottom:4}}>Type</label>
                      <select 
                        className="admin-input" 
                        value={newActivity.type} 
                        onChange={e => setNewActivity({...newActivity, type: e.target.value})}
                      >
                        <option>Run</option>
                        <option>Walk</option>
                        <option>Hike</option>
                      </select>
                    </div>
                    <div>
                      <label style={{display:'block',fontSize:12,marginBottom:4}}>Activity Name</label>
                      <input 
                        className="admin-input" 
                        value={newActivity.name} 
                        onChange={e => setNewActivity({...newActivity, name: e.target.value})}
                        placeholder="e.g., Morning Run"
                      />
                    </div>
                    <div>
                      <label style={{display:'block',fontSize:12,marginBottom:4}}>Elevation Gain (m)</label>
                      <input 
                        type="number" 
                        className="admin-input" 
                        value={newActivity.elevation_gain} 
                        onChange={e => setNewActivity({...newActivity, elevation_gain: e.target.value})}
                        placeholder="e.g., 100"
                      />
                    </div>
                    <div style={{gridColumn:'1 / -1',display:'flex',gap:8,justifyContent:'flex-end'}}>
                      <button type="button" className="btn btn-ghost" onClick={() => setShowAddActivity(false)}>Cancel</button>
                      <button type="submit" className="btn">Add Activity</button>
                    </div>
                  </form>
                </div>
              )}

              {/* Activities List */}
              {activitiesLoading && <div className="loading">Loading activities…</div>}
              {!activitiesLoading && activities.length === 0 && <div className="empty">No activities found</div>}
              
              {!activitiesLoading && activities.length > 0 && (
                <div style={{overflowX:'auto'}}>
                  <table style={{width:'100%',borderCollapse:'collapse',fontSize:14}}>
                    <thead>
                      <tr style={{borderBottom:'2px solid #e2e8f0',textAlign:'left'}}>
                        <th style={{padding:12}}>#</th>
                        <th style={{padding:12}}>Date</th>
                        <th style={{padding:12}}>Athlete</th>
                        <th style={{padding:12}}>Name</th>
                        <th style={{padding:12}}>Distance</th>
                        <th style={{padding:12}}>Time</th>
                        <th style={{padding:12}}>Type</th>
                        <th style={{padding:12}}>Source</th>
                        <th style={{padding:12}}>Actions</th>
                      </tr>
                    </thead>
                    <tbody>
                      {activities.map((act, idx) => (
                        <tr key={act.id} style={{borderBottom:'1px solid #e2e8f0'}}>
                          <td style={{padding:12}}>{idx + 1}</td>
                          <td style={{padding:12}}>{act.start_date ? new Date(act.start_date).toLocaleDateString() : '-'}</td>
                          <td style={{padding:12}}>{act.athlete_name || act.athlete_id}</td>
                          <td style={{padding:12}}>{act.name || '-'}</td>
                          <td style={{padding:12}}>{((act.distance || 0) / 1000).toFixed(2)} km</td>
                          <td style={{padding:12}}>{
                            (() => {
                              const moving = Number(act.moving_time || 0)
                              const elapsed = Number(act.elapsed_time || 0)
                              const tsec = Math.max(moving, elapsed)
                              if (!tsec) return '-'
                              if (tsec >= 3600) {
                                const hrs = Math.floor(tsec / 3600)
                                const mins = Math.floor((tsec % 3600) / 60)
                                return `${hrs}h ${mins}m`
                              }
                              return `${Math.floor(tsec / 60)} min`
                            })()
                          }</td>
                          <td style={{padding:12}}>{act.type || 'Run'}</td>
                          <td style={{padding:12}}>
                            <span style={{
                              fontSize:11,
                              padding:'2px 6px',
                              borderRadius:4,
                              background: act.source === 'manual' ? '#fef3c7' : act.source === 'strava_api' ? '#dbeafe' : '#f1f5f9',
                              color: act.source === 'manual' ? '#92400e' : act.source === 'strava_api' ? '#1e40af' : '#475569'
                            }}>
                              {act.source || 'unknown'}
                            </span>
                          </td>
                          <td style={{padding:12}}>
                            <button 
                              onClick={() => handleDeleteActivity(act.id)} 
                              style={{
                                background:'#fee2e2',
                                color:'#991b1b',
                                border:'none',
                                padding:'4px 12px',
                                borderRadius:4,
                                fontSize:12,
                                cursor:'pointer'
                              }}
                            >
                              Delete
                            </button>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </main>
          )}

          {(loading || savingAll || activitiesLoading || adminBusy) && (
            <div className="page-loader">
              <div className="loader-box">
                <div className="spinner"/> 
                <div>
                  {adminBusy ? (adminBusyMsg || 'Working…') : (savingAll ? 'Saving changes…' : activitiesLoading ? 'Loading activities…' : 'Loading members…')}
                </div>
              </div>
            </div>
          )}
        </>
      )}
    </div>
  )
}
