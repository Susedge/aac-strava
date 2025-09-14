import React, { useEffect, useState } from 'react';

function FriendlyError({ message, details }) {
  return (
    <div className="admin-card" style={{ padding: '18px' }}>
      <h2>Member Runs</h2>
      <p className="muted">No activities available — server returned an unexpected response when fetching persisted activities.</p>
      <div style={{ marginTop: 12 }}>
        <strong>{message}</strong>
        {details ? <div style={{ marginTop: 8, whiteSpace: 'pre-wrap' }}>{details}</div> : null}
      </div>
    </div>
  );
}

export default function User() {
  const [activities, setActivities] = useState(null);
  const [error, setError] = useState(null);
  const [usedFallback, setUsedFallback] = useState(false);

  useEffect(() => {
    // Try extracting id from path /user/{id} (decodeURIComponent), otherwise fall back to query
    let id = null;
    try {
      const p = (window.location && window.location.pathname) || '';
      const parts = p.split('/').filter(Boolean);
      // last part when path starts with 'user'
      if (parts.length >= 2 && parts[0] === 'user') {
        id = decodeURIComponent(parts.slice(1).join('/')) || null;
      }
    } catch (e) { /* ignore */ }
    if (!id) {
      const params = new URLSearchParams(window.location.search || '');
      id = params.get('id') || params.get('athlete_id') || null;
    }

    const base = window.__API || '';

    async function fetchPersisted() {
      const url = `${base}/debug/activities`;
      try {
        const res = await fetch(url);
        const content = await res.text();
        try {
          const json = JSON.parse(content);
          return json;
        } catch (err) {
          throw new Error('server returned non-JSON response.');
        }
      } catch (err) {
        throw err;
      }
    }

    async function fetchClubThenPersisted() {
      const clubUrl = `${base}/debug/club-activities`;
      try {
        const r = await fetch(clubUrl);
        const text = await r.text();
        try {
          const json = JSON.parse(text);
          return json;
        } catch (err) {
          // fallback to persisted
          const persisted = await fetchPersisted();
          setUsedFallback(true);
          return persisted;
        }
      } catch (err) {
        // fallback
        const persisted = await fetchPersisted();
        setUsedFallback(true);
        return persisted;
      }
    }

    (async () => {
      try {
        const data = await fetchClubThenPersisted();
        if (!Array.isArray(data) || data.length === 0) {
          setActivities([]);
          if (Array.isArray(data) && data.length === 0) {
            // no results but valid
            return;
          }
          // otherwise show friendly message
          setError({ message: 'Failed to fetch activities: server returned unexpected data.' });
          return;
        }

  // If id filter provided, filter client-side
  const filtered = id ? data.filter(a => String(a.athlete_id || a.athlete?.id) === String(id) || String(a.id) === String(id) || `name:${(a.athlete && ((a.athlete.firstname||a.athlete.first_name||'') + ' ' + (a.athlete.lastname||a.athlete.last_name||'')).trim())}` === String(id) || `username:${(a.athlete && (a.athlete.username||'') )}` === String(id)) : data;
        setActivities(filtered);
      } catch (err) {
        setError({ message: 'Failed to fetch activities: ' + (err.message || 'unknown') });
        setActivities([]);
      }
    })();
  }, []);

  if (error) {
    return <FriendlyError message={error.message} />;
  }

  if (activities === null) {
    return (
      <div className="admin-card">
        <h2>Member Runs</h2>
        <p className="muted">Loading activities…</p>
      </div>
    );
  }

  if (activities.length === 0) {
    return (
      <div className="admin-card">
        <h2>Member Runs</h2>
        <p className="muted">No activities available.</p>
        {usedFallback ? (
          <p className="muted" style={{ marginTop: 8 }}>
            — Note: the club endpoint was unavailable or returned unexpected data; showing persisted activities only.
          </p>
        ) : null}
      </div>
    );
  }

  return (
    <div className="admin-card">
      <a href="/" style={{ display: 'inline-block', marginBottom: 8 }}>← Back</a>
      <h2>Member Runs</h2>
      {usedFallback ? (
        <p className="muted">Showing persisted activities because the club endpoint was unavailable.</p>
      ) : null}
      <table className="simple-table" style={{ marginTop: 12 }}>
        <thead>
          <tr>
            <th>Date</th>
            <th>Member</th>
            <th>Distance</th>
            <th>Sport</th>
            <th>Title</th>
          </tr>
        </thead>
        <tbody>
          {activities.map(a => (
            <tr key={a.id}>
              <td>{new Date(a.start_date || a.start_date_local || a.date || Date.now()).toLocaleString()}</td>
              <td>{a.athlete?.name || a.name || a.display_name || a.athlete_id || ''}</td>
              <td>{(a.distance ? (a.distance / 1000).toFixed(2) + ' km' : a.distance_km ? a.distance_km + ' km' : '')}</td>
              <td>{a.type || a.sport}</td>
              <td>{a.name || a.title || ''}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
