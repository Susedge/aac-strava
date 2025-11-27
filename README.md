# AAC Strava

Starter project: Strava club leaderboard with Firebase-backed weekly data.

## Live Deployment

- **Frontend**: https://aac-tracker.web.app (Firebase Hosting)
- **Backend**: https://aac-strava-backend.onrender.com (Render)
- **Database**: Firebase Firestore

## Overview
- Backend: Node + Express. Handles Strava OAuth, token exchange, and leaderboard aggregation.
- Frontend: Vite + React. Sign-in with Strava and animated leaderboard UI.
- Database: Firebase Firestore for storing athlete tokens and weekly totals.

Setup

1. Strava

	- Create a Strava developer application at https://developers.strava.com/apps
	- Set the Authorization Callback Domain to the host where your frontend runs (e.g. http://localhost:5173)
	- Note your Client ID and Client Secret

2. Firebase

	- Create a Firebase project and enable Firestore
	- Create a service account and download the JSON file
	- Place it in `backend/service-account.json` or set `GOOGLE_APPLICATION_CREDENTIALS` in `backend/.env`

3. Environment

	- Copy `backend/.env.example` to `backend/.env` and fill STRAVA_CLIENT_ID, STRAVA_CLIENT_SECRET and STRAVA_REDIRECT_URI
	- Copy `frontend/.env.example` to `frontend/.env` and set `VITE_API_BASE` to your backend URL (http://localhost:4000)

Run locally

Backend

	cd backend; npm install
	npm run dev

Frontend

	cd frontend; npm install
	npm run dev

Populate aggregated data

	- The backend exposes POST /aggregate/weekly which will iterate stored athletes and fetch their activities from Strava for the last 7 days and write to Firestore `activities` collection (and save summary athlete records in `summary_athletes`). This aggregation step produces a cached leaderboard used by the frontend — the frontend reads the pre-computed `activities` documents instead of recomputing from raw_activities on every page load so the leaderboard is fast.
	- Important: aggregation stores raw activity documents in `raw_activities` in an append-only fashion. The aggregator will only update an existing `raw_activities` document when there is a definitive match (for example the Strava activity id or an exact athlete+start_date match). For fuzzy/near matches aggregation will not overwrite or replace existing documents — it will create new documents instead to preserve duplicates and avoid accidental data loss.
	 - Admin: Cleanup duplicates now supports a preview (dry-run) and uses stricter normalization to detect immediate duplicates.
		 - POST /admin/cleanup-raw-activities?dry_run=1 will return a preview of duplicate groups and which documents would be deleted without performing deletion.
		 - Cleanup groups are derived from athlete name (prefers name over id when available), activity name, distance (normalized to 1 decimal), moving_time (seconds), elapsed_time (seconds), and elevation (1 decimal). This balances robustness (avoid false negatives from float formatting) and safety (only immediate duplicates are targeted).
	- Performance optimization: each aggregation now writes a single snapshot document into `leaderboard_snapshots/latest` (and a timestamped archive). The frontend will prefer this single-document snapshot for leaderboard reads to avoid heavy reads during page load.
	- Call it manually (e.g. using curl or Postman) or wire it to a scheduler (Cloud Functions, cron, GitHub Actions).

Notes

	- This starter stores Strava token responses in Firestore under `athletes`, lightweight athlete summaries in `summary_athletes`, and aggregated results in `activities`.
	- The frontend shows the `activities` collection data in an animated leaderboard.

Admin token refresh

	- If the admin Strava token expires you can refresh it without re-running the OAuth flow using the backend endpoint POST /admin/refresh.
	- This will read the stored refresh_token from Firestore (admin/strava), call Strava's token endpoint, persist the refreshed tokens back to admin/strava, and return the refreshed response.

	PowerShell example:

	$resp = Invoke-RestMethod -Uri 'http://localhost:4000/admin/refresh' -Method Post -Verbose
	$resp | ConvertTo-Json -Depth 5

## Deployment

### Deploy to Production

**Backend (Render)**
- Backend automatically deploys from GitHub when you push to the main branch
- Environment variables are configured in Render dashboard
- Manual deploy: Go to Render dashboard → Your service → Manual Deploy

**Frontend (Firebase Hosting)**
```bash
cd frontend
npm run build
firebase deploy --only hosting
```

Or deploy via GitHub Actions (automatic on push to main):
- GitHub Actions workflow deploys automatically when you push changes
- Configured in `.github/workflows/deploy.yml`

### Switch Between Localhost and Production

**For Local Development:**

1. **Backend Environment** (`backend/.env`):
```env
STRAVA_REDIRECT_URI=http://localhost:5173/auth/callback
```

2. **Frontend Environment** (create `frontend/.env.local`):
```env
VITE_API_BASE=http://localhost:4000
```

3. **Strava App Settings**:
   - Authorization Callback Domain: `localhost:5173`

**For Production:**

1. **Backend Environment** (Render dashboard):
```env
STRAVA_REDIRECT_URI=https://aac-tracker.web.app/auth/callback
```

2. **Frontend Environment** (`frontend/.env.production`):
```env
VITE_API_BASE=https://aac-strava-backend.onrender.com
```

3. **Strava App Settings**:
   - Authorization Callback Domain: `aac-tracker.web.app`

**Note**: The frontend automatically uses `.env.production` when building for production (`npm run build`) and `.env.local` or `.env` for development (`npm run dev`).
