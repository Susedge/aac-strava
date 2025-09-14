# AAC Strava

Starter project: Strava club leaderboard with Firebase-backed weekly data.

Overview
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

	- The backend exposes POST /aggregate/weekly which will iterate stored athletes and fetch their activities from Strava for the last 7 days and write to Firestore `activities` collection (and save summary athlete records in `summary_athletes`).
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
