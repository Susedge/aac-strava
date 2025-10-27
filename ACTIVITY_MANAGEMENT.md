# Activity Management System

## Overview

This system solves the Strava API limitation by allowing manual activity management alongside automatic Strava data fetching.

## Key Features

### 1. **Persistent Activity Storage**
- All activities (Strava + Manual) are stored in `raw_activities` Firestore collection
- Activities are never lost, even if Strava API doesn't return them
- Each activity has a unique ID and source tracking

### 2. **Manual Activity Management**
- Add individual activities via Admin UI
- Bulk import via CSV
- Edit and delete manual activities
- View all activities with source labels

### 3. **Combined Aggregation**
- Aggregation combines Strava API activities + manual activities
- Automatically stores new Strava activities for future use
- No data loss between aggregations

## How It Works

### Data Flow

```
1. Strava API Activities → Stored in raw_activities (source: 'strava_api')
2. Manual Activities → Stored in raw_activities (source: 'manual')
3. Aggregation → Combines both sources → Updates summary
```

### Firestore Collections

**`raw_activities`** - Individual activity records
```javascript
{
  id: "auto-generated or strava_{id}",
  athlete_id: "12345678",
  athlete_name: "John Doe",
  distance: 5000,              // meters
  moving_time: 1800,           // seconds
  start_date: "2025-09-13",
  type: "Run",
  name: "Morning Run",
  elevation_gain: 100,         // meters
  source: "manual",            // or "strava_api"
  created_at: 1729987654000,
  updated_at: 1729987654000
}
```

**`activities`** - Aggregated summaries per athlete
```javascript
{
  id: "athlete_id",
  athlete: { ... },
  summary: {
    distance: 45000,
    count: 9,
    longest: 10000,
    avg_pace: 360,
    elev_gain: 500,
    updated_at: 1729987654000
  }
}
```

## Using the Admin UI

### Access Admin Panel
1. Go to `/admin` or `/admin.html`
2. Enter password: `susedge`
3. Click "Activities" tab

### Add Single Activity

1. Click "+ Add Activity"
2. Fill in the form:
   - **Athlete ID*** (required) - Get from athlete's Strava profile
   - **Distance*** (required) - In meters (e.g., 5000 for 5km)
   - **Start Date*** (required) - YYYY-MM-DD
   - **Athlete Name** - Optional, for display
   - **Moving Time** - In seconds
   - **Type** - Run, Walk, Hike
   - **Activity Name** - Optional description
   - **Elevation Gain** - In meters
3. Click "Add Activity"

### Bulk Import from CSV

1. Click "Import CSV"
2. Paste CSV data in format:
```csv
athlete_id,athlete_name,distance,moving_time,start_date,type,name,elevation_gain
12345678,John Doe,5000,1800,2025-09-13,Run,Morning Run,100
87654321,Jane Smith,10000,3600,2025-09-14,Run,Long Run,250
```
3. Press OK

### Delete Manual Activity

- Manual activities show a "Delete" button
- Strava API activities cannot be deleted (they'll reappear on next fetch)

### Run Aggregation

1. Click "Run Aggregation" button
2. This recalculates all summaries using:
   - Latest Strava API data
   - All manual activities
   - Previously stored activities

## API Endpoints

### Get Activities
```
GET /admin/raw-activities
GET /admin/raw-activities?athlete_id=12345678
```

### Add Activity
```
POST /admin/raw-activities
Content-Type: application/json

{
  "athlete_id": "12345678",
  "athlete_name": "John Doe",
  "distance": 5000,
  "moving_time": 1800,
  "start_date": "2025-09-13",
  "type": "Run",
  "name": "Morning Run",
  "elevation_gain": 100
}
```

### Update Activity
```
PUT /admin/raw-activities/{id}
Content-Type: application/json

{
  "distance": 6000
}
```

### Delete Activity
```
DELETE /admin/raw-activities/{id}
```

### Bulk Import
```
POST /admin/raw-activities/bulk
Content-Type: application/json

{
  "activities": [
    { ... },
    { ... }
  ]
}
```

## Solving the September 13 Problem

### Option 1: Manual Entry (Recommended)

1. Get athlete totals from September 13 - Oct 27 from Strava web
2. For each athlete, calculate their total distance/activities in that period
3. Create ONE manual activity per athlete with their September totals:

```csv
athlete_id,athlete_name,distance,moving_time,start_date,type,name,elevation_gain
12345678,John Doe,45000,14400,2025-09-13,Run,September Total,500
```

4. Run aggregation
5. Set `AGGREGATION_START_DATE=2025-09-13` in render.yaml
6. Deploy

### Option 2: Individual Activity Entry

1. Export each athlete's activities from Strava (if available)
2. Enter each activity individually via CSV bulk import
3. Run aggregation

## Benefits

✅ **No Data Loss** - Activities stored permanently in Firestore
✅ **Historical Data** - Can add activities from any date
✅ **Audit Trail** - Source tracking shows manual vs API activities
✅ **Flexibility** - Add/edit/delete as needed
✅ **Combined Totals** - Aggregation combines all sources

## Notes

- **Athlete IDs**: Must match Strava athlete IDs for proper aggregation
- **Dates**: Use ISO format (YYYY-MM-DD) or full ISO timestamp
- **Distance**: Always in meters (5000 = 5km)
- **Moving Time**: Always in seconds (1800 = 30 minutes)
- **Deduplication**: Strava activities use `strava_{id}` as document ID to prevent duplicates
- **Manual Activities**: Use auto-generated IDs, can be deleted anytime

## Deployment

After making changes:

```powershell
git add .
git commit -m "Add activity management system with manual entry and bulk import"
git push
```

Render will automatically redeploy the backend with new endpoints.
