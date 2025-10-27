# Quick Start: Fixing Missing September Activities

## Problem Summary
Strava's Club Activities API only returns ~2-4 weeks of recent activities. Your September 13+ data is not available via the API.

## Solution Implemented ✅

1. **Persistent Storage** - All activities now stored in Firestore (`raw_activities`)
2. **Manual Entry** - Admin UI to add historical activities
3. **Bulk Import** - CSV import for multiple activities
4. **Combined Aggregation** - Merges Strava API + manual activities

## Quick Steps to Fix Your Data

### Step 1: Deploy the Changes

```powershell
cd c:\Users\AAC\Documents\GitHub\aac-strava
git add .
git commit -m "Add activity management system to handle missing historical data"
git push
```

Wait for Render to deploy (~2-3 minutes).

### Step 2: Add September Historical Data

**Option A: Simple (One Entry Per Athlete)**

1. Go to https://aac-strava-backend.onrender.com/admin
2. Password: `susedge`
3. Click "Activities" tab
4. Click "Import CSV"
5. Paste this format (get totals from Strava web):

```csv
athlete_id,athlete_name,distance,moving_time,start_date,type,name,elevation_gain
12345678,John Doe,145000,50400,2025-09-13,Run,September Total,1200
87654321,Jane Smith,95000,34200,2025-09-13,Run,September Total,800
```

**Option B: Detailed (Individual Activities)**

If you have detailed activity data, import each activity separately:

```csv
athlete_id,athlete_name,distance,moving_time,start_date,type,name,elevation_gain
12345678,John Doe,5000,1800,2025-09-13,Run,Morning Run,100
12345678,John Doe,10000,3600,2025-09-15,Run,Long Run,250
12345678,John Doe,5000,1800,2025-09-17,Run,Evening Run,80
```

### Step 3: Run Aggregation

1. Still in Admin → Activities tab
2. Click "Run Aggregation" button
3. Wait for completion message

### Step 4: Verify Data

1. Go back to main leaderboard: https://aac-strava-backend.onrender.com
2. Check that athlete totals now include September data

## How to Get Athlete IDs

### Method 1: From Strava Profile URL
1. Go to athlete's Strava profile
2. URL will be: `https://www.strava.com/athletes/12345678`
3. The number is the athlete ID

### Method 2: From Your Admin Panel
1. Go to `/admin`
2. Click "Members" tab
3. The ID column shows each athlete's ID

### Method 3: From API
```
GET https://aac-strava-backend.onrender.com/admin/athletes
```

## CSV Format Reference

### Required Fields
- `athlete_id` - Strava athlete ID (number)
- `distance` - In meters (5000 = 5km, 10000 = 10km)
- `start_date` - YYYY-MM-DD format

### Optional Fields
- `athlete_name` - Display name
- `moving_time` - Seconds (1800 = 30 min, 3600 = 1 hour)
- `type` - Run, Walk, Hike (default: Run)
- `name` - Activity description
- `elevation_gain` - Meters

### Conversion Helper
- **Distance**: km × 1000 = meters (e.g., 5.5km = 5500m)
- **Time**: minutes × 60 = seconds (e.g., 45min = 2700s)

## Example Scenarios

### Scenario 1: Simple Baseline
You just want totals since September 13:

1. Check each athlete's Strava profile for total distance Sept 13 - Oct 27
2. Create one entry per athlete with their total
3. Import via CSV
4. Run aggregation

### Scenario 2: Detailed History
You have individual activity data:

1. Export or manually collect each activity
2. Format as CSV with all details
3. Bulk import
4. Run aggregation

### Scenario 3: Ongoing Manual Entries
For activities that don't appear via API:

1. Use "+ Add Activity" button in Admin UI
2. Fill in the form
3. Click "Add Activity"
4. Run aggregation when ready

## Troubleshooting

### "Activity added but not showing in totals"
→ Click "Run Aggregation" button

### "Wrong athlete ID"
→ Check Strava profile URL or `/admin/athletes` endpoint

### "Import failed"
→ Check CSV format matches exactly (no extra spaces)

### "Duplicate activities"
→ Manual activities can be deleted from Activities tab

## Automation

After initial setup, the system will:
1. ✅ Automatically store new Strava activities
2. ✅ Preserve historical manual entries
3. ✅ Combine both on every aggregation
4. ✅ Never lose data

## Need Help?

Check these files for details:
- `ACTIVITY_MANAGEMENT.md` - Full documentation
- `STRAVA_API_LIMITATION.md` - Why this was needed

## Summary

🎯 **You can now:**
- Add September 13+ activities manually
- Bulk import historical data
- Keep all activity data permanently
- Never lose data due to API limitations

The system will automatically combine Strava API data (recent ~4 weeks) with your manual historical entries to show complete totals! 🎉
