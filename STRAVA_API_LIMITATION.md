# Strava Club Activities API Limitation

## The Problem

You're experiencing missing activities from September 13 and earlier because **Strava's `/clubs/{id}/activities` endpoint has an undocumented limitation**.

### What We Know

1. **Current Results**: Only 184 activities across 4 pages (200 per page would be 800 max, but you have 184)
2. **Missing Data**: Activities from September 13 and before are not returned
3. **Recent Activities Work**: New activities appear immediately

### Strava API Limitation

According to Strava's API behavior and community reports:

**The `/api/v3/clubs/{id}/activities` endpoint:**
- Does NOT return all historical club activities
- Only returns activities from approximately the **last 2-4 weeks**
- The exact time window is not documented by Strava
- This is different from personal athlete activities (which can go back much further)

### Why This Happens

Strava's club activities feed is designed for:
- Showing recent club activity
- Not archiving historical data
- Performance optimization on Strava's end

### Verification

Your club shows 184 activities, which suggests:
- Activities newer than ~mid-October are being returned
- Older activities (September 13 and before) are outside Strava's club activities window
- The API simply doesn't expose them, even though they exist in the Strava app

## Solutions

### Option 1: Use What's Available (Recommended for Now)
- Accept that only recent activities (~2-4 weeks) are available via club API
- Set `AGGREGATION_START_DATE` to a more recent date (e.g., October 1, 2025)
- Document that the challenge started with available data

### Option 2: Manual Data Entry
- Manually record athlete totals from September 13 - October (from Strava web)
- Store these as initial values in Firestore
- Add them to the aggregated totals

### Option 3: Request OAuth from Each Athlete
- Have each athlete connect their individual Strava account
- Fetch their personal activities (which go back much further)
- This gives you full historical data but requires user participation

## Testing Endpoints

### Test 1: Current endpoint (recent activities only)
```
GET https://aac-strava-backend.onrender.com/debug/club-activities
```

### Test 2: New endpoint with date range support
```
GET https://aac-strava-backend.onrender.com/debug/club-activities-range?start_date=2025-09-13
```

### Test 3: Check activities from different time periods
```
GET https://aac-strava-backend.onrender.com/debug/club-activities-range?start_date=2025-10-01
GET https://aac-strava-backend.onrender.com/debug/club-activities-range?start_date=2025-09-13&before=2025-10-01
```

## Recommendation

**The club activities API limitation is a Strava platform constraint, not a bug in your code.**

Your best options are:
1. Adjust the challenge start date to when data is available (~October 1)
2. Manually enter September data as baseline values
3. Switch to individual athlete OAuth (more setup, but gets all historical data)
