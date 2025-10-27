// Helper script to generate CSV template for bulk import
// Run with: node scripts/generate-csv-template.js

const API = process.env.VITE_API_BASE || 'https://aac-strava-backend.onrender.com';

async function generateTemplate() {
  console.log('Fetching athletes from API...\n');
  
  try {
    const response = await fetch(`${API}/admin/athletes`);
    const data = await response.json();
    const athletes = data.rows || [];
    
    console.log(`Found ${athletes.length} athletes\n`);
    console.log('CSV Template:');
    console.log('=' .repeat(80));
    console.log('athlete_id,athlete_name,distance,moving_time,start_date,type,name,elevation_gain');
    
    athletes.forEach(athlete => {
      const id = athlete.id;
      const name = athlete.name || athlete.nickname || 'Unknown';
      // Template with example values - adjust as needed
      console.log(`${id},${name},5000,1800,2025-09-13,Run,September Activity,100`);
    });
    
    console.log('=' .repeat(80));
    console.log('\nInstructions:');
    console.log('1. Copy the CSV lines above');
    console.log('2. Update the distance, moving_time, start_date, etc. for each athlete');
    console.log('3. Paste into Admin → Activities → Import CSV');
    console.log('\nField Reference:');
    console.log('- distance: meters (5000 = 5km)');
    console.log('- moving_time: seconds (1800 = 30 minutes)');
    console.log('- start_date: YYYY-MM-DD format');
    console.log('- type: Run, Walk, or Hike');
    console.log('- name: Any description');
    console.log('- elevation_gain: meters\n');
    
  } catch (error) {
    console.error('Error:', error.message);
  }
}

generateTemplate();
