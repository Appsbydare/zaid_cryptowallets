// Test endpoint for last week's data extraction
export default async function handler(req, res) {
  // Set CORS headers
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') {
    res.status(200).end();
    return;
  }

  try {
    console.log('🧪 Starting LAST WEEK test extraction...');
    
    // Calculate last week's date range
    const now = new Date();
    const lastWeekStart = new Date(now);
    lastWeekStart.setDate(now.getDate() - 7);
    
    const startDate = lastWeekStart.toISOString();
    const endDate = now.toISOString();
    
    console.log(`📅 Test period: ${startDate} to ${endDate}`);
    
    // Call the main crypto-to-sheets API with last week's date
    const response = await fetch(`${process.env.VERCEL_URL || 'http://localhost:3000'}/api/crypto-to-sheets`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        startDate: startDate,
        testMode: true
      })
    });
    
    const result = await response.json();
    
    console.log('🧪 Test extraction completed:', result);
    
    res.status(200).json({
      success: true,
      testPeriod: {
        start: startDate,
        end: endDate
      },
      result: result,
      message: 'Last week test extraction completed'
    });
    
  } catch (error) {
    console.error('❌ Error in test extraction:', error);
    res.status(500).json({
      success: false,
      error: error.message,
      message: 'Test extraction failed'
    });
  }
} 