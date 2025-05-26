import { google } from 'googleapis';

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
    // Initialize Google Sheets API
    const auth = new google.auth.JWT(
      process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL,
      null,
      process.env.GOOGLE_PRIVATE_KEY?.replace(/\\n/g, '\n'),
      ['https://www.googleapis.com/auth/spreadsheets.readonly']
    );

    const sheets = google.sheets({ version: 'v4', auth });
    const spreadsheetId = process.env.GOOGLE_SHEET_ID || '1pLsxrfU5NgHF4aNLXNnCCvGgBvKO4EKjb44iiVvUp5Q';

    // Get data from FORMATTED_TRANSACTIONS sheet
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId,
      range: 'FORMATTED_TRANSACTIONS!A1:L1000', // Get headers + data
    });

    const rows = response.data.values;
    
    if (!rows || rows.length === 0) {
      return res.status(200).json({ 
        transactions: [], 
        message: 'No data found in sheets' 
      });
    }

    // Extract headers and data
    const headers = rows[0];
    const dataRows = rows.slice(1);

    // Transform data to match frontend expectations
    const transactions = dataRows
      .filter(row => row && row.length > 0 && row[0]) // Filter empty rows
      .map((row, index) => {
        return {
          id: index + 1,
          timestamp: row[0] || '', // Date & Time
          platform: row[1] || '', // Platform
          type: row[2]?.toLowerCase() || '', // Type (deposit/withdrawal)
          asset: row[3] || '', // Asset
          amount: row[4] || '0', // Amount
          amount_aed: row[5]?.replace(/[^\d.-]/g, '') || '0', // AED Value (remove currency symbols)
          rate: row[6] || '0', // Rate
          client: row[7] || '', // Client
          remarks: row[8] || '', // Remarks
          from_address: row[9] || '', // From Address
          to_address: row[10] || '', // To Address
          tx_id: row[11] || '', // TX ID
          status: 'Completed', // Default status
          network: getNetworkFromAsset(row[3]) // Derive network from asset
        };
      });

    // Generate mock wallet balances (this would come from real APIs later)
    const walletBalances = {
      "Binance (GC)": { BTC: 0.25, ETH: 2.1, USDT: 15000, SOL: 0 },
      "Binance (Main)": { BTC: 0.1, ETH: 5.5, USDT: 25000, SOL: 45 },
      "Binance (CV)": { BTC: 0.05, ETH: 1.2, USDT: 8000, SOL: 20 },
      "ByBit (CV)": { BTC: 0, ETH: 0.8, USDT: 12000, SOL: 125 },
      "Bitcoin Wallet": { BTC: 0.45 },
      "Ethereum Wallet": { ETH: 3.25, USDT: 5000 },
      "TRON Wallet": { USDT: 18000, TRX: 50000 },
      "Solana Wallet": { SOL: 275 },
      "BEP20 Wallet": { BNB: 15, USDT: 3000 }
    };

    res.status(200).json({
      transactions,
      walletBalances,
      lastUpdated: new Date().toISOString(),
      totalRows: transactions.length
    });

  } catch (error) {
    console.error('Google Sheets API Error:', error);
    
    // Return error details for debugging
    res.status(500).json({
      error: 'Failed to fetch data from Google Sheets',
      details: error.message,
      stack: process.env.NODE_ENV === 'development' ? error.stack : undefined
    });
  }
}

// Helper function to determine network from asset
function getNetworkFromAsset(asset) {
  const networkMap = {
    'BTC': 'BTC',
    'ETH': 'ETH',
    'USDT': 'TRC20', // Default for USDT
    'TRX': 'TRON',
    'SOL': 'SOL',
    'BNB': 'BEP20'
  };
  
  return networkMap[asset] || 'Unknown';
}
