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
    // Google Sheets configuration
    const spreadsheetId = process.env.GOOGLE_SHEET_ID || '1pLsxrfU5NgHF4aNLXNnCCvGgBvKO4EKjb44iiVvUp5Q';
    const range = 'FORMATTED_TRANSACTIONS!A1:L1000';
    
    // Get access token using service account
    const accessToken = await getAccessToken();
    
    // Fetch data from Google Sheets using REST API
    const sheetsUrl = `https://sheets.googleapis.com/v4/spreadsheets/${spreadsheetId}/values/${range}`;
    
    const response = await fetch(sheetsUrl, {
      headers: {
        'Authorization': `Bearer ${accessToken}`,
        'Content-Type': 'application/json'
      }
    });

    if (!response.ok) {
      throw new Error(`Google Sheets API error: ${response.status} ${response.statusText}`);
    }

    const data = await response.json();
    const rows = data.values || [];
    
    if (rows.length === 0) {
      return res.status(200).json({ 
        transactions: [], 
        walletBalances: {},
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
      totalRows: transactions.length,
      source: 'Google Sheets Direct API'
    });

  } catch (error) {
    console.error('Google Sheets API Error:', error);
    
    // Return error details for debugging
    res.status(500).json({
      error: 'Failed to fetch data from Google Sheets',
      details: error.message,
      stack: process.env.NODE_ENV === 'development' ? error.stack : undefined,
      timestamp: new Date().toISOString()
    });
  }
}

// Get Google OAuth2 access token using service account
async function getAccessToken() {
  const clientEmail = process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL;
  const privateKey = process.env.GOOGLE_PRIVATE_KEY?.replace(/\\n/g, '\n');
  
  if (!clientEmail || !privateKey) {
    throw new Error('Missing Google service account credentials');
  }

  // Create JWT payload
  const now = Math.floor(Date.now() / 1000);
  const payload = {
    iss: clientEmail,
    scope: 'https://www.googleapis.com/auth/spreadsheets.readonly',
    aud: 'https://oauth2.googleapis.com/token',
    exp: now + 3600,
    iat: now
  };

  // Create JWT token
  const token = await createJWT(payload, privateKey);
  
  // Exchange JWT for access token
  const response = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
    },
    body: new URLSearchParams({
      grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer',
      assertion: token
    })
  });

  if (!response.ok) {
    const error = await response.text();
    throw new Error(`OAuth2 error: ${response.status} ${error}`);
  }

  const data = await response.json();
  return data.access_token;
}

// Simple JWT creation without external libraries
async function createJWT(payload, privateKey) {
  // Base64 URL encode
  const base64URLEncode = (obj) => {
    return Buffer.from(JSON.stringify(obj))
      .toString('base64')
      .replace(/\+/g, '-')
      .replace(/\//g, '_')
      .replace(/=/g, '');
  };

  // JWT Header
  const header = base64URLEncode({
    alg: 'RS256',
    typ: 'JWT'
  });

  // JWT Payload
  const encodedPayload = base64URLEncode(payload);

  // Create signature (simplified - in production should use proper crypto)
  const data = `${header}.${encodedPayload}`;
  
  // For Vercel, we'll use the Web Crypto API
  const encoder = new TextEncoder();
  const keyData = encoder.encode(privateKey);
  const signData = encoder.encode(data);
  
  // Import the private key
  const cryptoKey = await crypto.subtle.importKey(
    'pkcs8',
    keyData,
    {
      name: 'RSASSA-PKCS1-v1_5',
      hash: 'SHA-256'
    },
    false,
    ['sign']
  );

  // Sign the data
  const signature = await crypto.subtle.sign(
    'RSASSA-PKCS1-v1_5',
    cryptoKey,
    signData
  );

  // Convert signature to base64url
  const signatureBase64 = Buffer.from(signature)
    .toString('base64')
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=/g, '');

  return `${data}.${signatureBase64}`;
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
