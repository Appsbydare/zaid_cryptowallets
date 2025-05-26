// File: api/fetch-exchange-data.js
// DEBUG VERSION - Replace your current file with this

import crypto from 'crypto';

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
    const debugInfo = {
      environmentCheck: {},
      apiResults: {},
      errors: []
    };

    // Check environment variables
    debugInfo.environmentCheck = {
      BINANCE_GC_API_KEY: !!process.env.BINANCE_GC_API_KEY,
      BINANCE_GC_API_SECRET: !!process.env.BINANCE_GC_API_SECRET,
      BINANCE_MAIN_API_KEY: !!process.env.BINANCE_MAIN_API_KEY,
      BINANCE_MAIN_API_SECRET: !!process.env.BINANCE_MAIN_API_SECRET,
      BINANCE_CV_API_KEY: !!process.env.BINANCE_CV_API_KEY,
      BINANCE_CV_API_SECRET: !!process.env.BINANCE_CV_API_SECRET,
      BYBIT_API_KEY: !!process.env.BYBIT_API_KEY,
      BYBIT_API_SECRET: !!process.env.BYBIT_API_SECRET
    };

    const allTransactions = [];

    // Binance Accounts Configuration
    const BINANCE_ACCOUNTS = [
      {
        name: "Binance (GC)",
        apiKey: process.env.BINANCE_GC_API_KEY,
        apiSecret: process.env.BINANCE_GC_API_SECRET
      },
      {
        name: "Binance (Main)",
        apiKey: process.env.BINANCE_MAIN_API_KEY,
        apiSecret: process.env.BINANCE_MAIN_API_SECRET
      },
      {
        name: "Binance (CV)",
        apiKey: process.env.BINANCE_CV_API_KEY,
        apiSecret: process.env.BINANCE_CV_API_SECRET
      }
    ];

    const BYBIT_CONFIG = {
      name: "ByBit (CV)",
      apiKey: process.env.BYBIT_API_KEY,
      apiSecret: process.env.BYBIT_API_SECRET
    };

    // Test each Binance account
    for (const account of BINANCE_ACCOUNTS) {
      if (!account.apiKey || !account.apiSecret) {
        debugInfo.errors.push(`${account.name}: Missing API credentials`);
        debugInfo.apiResults[account.name] = "SKIPPED - No credentials";
        continue;
      }

      console.log(`Testing ${account.name}...`);
      const result = await testBinanceAccount(account);
      debugInfo.apiResults[account.name] = result;
      
      if (result.success) {
        allTransactions.push(...result.transactions);
      } else {
        debugInfo.errors.push(`${account.name}: ${result.error}`);
      }
    }

    // Test ByBit
    if (!BYBIT_CONFIG.apiKey || !BYBIT_CONFIG.apiSecret) {
      debugInfo.errors.push("ByBit: Missing API credentials");
      debugInfo.apiResults["ByBit"] = "SKIPPED - No credentials";
    } else {
      console.log("Testing ByBit...");
      const result = await testByBitAccount(BYBIT_CONFIG);
      debugInfo.apiResults["ByBit"] = result;
      
      if (result.success) {
        allTransactions.push(...result.transactions);
      } else {
        debugInfo.errors.push(`ByBit: ${result.error}`);
      }
    }

    res.status(200).json({
      success: true,
      transactions: allTransactions,
      count: allTransactions.length,
      timestamp: new Date().toISOString(),
      debug: debugInfo
    });

  } catch (error) {
    console.error('Exchange API Error:', error);
    
    res.status(500).json({
      success: false,
      error: error.message,
      timestamp: new Date().toISOString()
    });
  }
}

/**
 * Test Binance account with simple account info call
 */
async function testBinanceAccount(account) {
  try {
    const timestamp = Date.now();
    
    // Simple account info call to test credentials
    const endpoint = "https://api.binance.com/api/v3/account";
    const params = {
      timestamp: timestamp,
      recvWindow: 5000
    };

    const signature = createBinanceSignature(params, account.apiSecret);
    const queryString = createQueryString(params);
    const url = `${endpoint}?${queryString}&signature=${signature}`;

    const response = await fetch(url, {
      method: "GET",
      headers: {
        "X-MBX-APIKEY": account.apiKey
      }
    });

    const responseText = await response.text();
    
    if (!response.ok) {
      return {
        success: false,
        error: `HTTP ${response.status}: ${responseText.substring(0, 200)}`,
        transactions: []
      };
    }

    const data = JSON.parse(responseText);
    
    if (data.code && data.code !== 200) {
      return {
        success: false,
        error: `Binance error: ${data.msg}`,
        transactions: []
      };
    }

    // If we get here, credentials work - now try to get recent deposits
    const deposits = await fetchBinanceDeposits(account, timestamp);
    
    return {
      success: true,
      error: null,
      transactions: deposits,
      accountInfo: `Account has ${data.balances?.length || 0} balances`
    };

  } catch (error) {
    return {
      success: false,
      error: error.message,
      transactions: []
    };
  }
}

/**
 * Test ByBit account
 */
async function testByBitAccount(config) {
  try {
    const timestamp = Date.now();
    
    // Simple account info call
    const endpoint = "https://api.bybit.com/v5/account/wallet-balance";
    const params = {
      accountType: "UNIFIED",
      timestamp: timestamp.toString()
    };

    const signature = createByBitSignature(params, config.apiSecret);
    const queryString = createQueryString(params);
    const url = `${endpoint}?${queryString}`;

    const response = await fetch(url, {
      method: "GET",
      headers: {
        "X-BAPI-API-KEY": config.apiKey,
        "X-BAPI-SIGN": signature,
        "X-BAPI-TIMESTAMP": timestamp.toString()
      }
    });

    const responseText = await response.text();
    
    if (!response.ok) {
      return {
        success: false,
        error: `HTTP ${response.status}: ${responseText.substring(0, 200)}`,
        transactions: []
      };
    }

    const data = JSON.parse(responseText);
    
    if (data.retCode !== 0) {
      return {
        success: false,
        error: `ByBit error: ${data.retMsg}`,
        transactions: []
      };
    }

    // If credentials work, try to get deposits
    const deposits = await fetchByBitDeposits(config, timestamp);
    
    return {
      success: true,
      error: null,
      transactions: deposits,
      accountInfo: "Account accessible"
    };

  } catch (error) {
    return {
      success: false,
      error: error.message,
      transactions: []
    };
  }
}

/**
 * Fetch Binance deposits (simplified for testing)
 */
async function fetchBinanceDeposits(account, timestamp) {
  try {
    const endpoint = "https://api.binance.com/sapi/v1/capital/deposit/hisrec";
    const params = {
      timestamp: timestamp,
      recvWindow: 5000,
      limit: 10 // Limit for testing
    };

    const signature = createBinanceSignature(params, account.apiSecret);
    const queryString = createQueryString(params);
    const url = `${endpoint}?${queryString}&signature=${signature}`;

    const response = await fetch(url, {
      method: "GET",
      headers: {
        "X-MBX-APIKEY": account.apiKey
      }
    });

    if (!response.ok) {
      throw new Error(`Deposits API error: ${response.status}`);
    }

    const data = await response.json();

    if (data.code && data.code !== 200) {
      throw new Error(`Binance deposits error: ${data.msg}`);
    }

    return data.map(deposit => ({
      platform: account.name,
      type: "deposit",
      asset: deposit.coin,
      amount: deposit.amount.toString(),
      timestamp: new Date(deposit.insertTime).toISOString(),
      from_address: deposit.address || "External",
      to_address: account.name,
      tx_id: deposit.txId || deposit.id,
      status: deposit.status === 1 ? "Completed" : "Pending",
      network: deposit.network,
      api_source: "Binance_Deposit"
    }));

  } catch (error) {
    console.error(`Error fetching deposits for ${account.name}:`, error);
    return [];
  }
}

/**
 * Fetch ByBit deposits (simplified for testing)
 */
async function fetchByBitDeposits(config, timestamp) {
  try {
    const endpoint = "https://api.bybit.com/v5/asset/deposit/query-record";
    const params = {
      limit: 10, // Limit for testing
      timestamp: timestamp.toString()
    };

    const signature = createByBitSignature(params, config.apiSecret);
    const queryString = createQueryString(params);
    const url = `${endpoint}?${queryString}`;

    const response = await fetch(url, {
      method: "GET",
      headers: {
        "X-BAPI-API-KEY": config.apiKey,
        "X-BAPI-SIGN": signature,
        "X-BAPI-TIMESTAMP": timestamp.toString()
      }
    });

    if (!response.ok) {
      throw new Error(`Deposits API error: ${response.status}`);
    }

    const data = await response.json();

    if (data.retCode !== 0) {
      throw new Error(`ByBit deposits error: ${data.retMsg}`);
    }

    return data.result.rows.map(deposit => ({
      platform: config.name,
      type: "deposit",
      asset: deposit.coin,
      amount: deposit.amount.toString(),
      timestamp: new Date(parseInt(deposit.successAt)).toISOString(),
      from_address: "External",
      to_address: config.name,
      tx_id: deposit.txID,
      status: deposit.status === 3 ? "Completed" : "Pending",
      network: deposit.chain,
      api_source: "ByBit_Deposit"
    }));

  } catch (error) {
    console.error("Error fetching ByBit deposits:", error);
    return [];
  }
}

/**
 * Create Binance signature
 */
function createBinanceSignature(params, secret) {
  const queryString = createQueryString(params);
  return crypto.createHmac('sha256', secret).update(queryString).digest('hex');
}

/**
 * Create ByBit signature
 */
function createByBitSignature(params, secret) {
  const queryString = createQueryString(params);
  return crypto.createHmac('sha256', secret).update(queryString).digest('hex');
}

/**
 * Create query string from parameters
 */
function createQueryString(params) {
  return Object.keys(params)
    .sort()
    .map(key => `${encodeURIComponent(key)}=${encodeURIComponent(params[key])}`)
    .join('&');
}
