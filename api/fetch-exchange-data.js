// File: api/fetch-exchange-data.js
// ENHANCED VERSION - Handles POST requests with API credentials/ENHANCED VERSION - Handles POST requests with API credentials


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
    let binanceAccounts = [];
    let bybitAccounts = [];

    // Handle both GET (legacy) and POST (new with credentials)
    if (req.method === 'POST' && req.body) {
      // New method: Use credentials from request body
      binanceAccounts = req.body.binance_accounts || [];
      bybitAccounts = req.body.bybit_accounts || [];
      console.log(`📊 Received ${binanceAccounts.length} Binance + ${bybitAccounts.length} ByBit accounts`);
    } else {
      // Legacy method: Use environment variables
      binanceAccounts = [
        {
          name: "GC Account",
          api_key: process.env.BINANCE_GC_API_KEY,
          api_secret: process.env.BINANCE_GC_API_SECRET
        },
        {
          name: "Main Account", 
          api_key: process.env.BINANCE_MAIN_API_KEY,
          api_secret: process.env.BINANCE_MAIN_API_SECRET
        },
        {
          name: "CV Account",
          api_key: process.env.BINANCE_CV_API_KEY,
          api_secret: process.env.BINANCE_CV_API_SECRET
        }
      ].filter(acc => acc.api_key && acc.api_secret);

      bybitAccounts = [
        {
          name: "CV Account",
          api_key: process.env.BYBIT_API_KEY,
          api_secret: process.env.BYBIT_API_SECRET
        }
      ].filter(acc => acc.api_key && acc.api_secret);
    }

    const results = {};
    const allTransactions = [];
    let totalCount = 0;

    // Process each Binance account
    for (const account of binanceAccounts) {
      console.log(`🧪 Processing Binance ${account.name}...`);
      
      const accountResult = await processBinanceAccount(account);
      results[`Binance (${account.name})`] = accountResult;
      
      if (accountResult.success && accountResult.transactions) {
        allTransactions.push(...accountResult.transactions);
        totalCount += accountResult.transactions.length;
      }
    }

    // Process each ByBit account  
    for (const account of bybitAccounts) {
      console.log(`🧪 Processing ByBit ${account.name}...`);
      
      const accountResult = await processByBitAccount(account);
      results[`ByBit (${account.name})`] = accountResult;
      
      if (accountResult.success && accountResult.transactions) {
        allTransactions.push(...accountResult.transactions);
        totalCount += accountResult.transactions.length;
      }
    }

    res.status(200).json({
      success: true,
      transactions: allTransactions,
      count: totalCount,
      results: results,
      timestamp: new Date().toISOString(),
      method: req.method,
      region: "Canada (yyz1)"
    });

  } catch (error) {
    console.error('Enhanced Exchange API Error:', error);
    
    res.status(500).json({
      success: false,
      error: error.message,
      timestamp: new Date().toISOString(),
      region: "Canada (yyz1)"
    });
  }
}

/**
 * Process a single Binance account (P2P + Pay + Standard)
 */
async function processBinanceAccount(account) {
  const result = {
    platform: `Binance (${account.name})`,
    success: false,
    transactions: [],
    p2p_count: 0,
    pay_count: 0,
    standard_count: 0,
    total_count: 0,
    error: null
  };

  try {
    const timestamp = Date.now();
    
    // 1. Try P2P transactions
    try {
      const p2pTransactions = await fetchBinanceP2P(account, timestamp);
      result.transactions.push(...p2pTransactions);
      result.p2p_count = p2pTransactions.length;
      console.log(`✅ ${account.name} P2P: ${p2pTransactions.length} transactions`);
    } catch (p2pError) {
      console.log(`⚠️ ${account.name} P2P failed: ${p2pError.message}`);
      if (p2pError.message.includes('451')) {
        result.error = "P2P blocked (region)";
      }
    }

    // 2. Try Binance Pay transactions
    try {
      const payTransactions = await fetchBinancePay(account, timestamp);
      result.transactions.push(...payTransactions);
      result.pay_count = payTransactions.length;
      console.log(`✅ ${account.name} Pay: ${payTransactions.length} transactions`);
    } catch (payError) {
      console.log(`⚠️ ${account.name} Pay failed: ${payError.message}`);
    }

    // 3. Try Standard transactions (deposits/withdrawals)
    try {
      const standardTransactions = await fetchBinanceStandard(account, timestamp);
      result.transactions.push(...standardTransactions);
      result.standard_count = standardTransactions.length;
      console.log(`✅ ${account.name} Standard: ${standardTransactions.length} transactions`);
    } catch (standardError) {
      console.log(`⚠️ ${account.name} Standard failed: ${standardError.message}`);
    }

    result.total_count = result.transactions.length;
    result.success = result.total_count > 0 || !result.error;

    if (result.total_count === 0 && !result.error) {
      result.error = "No transactions found (empty account or no history)";
    }

  } catch (error) {
    result.error = error.message;
    result.success = false;
  }

  return result;
}

/**
 * Process a single ByBit account
 */
async function processByBitAccount(account) {
  const result = {
    platform: `ByBit (${account.name})`,
    success: false,
    transactions: [],
    total_count: 0,
    error: null
  };

  try {
    const timestamp = Date.now();
    
    // Test ByBit deposits
    const depositTransactions = await fetchByBitDeposits(account, timestamp);
    result.transactions.push(...depositTransactions);
    
    // Test ByBit withdrawals  
    const withdrawalTransactions = await fetchByBitWithdrawals(account, timestamp);
    result.transactions.push(...withdrawalTransactions);

    result.total_count = result.transactions.length;
    result.success = true;

    console.log(`✅ ${account.name}: ${result.total_count} transactions`);

  } catch (error) {
    result.error = error.message;
    result.success = false;
    console.error(`❌ ${account.name}: ${error.message}`);
  }

  return result;
}

/**
 * Fetch Binance P2P transactions
 */
async function fetchBinanceP2P(account, timestamp) {
  const transactions = [];
  
  // P2P Buy orders (deposits)
  const buyEndpoint = "https://api.binance.com/sapi/v1/c2c/orderMatch/listUserOrderHistory";
  const buyParams = {
    tradeType: "BUY",
    timestamp: timestamp,
    recvWindow: 5000
  };
  
  const buySignature = createBinanceSignature(buyParams, account.api_secret);
  const buyQuery = createQueryString(buyParams);
  const buyUrl = `${buyEndpoint}?${buyQuery}&signature=${buySignature}`;
  
  const buyResponse = await fetch(buyUrl, {
    headers: { "X-MBX-APIKEY": account.api_key }
  });
  
  if (!buyResponse.ok) {
    throw new Error(`P2P API error: ${buyResponse.status}`);
  }
  
  const buyData = await buyResponse.json();
  
  if (buyData.data) {
    buyData.data.forEach(order => {
      transactions.push({
        platform: `Binance (${account.name})`,
        type: "deposit",
        asset: order.asset,
        amount: order.amount,
        timestamp: new Date(parseInt(order.createTime)).toISOString(),
        from_address: "P2P User",
        to_address: `Binance (${account.name})`,
        tx_id: `P2P_${order.orderNumber}`,
        status: order.orderStatus === "COMPLETED" ? "Completed" : "Pending",
        network: "P2P",
        api_source: "Binance_P2P"
      });
    });
  }
  
  // P2P Sell orders (withdrawals) - similar logic
  // ... (implement sell orders)
  
  return transactions;
}

/**
 * Fetch Binance Pay transactions
 */
async function fetchBinancePay(account, timestamp) {
  const endpoint = "https://api.binance.com/sapi/v1/pay/transactions";
  const params = {
    timestamp: timestamp,
    recvWindow: 5000,
    limit: 100
  };
  
  const signature = createBinanceSignature(params, account.api_secret);
  const queryString = createQueryString(params);
  const url = `${endpoint}?${queryString}&signature=${signature}`;
  
  const response = await fetch(url, {
    headers: { "X-MBX-APIKEY": account.api_key }
  });
  
  if (!response.ok) {
    throw new Error(`Binance Pay API error: ${response.status}`);
  }
  
  const data = await response.json();
  const transactions = [];
  
  if (data.data) {
    data.data.forEach(tx => {
      const isDeposit = tx.type === "PAY" && tx.direction === "IN";
      
      if (isDeposit || tx.direction === "OUT") {
        transactions.push({
          platform: `Binance (${account.name})`,
          type: isDeposit ? "deposit" : "withdrawal",
          asset: tx.currency,
          amount: tx.amount,
          timestamp: new Date(parseInt(tx.createTime)).toISOString(),
          from_address: isDeposit ? "Binance Pay User" : `Binance (${account.name})`,
          to_address: isDeposit ? `Binance (${account.name})` : "Binance Pay User",
          tx_id: `PAY_${tx.transactionId}`,
          status: tx.status === "SUCCESS" ? "Completed" : "Pending",
          network: "Binance Pay",
          api_source: "Binance_Pay"
        });
      }
    });
  }
  
  return transactions;
}

/**
 * Fetch Binance Standard transactions (deposits + withdrawals)
 */
async function fetchBinanceStandard(account, timestamp) {
  const transactions = [];
  
  // Fetch deposits
  const depositEndpoint = "https://api.binance.com/sapi/v1/capital/deposit/hisrec";
  const depositParams = {
    timestamp: timestamp,
    recvWindow: 5000,
    limit: 100
  };
  
  const depositSignature = createBinanceSignature(depositParams, account.api_secret);
  const depositQuery = createQueryString(depositParams);
  const depositUrl = `${depositEndpoint}?${depositQuery}&signature=${depositSignature}`;
  
  const depositResponse = await fetch(depositUrl, {
    headers: { "X-MBX-APIKEY": account.api_key }
  });
  
  if (depositResponse.ok) {
    const depositData = await depositResponse.json();
    
    if (Array.isArray(depositData)) {
      depositData.forEach(deposit => {
        transactions.push({
          platform: `Binance (${account.name})`,
          type: "deposit",
          asset: deposit.coin,
          amount: deposit.amount,
          timestamp: new Date(deposit.insertTime).toISOString(),
          from_address: deposit.address || "External",
          to_address: `Binance (${account.name})`,
          tx_id: deposit.txId || deposit.id,
          status: deposit.status === 1 ? "Completed" : "Pending",
          network: deposit.network,
          api_source: "Binance_Standard"
        });
      });
    }
  }
  
  // Fetch withdrawals (similar pattern)
  // ... (implement withdrawals)
  
  return transactions;
}

/**
 * Fetch ByBit deposits
 */
async function fetchByBitDeposits(account, timestamp) {
  // Implement ByBit deposit API logic
  return [];
}

/**
 * Fetch ByBit withdrawals
 */
async function fetchByBitWithdrawals(account, timestamp) {
  // Implement ByBit withdrawal API logic
  return [];
}

/**
 * Helper functions
 */
function createBinanceSignature(params, secret) {
  const queryString = createQueryString(params);
  return crypto.createHmac('sha256', secret).update(queryString).digest('hex');
}

function createQueryString(params) {
  return Object.keys(params)
    .sort()
    .map(key => `${encodeURIComponent(key)}=${encodeURIComponent(params[key])}`)
    .join('&');
}
