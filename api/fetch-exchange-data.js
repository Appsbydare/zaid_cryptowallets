// File: api/fetch-exchange-data.js
// Deploy this to Vercel with Canadian region

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

    // Fetch from all Binance accounts
    for (const account of BINANCE_ACCOUNTS) {
      if (!account.apiKey || !account.apiSecret) {
        console.log(`Skipping ${account.name} - missing credentials`);
        continue;
      }

      console.log(`Fetching ${account.name}...`);
      const transactions = await fetchBinanceTransactions(account);
      allTransactions.push(...transactions);
    }

    // Fetch from ByBit
    if (BYBIT_CONFIG.apiKey && BYBIT_CONFIG.apiSecret) {
      console.log("Fetching ByBit...");
      const bybitTransactions = await fetchByBitTransactions(BYBIT_CONFIG);
      allTransactions.push(...bybitTransactions);
    }

    res.status(200).json({
      success: true,
      transactions: allTransactions,
      count: allTransactions.length,
      timestamp: new Date().toISOString()
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
 * Fetch all Binance transactions for an account
 */
async function fetchBinanceTransactions(account) {
  try {
    const transactions = [];
    const now = Date.now();

    // Get deposits
    const deposits = await fetchBinanceDeposits(account, now);
    transactions.push(...deposits);

    // Get withdrawals
    const withdrawals = await fetchBinanceWithdrawals(account, now);
    transactions.push(...withdrawals);

    // Get P2P transactions
    const p2pTransactions = await fetchBinanceP2P(account, now);
    transactions.push(...p2pTransactions);

    return transactions;

  } catch (error) {
    console.error(`Error fetching ${account.name}:`, error);
    return [];
  }
}

/**
 * Fetch Binance deposits
 */
async function fetchBinanceDeposits(account, timestamp) {
  try {
    const endpoint = "https://api.binance.com/sapi/v1/capital/deposit/hisrec";
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

    if (!response.ok) {
      throw new Error(`Binance deposits API error: ${response.status} ${response.statusText}`);
    }

    const data = await response.json();

    if (data.code && data.code !== 200) {
      console.error(`Binance deposits error for ${account.name}:`, data.msg);
      return [];
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
 * Fetch Binance withdrawals
 */
async function fetchBinanceWithdrawals(account, timestamp) {
  try {
    const endpoint = "https://api.binance.com/sapi/v1/capital/withdraw/history";
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

    if (!response.ok) {
      throw new Error(`Binance withdrawals API error: ${response.status} ${response.statusText}`);
    }

    const data = await response.json();

    if (data.code && data.code !== 200) {
      console.error(`Binance withdrawals error for ${account.name}:`, data.msg);
      return [];
    }

    return data.map(withdrawal => ({
      platform: account.name,
      type: "withdrawal",
      asset: withdrawal.coin,
      amount: withdrawal.amount.toString(),
      timestamp: new Date(withdrawal.applyTime).toISOString(),
      from_address: account.name,
      to_address: withdrawal.address,
      tx_id: withdrawal.txId || withdrawal.id,
      status: withdrawal.status === 6 ? "Completed" : "Pending",
      network: withdrawal.network,
      api_source: "Binance_Withdrawal"
    }));

  } catch (error) {
    console.error(`Error fetching withdrawals for ${account.name}:`, error);
    return [];
  }
}

/**
 * Fetch Binance P2P transactions
 */
async function fetchBinanceP2P(account, timestamp) {
  try {
    const endpoint = "https://api.binance.com/sapi/v1/c2c/orderMatch/listUserOrderHistory";
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

    if (!response.ok) {
      throw new Error(`Binance P2P API error: ${response.status} ${response.statusText}`);
    }

    const data = await response.json();

    if (data.code && data.code !== 200) {
      console.error(`Binance P2P error for ${account.name}:`, data.msg);
      return [];
    }

    return data.data.map(p2p => ({
      platform: account.name,
      type: p2p.tradeType.toLowerCase() === 'buy' ? 'deposit' : 'withdrawal',
      asset: p2p.asset,
      amount: p2p.totalPrice.toString(),
      timestamp: new Date(p2p.createTime).toISOString(),
      from_address: "P2P Trade",
      to_address: account.name,
      tx_id: p2p.orderNumber,
      status: p2p.orderStatus === "COMPLETED" ? "Completed" : "Pending",
      network: "P2P",
      api_source: "Binance_P2P"
    }));

  } catch (error) {
    console.error(`Error fetching P2P for ${account.name}:`, error);
    return [];
  }
}

/**
 * Fetch ByBit transactions
 */
async function fetchByBitTransactions(config) {
  try {
    const transactions = [];
    const timestamp = Date.now();

    // Get deposits
    const deposits = await fetchByBitDeposits(config, timestamp);
    transactions.push(...deposits);

    // Get withdrawals
    const withdrawals = await fetchByBitWithdrawals(config, timestamp);
    transactions.push(...withdrawals);

    return transactions;

  } catch (error) {
    console.error("Error fetching ByBit:", error);
    return [];
  }
}

/**
 * Fetch ByBit deposits
 */
async function fetchByBitDeposits(config, timestamp) {
  try {
    const endpoint = "https://api.bybit.com/v5/asset/deposit/query-record";
    const params = {
      api_key: config.apiKey,
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
      throw new Error(`ByBit deposits API error: ${response.status} ${response.statusText}`);
    }

    const data = await response.json();

    if (data.retCode !== 0) {
      console.error("ByBit deposits error:", data.retMsg);
      return [];
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
 * Fetch ByBit withdrawals
 */
async function fetchByBitWithdrawals(config, timestamp) {
  try {
    const endpoint = "https://api.bybit.com/v5/asset/withdraw/query-record";
    const params = {
      api_key: config.apiKey,
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
      throw new Error(`ByBit withdrawals API error: ${response.status} ${response.statusText}`);
    }

    const data = await response.json();

    if (data.retCode !== 0) {
      console.error("ByBit withdrawals error:", data.retMsg);
      return [];
    }

    return data.result.rows.map(withdrawal => ({
      platform: config.name,
      type: "withdrawal",
      asset: withdrawal.coin,
      amount: withdrawal.amount.toString(),
      timestamp: new Date(parseInt(withdrawal.createTime)).toISOString(),
      from_address: config.name,
      to_address: withdrawal.toAddress,
      tx_id: withdrawal.txID,
      status: withdrawal.status === "success" ? "Completed" : "Pending",
      network: withdrawal.chain,
      api_source: "ByBit_Withdrawal"
    }));

  } catch (error) {
    console.error("Error fetching ByBit withdrawals:", error);
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
