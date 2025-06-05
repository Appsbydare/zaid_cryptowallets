// ===========================================
// FILE 1: api/crypto-to-sheets.js (NEW VERCEL ENDPOINT)
// ===========================================

import crypto from 'crypto';
import { GoogleAuth } from 'google-auth-library';
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
    console.log('🚀 Starting Vercel-to-Sheets crypto data fetch...');

    const allTransactions = [];
    const apiStatusResults = {};

    // ===========================================
    // STEP 1: FETCH BINANCE DATA (ALL TYPES)
    // ===========================================
    const binanceAccounts = [
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

    for (const account of binanceAccounts) {
      if (!account.apiKey || !account.apiSecret) {
        console.log(`⚠️ ${account.name}: Missing API credentials`);
        apiStatusResults[account.name] = {
          status: 'Error',
          lastSync: new Date().toISOString(),
          autoUpdate: 'Every Hour',
          notes: '❌ Missing credentials',
          transactionCount: 0
        };
        continue;
      }

      console.log(`🧪 Testing ${account.name}...`);
      const result = await testBinanceAccountVercel(account);
      apiStatusResults[account.name] = result.status;
      
      if (result.success) {
        allTransactions.push(...result.transactions);
      }
    }

    // ===========================================
    // STEP 2: FETCH BYBIT DATA
    // ===========================================
    if (process.env.BYBIT_API_KEY && process.env.BYBIT_API_SECRET) {
      console.log('🧪 Testing ByBit...');
      const bybitResult = await testByBitAccountVercel({
        name: "ByBit (CV)",
        apiKey: process.env.BYBIT_API_KEY,
        apiSecret: process.env.BYBIT_API_SECRET
      });
      
      apiStatusResults["ByBit (CV)"] = bybitResult.status;
      if (bybitResult.success) {
        allTransactions.push(...bybitResult.transactions);
      }
    }

    // ===========================================
    // STEP 3: FETCH BLOCKCHAIN DATA
    // ===========================================
    console.log('🧪 Fetching blockchain data...');
    const blockchainData = await fetchBlockchainDataVercel();
    allTransactions.push(...blockchainData.transactions);
    Object.assign(apiStatusResults, blockchainData.apiStatus);

    // ===========================================
    // STEP 4: WRITE TO GOOGLE SHEETS
    // ===========================================
    console.log(`📊 Writing ${allTransactions.length} transactions to Google Sheets...`);
    const sheetsResult = await writeToGoogleSheets(allTransactions, apiStatusResults);

    res.status(200).json({
      success: true,
      message: 'Data successfully written to Google Sheets',
      transactions: allTransactions.length,
      sheetsResult: sheetsResult,
      apiStatus: apiStatusResults,
      timestamp: new Date().toISOString()
    });

  } catch (error) {
    console.error('❌ Vercel-to-Sheets Error:', error);
    
    res.status(500).json({
      success: false,
      error: error.message,
      timestamp: new Date().toISOString()
    });
  }
}

// ===========================================
// BINANCE API FUNCTIONS (VERCEL VERSION)
// ===========================================

async function testBinanceAccountVercel(account) {
  try {
    const allTransactions = [];
    let notes = [];

    // Try P2P transactions
    try {
      const p2pTxs = await fetchBinanceP2PVercel(account.apiKey, account.apiSecret, account.name);
      allTransactions.push(...p2pTxs);
      notes.push(`P2P:${p2pTxs.length}`);
    } catch (p2pError) {
      console.log(`⚠️ ${account.name} P2P failed: ${p2pError.message}`);
      notes.push(`P2P:❌`);
    }

    // Try Binance Pay transactions
    try {
      const payTxs = await fetchBinancePayVercel(account.apiKey, account.apiSecret, account.name);
      allTransactions.push(...payTxs);
      notes.push(`Pay:${payTxs.length}`);
    } catch (payError) {
      console.log(`⚠️ ${account.name} Pay failed: ${payError.message}`);
      notes.push(`Pay:❌`);
    }

    // Try standard transactions
    try {
      const standardTxs = await fetchBinanceStandardVercel(account.apiKey, account.apiSecret, account.name);
      allTransactions.push(...standardTxs);
      notes.push(`Standard:${standardTxs.length}`);
    } catch (standardError) {
      console.log(`⚠️ ${account.name} Standard failed: ${standardError.message}`);
      notes.push(`Standard:❌`);
    }

    const totalTxs = allTransactions.length;
    
    return {
      success: totalTxs > 0,
      transactions: allTransactions,
      status: {
        status: totalTxs > 0 ? 'Active' : 'Error',
        lastSync: new Date().toISOString(),
        autoUpdate: 'Every Hour',
        notes: notes.join(', '),
        transactionCount: totalTxs
      }
    };

  } catch (error) {
    return {
      success: false,
      transactions: [],
      status: {
        status: 'Error',
        lastSync: new Date().toISOString(),
        autoUpdate: 'Every Hour',
        notes: `❌ ${error.message}`,
        transactionCount: 0
      }
    };
  }
}

async function fetchBinanceP2PVercel(apiKey, secretKey, accountName) {
  const binanceEndpoints = [
    "https://api.binance.com",
    "https://api1.binance.com", 
    "https://api2.binance.com",
    "https://api3.binance.com"
  ];
  
  for (const baseUrl of binanceEndpoints) {
    try {
      const timestamp = Date.now();
      
      const endpoint = `${baseUrl}/sapi/v1/c2c/orderMatch/listUserOrderHistory`;
      const params = {
        tradeType: "BUY",
        timestamp: timestamp,
        recvWindow: 5000
      };
      
      const signature = createBinanceSignatureVercel(params, secretKey);
      const queryString = createQueryStringVercel(params);
      const url = `${endpoint}?${queryString}&signature=${signature}`;
      
      const response = await fetch(url, {
        method: "GET",
        headers: {
          "X-MBX-APIKEY": apiKey,
          "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
          "Accept": "application/json"
        }
      });
      
      if (response.status === 451) {
        console.log(`⚠️ Endpoint ${baseUrl} blocked (451), trying next...`);
        continue;
      }
      
      if (!response.ok) {
        throw new Error(`P2P API error: ${response.status}`);
      }
      
      const data = await response.json();
      
      if (data.code && data.code !== 200) {
        throw new Error(`Binance P2P error: ${data.msg}`);
      }
      
      const transactions = [];
      
      // Process BUY orders (deposits)
      if (data.data) {
        data.data.forEach(order => {
          transactions.push({
            platform: `Binance (${accountName.replace('Binance (', '').replace(')', '')})`,
            type: "deposit",
            asset: order.asset,
            amount: order.amount,
            timestamp: new Date(parseInt(order.createTime)).toISOString(),
            from_address: "P2P User",
            to_address: `Binance (${accountName.replace('Binance (', '').replace(')', '')})`,
            tx_id: `P2P_${order.orderNumber}`,
            status: order.orderStatus === "COMPLETED" ? "Completed" : "Pending",
            network: "P2P",
            api_source: "Binance_P2P"
          });
        });
      }
      
      console.log(`✅ P2P API success on ${baseUrl} - found ${transactions.length} transactions`);
      return transactions;
      
    } catch (error) {
      console.log(`❌ Error on ${baseUrl}: ${error.message}`);
      continue;
    }
  }
  
  throw new Error("All Binance P2P endpoints blocked or failed");
}

async function fetchBinancePayVercel(apiKey, secretKey, accountName) {
  // Similar implementation to P2P but for Pay API
  const binanceEndpoints = [
    "https://api.binance.com",
    "https://api1.binance.com", 
    "https://api2.binance.com",
    "https://api3.binance.com"
  ];
  
  for (const baseUrl of binanceEndpoints) {
    try {
      const timestamp = Date.now();
      
      const endpoint = `${baseUrl}/sapi/v1/pay/transactions`;
      const params = {
        timestamp: timestamp,
        recvWindow: 5000,
        limit: 100
      };
      
      const signature = createBinanceSignatureVercel(params, secretKey);
      const queryString = createQueryStringVercel(params);
      const url = `${endpoint}?${queryString}&signature=${signature}`;
      
      const response = await fetch(url, {
        method: "GET",
        headers: {
          "X-MBX-APIKEY": apiKey,
          "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
          "Accept": "application/json"
        }
      });
      
      if (response.status === 451) {
        console.log(`⚠️ Endpoint ${baseUrl} blocked (451), trying next...`);
        continue;
      }
      
      if (!response.ok) {
        throw new Error(`Pay API error: ${response.status}`);
      }
      
      const data = await response.json();
      
      if (data.code && data.code !== 200) {
        throw new Error(`Binance Pay error: ${data.msg}`);
      }
      
      const transactions = [];
      
      if (data.data) {
        data.data.forEach(tx => {
          const isDeposit = tx.type === "PAY" && tx.direction === "IN";
          const isWithdrawal = tx.type === "PAY" && tx.direction === "OUT";
          
          if (isDeposit || isWithdrawal) {
            transactions.push({
              platform: `Binance (${accountName.replace('Binance (', '').replace(')', '')})`,
              type: isDeposit ? "deposit" : "withdrawal",
              asset: tx.currency,
              amount: tx.amount,
              timestamp: new Date(parseInt(tx.createTime)).toISOString(),
              from_address: isDeposit ? "Binance Pay User" : `Binance (${accountName.replace('Binance (', '').replace(')', '')})`,
              to_address: isDeposit ? `Binance (${accountName.replace('Binance (', '').replace(')', '')})` : "Binance Pay User",
              tx_id: `PAY_${tx.transactionId}`,
              status: tx.status === "SUCCESS" ? "Completed" : "Pending",
              network: "Binance Pay",
              api_source: "Binance_Pay"
            });
          }
        });
      }
      
      console.log(`✅ Pay API success on ${baseUrl} - found ${transactions.length} transactions`);
      return transactions;
      
    } catch (error) {
      console.log(`❌ Error on ${baseUrl}: ${error.message}`);
      continue;
    }
  }
  
  throw new Error("All Binance Pay endpoints blocked or failed");
}

async function fetchBinanceStandardVercel(apiKey, secretKey, accountName) {
  // Similar implementation for standard deposit/withdrawal API
  const binanceEndpoints = [
    "https://api.binance.com",
    "https://api1.binance.com", 
    "https://api2.binance.com",
    "https://api3.binance.com"
  ];
  
  for (const baseUrl of binanceEndpoints) {
    try {
      const transactions = [];
      const timestamp = Date.now();
      
      // Fetch deposits
      const depositEndpoint = `${baseUrl}/sapi/v1/capital/deposit/hisrec`;
      const depositParams = {
        timestamp: timestamp,
        recvWindow: 5000,
        limit: 100
      };
      
      const depositSignature = createBinanceSignatureVercel(depositParams, secretKey);
      const depositQueryString = createQueryStringVercel(depositParams);
      const depositUrl = `${depositEndpoint}?${depositQueryString}&signature=${depositSignature}`;
      
      const depositResponse = await fetch(depositUrl, {
        method: "GET",
        headers: {
          "X-MBX-APIKEY": apiKey,
          "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
          "Accept": "application/json"
        }
      });
      
      if (depositResponse.status === 451) {
        console.log(`⚠️ Endpoint ${baseUrl} blocked (451), trying next...`);
        continue;
      }
      
      if (depositResponse.ok) {
        const depositData = await depositResponse.json();
        
        if (Array.isArray(depositData)) {
          depositData.forEach(deposit => {
            transactions.push({
              platform: `Binance (${accountName.replace('Binance (', '').replace(')', '')})`,
              type: "deposit",
              asset: deposit.coin,
              amount: deposit.amount,
              timestamp: new Date(deposit.insertTime).toISOString(),
              from_address: deposit.address || "External",
              to_address: `Binance (${accountName.replace('Binance (', '').replace(')', '')})`,
              tx_id: deposit.txId || deposit.id,
              status: deposit.status === 1 ? "Completed" : "Pending",
              network: deposit.network,
              api_source: "Binance_Standard"
            });
          });
        }
      }
      
      console.log(`✅ Standard API success on ${baseUrl} - found ${transactions.length} transactions`);
      return transactions;
      
    } catch (error) {
      console.log(`❌ Error on ${baseUrl}: ${error.message}`);
      continue;
    }
  }
  
  throw new Error("All Binance Standard endpoints blocked or failed");
}

// ===========================================
// BYBIT API FUNCTION (VERCEL VERSION)
// ===========================================

async function testByBitAccountVercel(config) {
  try {
    const timestamp = Date.now();
    
    const endpoint = "https://api.bybit.com/v5/account/wallet-balance";
    const params = {
      accountType: "UNIFIED",
      timestamp: timestamp.toString()
    };

    const signature = createByBitSignatureVercel(params, config.apiSecret);
    const queryString = createQueryStringVercel(params);
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
      throw new Error(`HTTP ${response.status}: ${await response.text()}`);
    }

    const data = await response.json();
    
    if (data.retCode !== 0) {
      throw new Error(`ByBit error: ${data.retMsg}`);
    }

    return {
      success: true,
      transactions: [], // Would implement actual transaction fetching
      status: {
        status: 'Active',
        lastSync: new Date().toISOString(),
        autoUpdate: 'Every Hour',
        notes: '✅ Connected successfully',
        transactionCount: 0
      }
    };

  } catch (error) {
    return {
      success: false,
      transactions: [],
      status: {
        status: 'Error',
        lastSync: new Date().toISOString(),
        autoUpdate: 'Every Hour',
        notes: `❌ ${error.message}`,
        transactionCount: 0
      }
    };
  }
}

// ===========================================
// BLOCKCHAIN API FUNCTIONS (VERCEL VERSION)
// ===========================================

async function fetchBlockchainDataVercel() {
  const transactions = [];
  const apiStatus = {};
  
  const wallets = {
    BTC: "bc1qkuefzcmc6c8enw9f7a2e9w2hy964q3jgwcv35g",
    ETH: "0x856851a1d5111330729744f95238e5D810ba773c",
    TRON: "TAUDuQAZSTUH88xno1imPoKN25eJN6aJkN",
    SOL: "BURkHx6BNTqryY3sCqXcYNVkhN6Mz3ttDUdGQ6hXuX4n"
  };

  // Bitcoin transactions
  try {
    const btcTxs = await fetchBitcoinTransactionsVercel(wallets.BTC);
    transactions.push(...btcTxs);
    apiStatus['Bitcoin Wallet'] = {
      status: 'Active',
      lastSync: new Date().toISOString(),
      autoUpdate: 'Every Hour',
      notes: `✅ ${btcTxs.length} transactions found`,
      transactionCount: btcTxs.length
    };
  } catch (error) {
    apiStatus['Bitcoin Wallet'] = {
      status: 'Error',
      lastSync: new Date().toISOString(),
      autoUpdate: 'Every Hour',
      notes: `❌ ${error.message}`,
      transactionCount: 0
    };
  }

  // Similar for other blockchains...
  
  return {
    transactions: transactions,
    apiStatus: apiStatus
  };
}

async function fetchBitcoinTransactionsVercel(address) {
  try {
    const endpoint = `https://blockchain.info/rawaddr/${address}`;
    const response = await fetch(endpoint);
    
    if (response.status === 429) {
      console.log("⏳ Bitcoin API rate limited");
      return [];
    }
    
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${await response.text()}`);
    }
    
    const data = await response.json();
    const transactions = [];
    
    data.txs.slice(0, 50).forEach(tx => {
      const isDeposit = tx.out.some(output => output.addr === address);
      const isWithdrawal = tx.inputs.some(input => input.prev_out && input.prev_out.addr === address);
      
      if (isDeposit) {
        const output = tx.out.find(o => o.addr === address);
        transactions.push({
          platform: "Bitcoin Wallet",
          type: "deposit",
          asset: "BTC",
          amount: (output.value / 100000000).toString(),
          timestamp: new Date(tx.time * 1000).toISOString(),
          from_address: getFirstSenderAddressVercel(tx.inputs),
          to_address: address,
          tx_id: tx.hash,
          status: "Completed",
          network: "BTC",
          api_source: "Blockchain_Info"
        });
      }
      
      if (isWithdrawal) {
        const input = tx.inputs.find(i => i.prev_out && i.prev_out.addr === address);
        transactions.push({
          platform: "Bitcoin Wallet",
          type: "withdrawal",
          asset: "BTC",
          amount: (input.prev_out.value / 100000000).toString(),
          timestamp: new Date(tx.time * 1000).toISOString(),
          from_address: address,
          to_address: getFirstReceiverAddressVercel(tx.out, address),
          tx_id: tx.hash,
          status: "Completed",
          network: "BTC",
          api_source: "Blockchain_Info"
        });
      }
    });
    
    return transactions;
    
  } catch (error) {
    console.error("Error fetching Bitcoin transactions:", error);
    throw error;
  }
}

// ===========================================
// GOOGLE SHEETS INTEGRATION
// ===========================================

async function writeToGoogleSheets(transactions, apiStatus) {
  try {
    // Create service account credentials from environment variables
    const credentials = {
      type: "service_account",
      project_id: process.env.GOOGLE_PROJECT_ID,
      private_key_id: process.env.GOOGLE_PRIVATE_KEY_ID,
      private_key: process.env.GOOGLE_PRIVATE_KEY.replace(/\\n/g, '\n'),
      client_email: process.env.GOOGLE_CLIENT_EMAIL,
      client_id: process.env.GOOGLE_CLIENT_ID,
      auth_uri: "https://accounts.google.com/o/oauth2/auth",
      token_uri: "https://oauth2.googleapis.com/token",
      auth_provider_x509_cert_url: "https://www.googleapis.com/oauth2/v1/certs",
      client_x509_cert_url: `https://www.googleapis.com/robot/v1/metadata/x509/${process.env.GOOGLE_CLIENT_EMAIL}`
    };

    const auth = new GoogleAuth({
      credentials: credentials,
      scopes: ['https://www.googleapis.com/auth/spreadsheets']
    });

    const sheets = google.sheets({ version: 'v4', auth });
    const spreadsheetId = '1pLsxrfU5NgHF4aNLXNnCCvGgBvKO4EKjb44iiVvUp5Q';

    // Separate withdrawals and deposits
    const withdrawals = transactions.filter(tx => tx.type === 'withdrawal');
    const deposits = transactions.filter(tx => tx.type === 'deposit');

    // Write withdrawals to Withdrawals sheet
    if (withdrawals.length > 0) {
      const withdrawalRows = withdrawals.map(tx => [
        '', // Client (accountant fills)
        '', // Amount (AED) (accountant fills)  
        '', // Amount (USDT) (accountant fills)
        '', // Sell Rate (accountant fills)
        '', // Remark (accountant fills)
        tx.platform, // Platform (auto)
        tx.asset, // Asset (auto)
        parseFloat(tx.amount).toFixed(8), // Amount (auto)
        formatDateTimeVercel(tx.timestamp), // Timestamp (auto)
        tx.from_address, // From Address (auto)
        tx.to_address, // To Address (auto)
        tx.tx_id // TX ID (auto)
      ]);

      await sheets.spreadsheets.values.append({
        spreadsheetId,
        range: 'Withdrawals!A:L',
        valueInputOption: 'RAW',
        requestBody: {
          values: withdrawalRows
        }
      });
    }

    // Write deposits to Deposits sheet
    if (deposits.length > 0) {
      const depositRows = deposits.map(tx => [
        '', // Client (accountant fills)
        '', // AED (accountant fills)
        '', // USDT (accountant fills)  
        '', // Rate (accountant fills)
        '', // Remarks (accountant fills)
        tx.platform, // Platform (auto)
        tx.asset, // Asset (auto)
        parseFloat(tx.amount).toFixed(8), // Amount (auto)
        formatDateTimeVercel(tx.timestamp), // Timestamp (auto)
        tx.from_address, // From Address (auto)
        tx.to_address, // To Address (auto)
        tx.tx_id // TX ID (auto)
      ]);

      await sheets.spreadsheets.values.append({
        spreadsheetId,
        range: 'Deposits!A:L',
        valueInputOption: 'RAW',
        requestBody: {
          values: depositRows
        }
      });
    }

    // Update status in Settings sheet
    await updateSettingsStatus(sheets, spreadsheetId, apiStatus);

    return {
      success: true,
      withdrawalsAdded: withdrawals.length,
      depositsAdded: deposits.length,
      statusUpdated: true
    };

  } catch (error) {
    console.error('❌ Error writing to Google Sheets:', error);
    throw error;
  }
}

async function updateSettingsStatus(sheets, spreadsheetId, apiStatus) {
  // Update Connected Exchanges status table
  const statusUpdates = [];
  Object.entries(apiStatus).forEach(([platform, status]) => {
    statusUpdates.push([
      platform,
      status.status,
      formatDateTimeVercel(status.lastSync),
      status.autoUpdate,
      status.notes
    ]);
  });

  if (statusUpdates.length > 0) {
    await sheets.spreadsheets.values.update({
      spreadsheetId,
      range: 'SETTINGS!A3:E20', // Adjust range as needed
      valueInputOption: 'RAW',
      requestBody: {
        values: statusUpdates
      }
    });
  }
}

// ===========================================
// UTILITY FUNCTIONS
// ===========================================

function createBinanceSignatureVercel(params, secret) {
  const queryString = createQueryStringVercel(params);
  return crypto.createHmac('sha256', secret).update(queryString).digest('hex');
}

function createByBitSignatureVercel(params, secret) {
  const queryString = createQueryStringVercel(params);
  return crypto.createHmac('sha256', secret).update(queryString).digest('hex');
}

function createQueryStringVercel(params) {
  return Object.keys(params)
    .sort()
    .map(key => `${encodeURIComponent(key)}=${encodeURIComponent(params[key])}`)
    .join('&');
}

function formatDateTimeVercel(isoString) {
  const date = new Date(isoString);
  return date.toISOString().slice(0, 16).replace('T', ' ');
}

function getFirstSenderAddressVercel(inputs) {
  for (const input of inputs) {
    if (input.prev_out && input.prev_out.addr) {
      return input.prev_out.addr;
    }
  }
  return "Unknown";
}

function getFirstReceiverAddressVercel(outputs, excludeAddress) {
  for (const output of outputs) {
    if (output.addr && output.addr !== excludeAddress) {
      return output.addr;
    }
  }
  return "Unknown";
}

// ===========================================
// FILE 2: Updated Apps Script (SIMPLIFIED)
// ===========================================

/**
 * SIMPLIFIED Apps Script - Only reads and formats data
 * Vercel handles all external API calls
 */

const CONFIG = {
  SHEET_ID: "1pLsxrfU5NgHF4aNLXNnCCvGgBvKO4EKjb44iiVvUp5Q",
  FORMATTED_SHEET: "FORMATTED_TRANSACTIONS",
  WITHDRAWALS_SHEET: "Withdrawals",
  DEPOSITS_SHEET: "Deposits",
  SETTINGS_SHEET: "SETTINGS",
  
  // New Vercel endpoint for direct data writing
  VERCEL_CRYPTO_ENDPOINT: "https://zaid-cryptowallets.vercel.app/api/crypto-to-sheets"
};

/**
 * NEW MAIN FUNCTION - Triggers Vercel and formats data
 */
function mainVercelMode() {
  try {
    console.log("🚀 Starting Vercel-mode crypto data fetch...");
    
    // Step 1: Trigger Vercel to fetch and write data
    console.log("📡 Triggering Vercel data fetch...");
    triggerVercelDataFetch();
    
    // Step 2: Wait a bit for Vercel to complete
    console.log("⏳ Waiting for Vercel to complete...");
    Utilities.sleep(10000); // 10 seconds
    
    // Step 3: Consolidate and format data
    console.log("🔄 Consolidating to formatted sheet...");
    consolidateToFormatted();
    
    // Step 4: Update last execution timestamp
    updateLastExecutionTime();
    
    console.log("✅ Vercel-mode flow completed successfully");
    
  } catch (error) {
    console.error("❌ Error in Vercel-mode main function:", error);
  }
}

/**
 * Trigger Vercel endpoint to fetch and write data
 */
function triggerVercelDataFetch() {
  try {
    const response = UrlFetchApp.fetch(CONFIG.VERCEL_CRYPTO_ENDPOINT, {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      payload: JSON.stringify({
        action: "fetch_and_write",
        timestamp: new Date().toISOString()
      }),
      muteHttpExceptions: true
    });
    
    const responseText = response.getContentText();
    
    if (response.getResponseCode() !== 200) {
      throw new Error(`Vercel API error: ${response.getResponseCode()} - ${responseText}`);
    }
    
    const data = JSON.parse(responseText);
    
    if (!data.success) {
      throw new Error(`Vercel process failed: ${data.error}`);
    }
    
    console.log(`✅ Vercel processed ${data.transactions} transactions`);
    console.log("📊 Vercel result:", data.sheetsResult);
    
  } catch (error) {
    console.error("❌ Error triggering Vercel:", error);
    throw error;
  }
}

/**
 * Set up hourly trigger for Vercel-mode
 */
function setupVercelTrigger() {
  // Delete existing triggers
  ScriptApp.getProjectTriggers().forEach(trigger => {
    if (trigger.getHandlerFunction() === 'mainVercelMode') {
      ScriptApp.deleteTrigger(trigger);
    }
  });
  
  // Create new hourly trigger for Vercel-mode main function
  ScriptApp.newTrigger('mainVercelMode')
    .timeBased()
    .everyHours(1)
    .create();
  
  console.log("✅ Vercel-mode hourly trigger set up successfully");
}
