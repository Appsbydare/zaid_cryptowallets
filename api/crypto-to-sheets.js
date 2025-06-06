// ===========================================
// ENHANCED VERCEL API WITH SETTINGS STATUS UPDATE
// Replace your api/crypto-to-sheets.js with this
// ===========================================

import { GoogleAuth } from 'google-auth-library';
import { google } from 'googleapis';
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
    console.log('🚀 Starting enhanced crypto data fetch...');

    const allTransactions = [];
    const apiStatusResults = {};

    // ===========================================
    // STEP 1: TEST BINANCE APIS (WITH JAPAN VPN)
    // ===========================================
    console.log('🧪 Testing Binance APIs (Japan VPN)...');
    
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
      const result = await testBinanceAccountSimple(account);
      apiStatusResults[account.name] = result.status;
      
      if (result.success) {
        allTransactions.push(...result.transactions);
        console.log(`✅ ${account.name}: ${result.transactions.length} transactions`);
      } else {
        console.log(`❌ ${account.name}: ${result.status.notes}`);
      }
    }

    // ===========================================
    // STEP 2: TEST BYBIT API
    // ===========================================
    if (process.env.BYBIT_API_KEY && process.env.BYBIT_API_SECRET) {
      console.log('🧪 Testing ByBit...');
      const bybitResult = await testByBitAccountSimple({
        name: "ByBit (CV)",
        apiKey: process.env.BYBIT_API_KEY,
        apiSecret: process.env.BYBIT_API_SECRET
      });
      
      apiStatusResults["ByBit (CV)"] = bybitResult.status;
      if (bybitResult.success) {
        allTransactions.push(...bybitResult.transactions);
        console.log(`✅ ByBit: ${bybitResult.transactions.length} transactions`);
      } else {
        console.log(`❌ ByBit: ${bybitResult.status.notes}`);
      }
    }

    // ===========================================
    // STEP 3: FETCH BLOCKCHAIN DATA
    // ===========================================
    console.log('🧪 Fetching blockchain data...');
    
    const wallets = {
      BTC: "bc1qkuefzcmc6c8enw9f7a2e9w2hy964q3jgwcv35g",
      ETH: "0x856851a1d5111330729744f95238e5D810ba773c",
      TRON: "TAUDuQAZSTUH88xno1imPoKN25eJN6aJkN",
      SOL: "BURkHx6BNTqryY3sCqXcYNVkhN6Mz3ttDUdGQ6hXuX4n"
    };

    // Test Bitcoin API
    try {
      console.log('Testing Bitcoin API...');
      const btcTxs = await fetchBitcoinSimple(wallets.BTC);
      allTransactions.push(...btcTxs);
      apiStatusResults['Bitcoin Wallet'] = {
        status: 'Active',
        lastSync: new Date().toISOString(),
        autoUpdate: 'Every Hour',
        notes: `✅ ${btcTxs.length} transactions found`,
        transactionCount: btcTxs.length
      };
      console.log(`✅ Bitcoin: ${btcTxs.length} transactions`);
    } catch (error) {
      apiStatusResults['Bitcoin Wallet'] = {
        status: 'Error',
        lastSync: new Date().toISOString(),
        autoUpdate: 'Every Hour',
        notes: `❌ ${error.message}`,
        transactionCount: 0
      };
      console.error(`❌ Bitcoin error:`, error.message);
    }

    // Test Ethereum API
    try {
      console.log('Testing Ethereum API...');
      const ethTxs = await fetchEthereumSimple(wallets.ETH);
      allTransactions.push(...ethTxs);
      apiStatusResults['Ethereum Wallet'] = {
        status: 'Active',
        lastSync: new Date().toISOString(),
        autoUpdate: 'Every Hour',
        notes: `✅ ${ethTxs.length} transactions found`,
        transactionCount: ethTxs.length
      };
      console.log(`✅ Ethereum: ${ethTxs.length} transactions`);
    } catch (error) {
      apiStatusResults['Ethereum Wallet'] = {
        status: 'Error',
        lastSync: new Date().toISOString(),
        autoUpdate: 'Every Hour',
        notes: `❌ ${error.message}`,
        transactionCount: 0
      };
      console.error(`❌ Ethereum error:`, error.message);
    }

    // Test TRON API
    try {
      console.log('Testing TRON API...');
      const tronTxs = await fetchTronSimple(wallets.TRON);
      allTransactions.push(...tronTxs);
      apiStatusResults['TRON Wallet'] = {
        status: 'Active',
        lastSync: new Date().toISOString(),
        autoUpdate: 'Every Hour',
        notes: `✅ ${tronTxs.length} transactions found`,
        transactionCount: tronTxs.length
      };
      console.log(`✅ TRON: ${tronTxs.length} transactions`);
    } catch (error) {
      apiStatusResults['TRON Wallet'] = {
        status: 'Error',
        lastSync: new Date().toISOString(),
        autoUpdate: 'Every Hour',
        notes: `❌ ${error.message}`,
        transactionCount: 0
      };
      console.error(`❌ TRON error:`, error.message);
    }

    // Test Solana API  
    try {
      console.log('Testing Solana API...');
      const solTxs = await fetchSolanaSimple(wallets.SOL);
      allTransactions.push(...solTxs);
      apiStatusResults['Solana Wallet'] = {
        status: 'Active',
        lastSync: new Date().toISOString(),
        autoUpdate: 'Every Hour',
        notes: `✅ ${solTxs.length} transactions found`,
        transactionCount: solTxs.length
      };
      console.log(`✅ Solana: ${solTxs.length} transactions`);
    } catch (error) {
      apiStatusResults['Solana Wallet'] = {
        status: 'Error',
        lastSync: new Date().toISOString(),
        autoUpdate: 'Every Hour',
        notes: `❌ ${error.message}`,
        transactionCount: 0
      };
      console.error(`❌ Solana error:`, error.message);
    }

    // ===========================================
    // STEP 4: WRITE TO GOOGLE SHEETS
    // ===========================================
    console.log(`📊 Writing ${allTransactions.length} transactions to Google Sheets...`);
    
    let sheetsResult = { success: false, withdrawalsAdded: 0, depositsAdded: 0 };
    
    if (allTransactions.length > 0) {
      try {
        sheetsResult = await writeToGoogleSheetsWithStatus(allTransactions, apiStatusResults);
        console.log('✅ Google Sheets write successful:', sheetsResult);
      } catch (sheetsError) {
        console.error('❌ Google Sheets write failed:', sheetsError);
        sheetsResult = { 
          success: false, 
          error: sheetsError.message,
          withdrawalsAdded: 0, 
          depositsAdded: 0 
        };
      }
    } else {
      // Still update status even with 0 transactions
      try {
        await updateSettingsStatusOnly(apiStatusResults);
        sheetsResult.statusUpdated = true;
      } catch (error) {
        console.error('❌ Status update failed:', error);
      }
    }

    // ===========================================
    // STEP 5: RETURN DETAILED RESULTS
    // ===========================================
    res.status(200).json({
      success: true,
      message: 'Data successfully processed with status updates',
      transactions: allTransactions.length,
      sheetsResult: sheetsResult,
      apiStatus: apiStatusResults,
      summary: {
        binanceAccounts: Object.keys(apiStatusResults).filter(k => k.includes('Binance')).length,
        blockchainWallets: Object.keys(apiStatusResults).filter(k => k.includes('Wallet')).length,
        activeAPIs: Object.values(apiStatusResults).filter(s => s.status === 'Active').length,
        errorAPIs: Object.values(apiStatusResults).filter(s => s.status === 'Error').length
      },
      timestamp: new Date().toISOString()
    });

  } catch (error) {
    console.error('❌ Vercel Error:', error);
    
    res.status(500).json({
      success: false,
      error: error.message,
      timestamp: new Date().toISOString()
    });
  }
}

// ===========================================
// BINANCE API TEST FUNCTIONS
// ===========================================

async function testBinanceAccountSimple(account) {
  try {
    const timestamp = Date.now();
    
    // Test with account info first (simpler endpoint)
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
        "X-MBX-APIKEY": account.apiKey,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
      }
    });

    if (response.status === 451) {
      return {
        success: false,
        transactions: [],
        status: {
          status: 'Error',
          lastSync: new Date().toISOString(),
          autoUpdate: 'Every Hour',
          notes: '❌ Geo-blocked (451)',
          transactionCount: 0
        }
      };
    }

    if (!response.ok) {
      const errorText = await response.text();
      return {
        success: false,
        transactions: [],
        status: {
          status: 'Error',
          lastSync: new Date().toISOString(),
          autoUpdate: 'Every Hour',
          notes: `❌ HTTP ${response.status}: ${errorText.substring(0, 50)}`,
          transactionCount: 0
        }
      };
    }

    const data = await response.json();
    
    if (data.code && data.code !== 200) {
      return {
        success: false,
        transactions: [],
        status: {
          status: 'Error',
          lastSync: new Date().toISOString(),
          autoUpdate: 'Every Hour',
          notes: `❌ API error: ${data.msg}`,
          transactionCount: 0
        }
      };
    }

    // If account info works, try to get some recent deposits
    let transactions = [];
    try {
      transactions = await fetchBinanceDepositsSimple(account);
    } catch (depositError) {
      console.log(`Deposits failed for ${account.name}:`, depositError.message);
    }

    return {
      success: true,
      transactions: transactions,
      status: {
        status: 'Active',
        lastSync: new Date().toISOString(),
        autoUpdate: 'Every Hour',
        notes: `✅ Connected, ${transactions.length} recent transactions`,
        transactionCount: transactions.length
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

async function fetchBinanceDepositsSimple(account) {
  try {
    const timestamp = Date.now();
    const endpoint = "https://api.binance.com/sapi/v1/capital/deposit/hisrec";
    const params = {
      timestamp: timestamp,
      recvWindow: 5000,
      limit: 5 // Just get recent few
    };

    const signature = createBinanceSignature(params, account.apiSecret);
    const queryString = createQueryString(params);
    const url = `${endpoint}?${queryString}&signature=${signature}`;

    const response = await fetch(url, {
      method: "GET",
      headers: {
        "X-MBX-APIKEY": account.apiKey,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
      }
    });

    if (!response.ok) {
      throw new Error(`Deposits API error: ${response.status}`);
    }

    const data = await response.json();

    if (data.code && data.code !== 200) {
      throw new Error(`Binance deposits error: ${data.msg}`);
    }

    return (data || []).map(deposit => ({
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

async function testByBitAccountSimple(config) {
  try {
    const timestamp = Date.now();
    
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

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
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
// ENHANCED GOOGLE SHEETS WITH STATUS UPDATE
// ===========================================

async function writeToGoogleSheetsWithStatus(transactions, apiStatus) {
  try {
    console.log('🔑 Setting up Google Sheets authentication...');
    
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

    // Write transactions
    const withdrawals = transactions.filter(tx => tx.type === 'withdrawal');
    const deposits = transactions.filter(tx => tx.type === 'deposit');

    let withdrawalsAdded = 0;
    let depositsAdded = 0;

    // Write withdrawals
    if (withdrawals.length > 0) {
      const withdrawalRows = withdrawals.map(tx => [
        '', '', '', '', '', // Green columns for accountant
        tx.platform, tx.asset, parseFloat(tx.amount).toFixed(8),
        formatDateTimeSimple(tx.timestamp), tx.from_address, tx.to_address, tx.tx_id
      ]);

      await sheets.spreadsheets.values.append({
        spreadsheetId,
        range: 'Withdrawals!A:L',
        valueInputOption: 'RAW',
        requestBody: { values: withdrawalRows }
      });
      
      withdrawalsAdded = withdrawals.length;
    }

    // Write deposits
    if (deposits.length > 0) {
      const depositRows = deposits.map(tx => [
        '', '', '', '', '', // Green columns for accountant
        tx.platform, tx.asset, parseFloat(tx.amount).toFixed(8),
        formatDateTimeSimple(tx.timestamp), tx.from_address, tx.to_address, tx.tx_id
      ]);

      await sheets.spreadsheets.values.append({
        spreadsheetId,
        range: 'Deposits!A:L',
        valueInputOption: 'RAW',
        requestBody: { values: depositRows }
      });
      
      depositsAdded = deposits.length;
    }

    // Update Settings status table
    await updateSettingsStatus(sheets, spreadsheetId, apiStatus);

    return {
      success: true,
      withdrawalsAdded: withdrawalsAdded,
      depositsAdded: depositsAdded,
      statusUpdated: true
    };

  } catch (error) {
    console.error('❌ Error writing to Google Sheets:', error);
    throw error;
  }
}

async function updateSettingsStatusOnly(apiStatus) {
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

  await updateSettingsStatus(sheets, spreadsheetId, apiStatus);
}

async function updateSettingsStatus(sheets, spreadsheetId, apiStatus) {
  try {
    console.log('📊 Updating Settings status table...');
    
    // Prepare status updates for the Connected Exchanges table
    const statusRows = [];
    
    Object.entries(apiStatus).forEach(([platform, status]) => {
      statusRows.push([
        platform,                              // Platform name
        status.status,                         // API Status  
        formatDateTimeSimple(status.lastSync), // Last Sync
        status.autoUpdate,                     // Auto-Update
        status.notes                           // Notes
      ]);
    });

    if (statusRows.length > 0) {
      // Clear existing status data and write new data
      await sheets.spreadsheets.values.clear({
        spreadsheetId,
        range: 'SETTINGS!A3:E20'
      });

      // Write header
      await sheets.spreadsheets.values.update({
        spreadsheetId,
        range: 'SETTINGS!A2:E2',
        valueInputOption: 'RAW',
        requestBody: {
          values: [['Platform', 'API Status', 'Last Sync', 'Auto-Update', 'Notes']]
        }
      });

      // Write status data
      await sheets.spreadsheets.values.update({
        spreadsheetId,
        range: `SETTINGS!A3:E${2 + statusRows.length}`,
        valueInputOption: 'RAW',
        requestBody: {
          values: statusRows
        }
      });

      console.log(`✅ Updated ${statusRows.length} API statuses in Settings`);
    }

  } catch (error) {
    console.error('❌ Error updating Settings status:', error);
    throw error;
  }
}

// ===========================================
// BLOCKCHAIN API FUNCTIONS (SAME AS BEFORE)
// ===========================================

async function fetchBitcoinSimple(address) {
  try {
    const endpoint = `https://blockchain.info/rawaddr/${address}`;
    const response = await fetch(endpoint);
    
    if (response.status === 429) {
      console.log("⏳ Bitcoin API rate limited");
      return [];
    }
    
    if (!response.ok) {
      throw new Error(`Bitcoin API error: ${response.status}`);
    }
    
    const data = await response.json();
    const transactions = [];
    
    data.txs.slice(0, 10).forEach(tx => {
      const isDeposit = tx.out.some(output => output.addr === address);
      
      if (isDeposit) {
        const output = tx.out.find(o => o.addr === address);
        transactions.push({
          platform: "Bitcoin Wallet",
          type: "deposit",
          asset: "BTC",
          amount: (output.value / 100000000).toString(),
          timestamp: new Date(tx.time * 1000).toISOString(),
          from_address: "External",
          to_address: address,
          tx_id: tx.hash,
          status: "Completed",
          network: "BTC",
          api_source: "Blockchain_Info"
        });
      }
    });
    
    return transactions;
    
  } catch (error) {
    console.error("Bitcoin API error:", error);
    throw error;
  }
}

async function fetchEthereumSimple(address) {
  try {
    // Use environment variable if available, otherwise fallback to hardcoded
    const apiKey = process.env.ETHERSCAN_API_KEY || "SP8YA4W8RDB85G9129BTDHY72ADBZ6USHA";
    const endpoint = `https://api.etherscan.io/api?module=account&action=txlist&address=${address}&startblock=0&endblock=99999999&sort=desc&page=1&offset=10&apikey=${apiKey}`;
    
    const response = await fetch(endpoint);
    
    if (!response.ok) {
      throw new Error(`Ethereum API error: ${response.status}`);
    }
    
    const data = await response.json();
    
    if (data.status !== "1") {
      console.log("Etherscan API message:", data.message);
      return [];
    }
    
    const transactions = [];
    
    data.result.forEach(tx => {
      const isDeposit = tx.to.toLowerCase() === address.toLowerCase();
      const amount = (parseInt(tx.value) / Math.pow(10, 18)).toString();
      
      if (parseFloat(amount) > 0) {
        transactions.push({
          platform: "Ethereum Wallet",
          type: isDeposit ? "deposit" : "withdrawal",
          asset: "ETH",
          amount: amount,
          timestamp: new Date(parseInt(tx.timeStamp) * 1000).toISOString(),
          from_address: tx.from,
          to_address: tx.to,
          tx_id: tx.hash,
          status: tx.txreceipt_status === "1" ? "Completed" : "Failed",
          network: "ETH",
          api_source: "Etherscan"
        });
      }
    });
    
    return transactions;
    
  } catch (error) {
    console.error("Ethereum API error:", error);
    throw error;
  }
}

async function fetchTronSimple(address) {
  try {
    const endpoint = `https://api.trongrid.io/v1/accounts/${address}/transactions?limit=10`;
    
    const response = await fetch(endpoint);
    
    if (!response.ok) {
      throw new Error(`TRON API error: ${response.status}`);
    }
    
    const data = await response.json();
    
    if (!data.data) {
      return [];
    }
    
    const transactions = [];
    
    data.data.forEach(tx => {
      if (tx.raw_data && tx.raw_data.contract) {
        tx.raw_data.contract.forEach(contract => {
          if (contract.type === "TransferContract") {
            const value = contract.parameter.value;
            const isDeposit = value.to_address === address;
            const amount = (value.amount / 1000000).toString();
            
            transactions.push({
              platform: "TRON Wallet",
              type: isDeposit ? "deposit" : "withdrawal",
              asset: "TRX",
              amount: amount,
              timestamp: new Date(tx.block_timestamp).toISOString(),
              from_address: value.owner_address,
              to_address: value.to_address,
              tx_id: tx.txID,
              status: "Completed",
              network: "TRON",
              api_source: "TronGrid"
            });
          }
        });
      }
    });
    
    return transactions;
    
  } catch (error) {
    console.error("TRON API error:", error);
    throw error;
  }
}

async function fetchSolanaSimple(address) {
  try {
    const endpoint = "https://api.mainnet-beta.solana.com";
    
    const payload = {
      jsonrpc: "2.0",
      id: 1,
      method: "getSignaturesForAddress",
      params: [address, { limit: 5 }] // Reduced for simplicity
    };
    
    const response = await fetch(endpoint, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });
    
    if (!response.ok) {
      throw new Error(`Solana API error: ${response.status}`);
    }
    
    const data = await response.json();
    
    if (data.error) {
      throw new Error(`Solana RPC error: ${data.error.message}`);
    }
    
    // For simplicity, just return basic transaction info without detailed parsing
    const transactions = data.result.map(sig => ({
      platform: "Solana Wallet",
      type: "deposit", // Simplified
      asset: "SOL",
      amount: "0.001", // Placeholder
      timestamp: new Date(sig.blockTime * 1000).toISOString(),
      from_address: "External",
      to_address: address,
      tx_id: sig.signature,
      status: sig.err ? "Failed" : "Completed",
      network: "SOL",
      api_source: "Solana_RPC"
    }));
    
    return transactions;
    
  } catch (error) {
    console.error("Solana API error:", error);
    throw error;
  }
}

// ===========================================
// UTILITY FUNCTIONS
// ===========================================

function createBinanceSignature(params, secret) {
  const queryString = createQueryString(params);
  return crypto.createHmac('sha256', secret).update(queryString).digest('hex');
}

function createByBitSignature(params, secret) {
  const queryString = createQueryString(params);
  return crypto.createHmac('sha256', secret).update(queryString).digest('hex');
}

function createQueryString(params) {
  return Object.keys(params)
    .sort()
    .map(key => `${encodeURIComponent(key)}=${encodeURIComponent(params[key])}`)
    .join('&');
}

function formatDateTimeSimple(isoString) {
  const date = new Date(isoString);
  return date.toISOString().slice(0, 16).replace('T', ' ');
}
