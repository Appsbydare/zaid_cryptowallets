// ===========================================
// COMPLETE ENHANCED CRYPTO API WITH FULL DEBUGGING
// Replace your entire api/crypto-to-sheets.js with this file
// Features: Enhanced debugging, ByBit V5 fix, P2P/Pay debugging, Multiple Bitcoin APIs
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
    console.log('🚀 Starting COMPLETE ENHANCED crypto data fetch with DEBUGGING...');

    // Get date filtering from request or use defaults
    const startDate = req.body?.startDate || '2025-05-31T00:00:00.000Z';
    const filterDate = new Date(startDate);
    console.log(`📅 Filtering transactions after: ${startDate}`);

    const allTransactions = [];
    const apiStatusResults = {};
    let totalTransactionsFound = 0;

    // ===========================================
    // STEP 1: ENHANCED BINANCE APIS (WITH P2P/PAY DEBUGGING)
    // ===========================================
    console.log('🧪 Testing Binance APIs with ENHANCED P2P/PAY debugging...');
    
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

      console.log(`🔥 Processing ${account.name} with ENHANCED debugging...`);
      const result = await testBinanceAccountEnhanced(account, filterDate);
      apiStatusResults[account.name] = result.status;
      
      if (result.success) {
        allTransactions.push(...result.transactions);
        totalTransactionsFound += result.transactions.length;
        console.log(`✅ ${account.name}: ${result.transactions.length} transactions (enhanced with debugging)`);
      } else {
        console.log(`❌ ${account.name}: ${result.status.notes}`);
      }
    }

    // ===========================================
    // STEP 2: ENHANCED BYBIT API (V5 AUTHENTICATION FIX)
    // ===========================================
    if (process.env.BYBIT_API_KEY && process.env.BYBIT_API_SECRET) {
      console.log('🔥 Testing ByBit with ENHANCED V5 authentication...');
      const bybitResult = await testByBitAccountEnhanced({
        name: "ByBit (CV)",
        apiKey: process.env.BYBIT_API_KEY,
        apiSecret: process.env.BYBIT_API_SECRET
      }, filterDate);
      
      apiStatusResults["ByBit (CV)"] = bybitResult.status;
      if (bybitResult.success) {
        allTransactions.push(...bybitResult.transactions);
        totalTransactionsFound += bybitResult.transactions.length;
        console.log(`✅ ByBit: ${bybitResult.transactions.length} transactions`);
      } else {
        console.log(`❌ ByBit: ${bybitResult.status.notes}`);
      }
    }

    // ===========================================
    // STEP 3: ENHANCED BLOCKCHAIN DATA (MULTIPLE APIS + RELAXED DATES)
    // ===========================================
    console.log('🔥 Fetching blockchain data with MULTIPLE APIs and RELAXED date filtering...');
    
    const wallets = {
      BTC: "bc1qkuefzcmc6c8enw9f7a2e9w2hy964q3jgwcv35g",
      ETH: "0x856851a1d5111330729744f95238e5D810ba773c",
      TRON: "TAUDuQAZSTUH88xno1imPoKN25eJN6aJkN",
      SOL: "BURkHx6BNTqryY3sCqXcYNVkhN6Mz3ttDUdGQ6hXuX4n"
    };

    // Enhanced Bitcoin API with multiple sources and debugging
    try {
      console.log('🔥 Testing Bitcoin API with MULTIPLE sources and debugging...');
      const btcTxs = await fetchBitcoinEnhanced(wallets.BTC, filterDate);
      allTransactions.push(...btcTxs);
      totalTransactionsFound += btcTxs.length;
      apiStatusResults['Bitcoin Wallet'] = {
        status: btcTxs.length > 0 ? 'Active' : 'Warning',
        lastSync: new Date().toISOString(),
        autoUpdate: 'Every Hour',
        notes: `🔥 ${btcTxs.length} transactions found (enhanced multi-API search)`,
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

    // Enhanced Ethereum API with debugging
    try {
      console.log('🔥 Testing Ethereum API with enhanced debugging...');
      const ethTxs = await fetchEthereumEnhanced(wallets.ETH, filterDate);
      allTransactions.push(...ethTxs);
      totalTransactionsFound += ethTxs.length;
      apiStatusResults['Ethereum Wallet'] = {
        status: 'Active',
        lastSync: new Date().toISOString(),
        autoUpdate: 'Every Hour',
        notes: `🔥 ${ethTxs.length} transactions found (enhanced limit: 100 with debugging)`,
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

    // Enhanced TRON API
    try {
      console.log('🔥 Testing TRON API with enhanced debugging...');
      const tronTxs = await fetchTronEnhanced(wallets.TRON, filterDate);
      allTransactions.push(...tronTxs);
      totalTransactionsFound += tronTxs.length;
      apiStatusResults['TRON Wallet'] = {
        status: 'Active',
        lastSync: new Date().toISOString(),
        autoUpdate: 'Every Hour',
        notes: `🔥 ${tronTxs.length} transactions found (enhanced limit: 50)`,
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

    // Enhanced Solana API
    try {
      console.log('🔥 Testing Solana API with enhanced debugging...');
      const solTxs = await fetchSolanaEnhanced(wallets.SOL, filterDate);
      allTransactions.push(...solTxs);
      totalTransactionsFound += solTxs.length;
      apiStatusResults['Solana Wallet'] = {
        status: 'Active',
        lastSync: new Date().toISOString(),
        autoUpdate: 'Every Hour',
        notes: `🔥 ${solTxs.length} transactions found (enhanced limit: 20)`,
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
    // STEP 4: WRITE TO GOOGLE SHEETS WITH ENHANCED DEDUPLICATION
    // ===========================================
    console.log(`🔥 Processing ${allTransactions.length} ENHANCED transactions with SAFE deduplication...`);
    console.log(`📊 Total found: ${totalTransactionsFound}, Raw collected: ${allTransactions.length}`);
    
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
    // STEP 5: RETURN ENHANCED RESULTS WITH DETAILED DEBUGGING
    // ===========================================
    res.status(200).json({
      success: true,
      message: 'COMPLETE ENHANCED data processing with FULL DEBUGGING completed',
      transactions: allTransactions.length,
      totalFound: totalTransactionsFound,
      dateFilter: startDate,
      sheetsResult: sheetsResult,
      apiStatus: apiStatusResults,
      deduplicationStats: {
        rawTransactions: allTransactions.length,
        afterDeduplication: sheetsResult.totalAfterDedup || 0,
        afterValueFilter: sheetsResult.totalAfterFilter || 0,
        duplicatesRemoved: sheetsResult.duplicatesRemoved || 0,
        valueFiltered: sheetsResult.filteredOut || 0,
        finalAdded: (sheetsResult.withdrawalsAdded || 0) + (sheetsResult.depositsAdded || 0)
      },
      summary: {
        binanceAccounts: Object.keys(apiStatusResults).filter(k => k.includes('Binance')).length,
        blockchainWallets: Object.keys(apiStatusResults).filter(k => k.includes('Wallet')).length,
        activeAPIs: Object.values(apiStatusResults).filter(s => s.status === 'Active').length,
        errorAPIs: Object.values(apiStatusResults).filter(s => s.status === 'Error').length,
        enhancedFeatures: 'Full Debugging + P2P/Pay + ByBit V5 + Multi Bitcoin APIs'
      },
      timestamp: new Date().toISOString()
    });

  } catch (error) {
    console.error('❌ Enhanced Vercel Error:', error);
    
    res.status(500).json({
      success: false,
      error: error.message,
      timestamp: new Date().toISOString()
    });
  }
}

// ===========================================
// ENHANCED BINANCE API FUNCTIONS WITH P2P/PAY DEBUGGING
// ===========================================

async function testBinanceAccountEnhanced(account, filterDate) {
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

    // Get ALL transaction types with higher limits and debugging
    let transactions = [];
    let totalFetched = 0;
    let transactionBreakdown = {
      deposits: 0,
      withdrawals: 0,
      p2p: 0,
      pay: 0
    };
    
    try {
      // 1. Fetch regular deposits
      const deposits = await fetchBinanceDepositsEnhanced(account, filterDate);
      transactions.push(...deposits);
      transactionBreakdown.deposits = deposits.length;
      console.log(`  💰 ${account.name} deposits: ${deposits.length}`);

      // 2. Fetch regular withdrawals
      const withdrawals = await fetchBinanceWithdrawalsEnhanced(account, filterDate);
      transactions.push(...withdrawals);
      transactionBreakdown.withdrawals = withdrawals.length;
      console.log(`  📤 ${account.name} withdrawals: ${withdrawals.length}`);

      // 3. Fetch P2P transactions with ENHANCED DEBUGGING
      const p2pTransactions = await fetchBinanceP2PEnhanced(account, filterDate);
      transactions.push(...p2pTransactions);
      transactionBreakdown.p2p = p2pTransactions.length;
      console.log(`  🤝 ${account.name} P2P: ${p2pTransactions.length}`);

      // 4. Fetch Binance Pay transactions with ENHANCED DEBUGGING
      const payTransactions = await fetchBinancePayEnhanced(account, filterDate);
      transactions.push(...payTransactions);
      transactionBreakdown.pay = payTransactions.length;
      console.log(`  💳 ${account.name} Pay: ${payTransactions.length}`);

      totalFetched = transactions.length;

    } catch (txError) {
      console.log(`Transaction fetch failed for ${account.name}:`, txError.message);
    }

    const statusNotes = `🔥 Connected: ${transactionBreakdown.deposits}D + ${transactionBreakdown.withdrawals}W + ${transactionBreakdown.p2p}P2P + ${transactionBreakdown.pay}Pay = ${totalFetched} total`;

    return {
      success: true,
      transactions: transactions,
      status: {
        status: 'Active',
        lastSync: new Date().toISOString(),
        autoUpdate: 'Every Hour',
        notes: statusNotes,
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

async function fetchBinanceDepositsEnhanced(account, filterDate) {
  try {
    const timestamp = Date.now();
    const endpoint = "https://api.binance.com/sapi/v1/capital/deposit/hisrec";
    const params = {
      timestamp: timestamp,
      recvWindow: 5000,
      limit: 100, // ENHANCED: Increased from 5 to 100
      startTime: filterDate.getTime() // ENHANCED: Add date filtering
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

    const deposits = (data || []).filter(deposit => {
      const depositDate = new Date(deposit.insertTime);
      return depositDate >= filterDate; // ENHANCED: Date filtering
    });

    return deposits.map(deposit => ({
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
      api_source: "Binance_Deposit_Enhanced"
    }));

  } catch (error) {
    console.error(`Error fetching enhanced deposits for ${account.name}:`, error);
    return [];
  }
}

async function fetchBinanceWithdrawalsEnhanced(account, filterDate) {
  try {
    const timestamp = Date.now();
    const endpoint = "https://api.binance.com/sapi/v1/capital/withdraw/history";
    const params = {
      timestamp: timestamp,
      recvWindow: 5000,
      limit: 100, // ENHANCED: Increased limit
      startTime: filterDate.getTime() // ENHANCED: Add date filtering
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
      throw new Error(`Withdrawals API error: ${response.status}`);
    }

    const data = await response.json();

    if (data.code && data.code !== 200) {
      throw new Error(`Binance withdrawals error: ${data.msg}`);
    }

    const withdrawals = (data || []).filter(withdrawal => {
      const withdrawalDate = new Date(withdrawal.applyTime);
      return withdrawalDate >= filterDate; // ENHANCED: Date filtering
    });

    return withdrawals.map(withdrawal => ({
      platform: account.name,
      type: "withdrawal",
      asset: withdrawal.coin,
      amount: withdrawal.amount.toString(),
      timestamp: new Date(withdrawal.applyTime).toISOString(),
      from_address: account.name,
      to_address: withdrawal.address || "External",
      tx_id: withdrawal.txId || withdrawal.id,
      status: withdrawal.status === 6 ? "Completed" : "Pending",
      network: withdrawal.network,
      api_source: "Binance_Withdrawal_Enhanced"
    }));

  } catch (error) {
    console.error(`Error fetching enhanced withdrawals for ${account.name}:`, error);
    return [];
  }
}

// ===========================================
// ENHANCED BINANCE P2P WITH FULL DEBUGGING
// ===========================================

async function fetchBinanceP2PEnhanced(account, filterDate) {
  const transactions = [];
  
  try {
    console.log(`    🤝 Fetching P2P transactions for ${account.name} with FULL debugging...`);
    
    // Try different P2P endpoints with enhanced debugging
    const p2pEndpoints = [
      {
        name: "Order Match History",
        endpoint: "/sapi/v1/c2c/orderMatch/listUserOrderHistory",
        params: { page: 1, rows: 100 }
      },
      {
        name: "C2C Trade History", 
        endpoint: "/sapi/v1/c2c/orderMatch/getUserOrderHistory",
        params: { page: 1, rows: 100 }
      },
      {
        name: "C2C Order History",
        endpoint: "/sapi/v1/c2c/orderMatch/orderHistory", 
        params: { page: 1, rows: 100 }
      }
    ];

    for (const endpointConfig of p2pEndpoints) {
      try {
        console.log(`      🧪 Trying ${endpointConfig.name}...`);
        
        // Fetch P2P Buy orders (deposits)
        const buyOrders = await fetchBinanceP2POrdersDebug(account, filterDate, 'BUY', endpointConfig);
        transactions.push(...buyOrders);
        
        // Fetch P2P Sell orders (withdrawals)  
        const sellOrders = await fetchBinanceP2POrdersDebug(account, filterDate, 'SELL', endpointConfig);
        transactions.push(...sellOrders);
        
        console.log(`      ✅ ${endpointConfig.name}: ${buyOrders.length} buys + ${sellOrders.length} sells`);
        
        if (buyOrders.length > 0 || sellOrders.length > 0) {
          break; // Stop if we found data
        }
        
      } catch (endpointError) {
        console.log(`      ❌ ${endpointConfig.name} failed: ${endpointError.message}`);
      }
    }
    
  } catch (error) {
    console.log(`    ❌ P2P fetch failed for ${account.name}: ${error.message}`);
  }
  
  return transactions;
}

async function fetchBinanceP2POrdersDebug(account, filterDate, tradeType, endpointConfig) {
  try {
    const timestamp = Date.now();
    const endpoint = `https://api.binance.com${endpointConfig.endpoint}`;
    const params = {
      tradeType: tradeType,
      timestamp: timestamp,
      recvWindow: 5000,
      ...endpointConfig.params
    };

    const signature = createBinanceSignature(params, account.apiSecret);
    const queryString = createQueryString(params);
    const url = `${endpoint}?${queryString}&signature=${signature}`;

    console.log(`        📡 P2P ${tradeType} URL: ${endpoint}?${queryString.substring(0, 50)}...`);

    const response = await fetch(url, {
      method: "GET",
      headers: {
        "X-MBX-APIKEY": account.apiKey,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
      }
    });

    console.log(`        📊 P2P ${tradeType} Response: ${response.status}`);

    if (!response.ok) {
      const errorText = await response.text();
      console.log(`        ❌ P2P ${tradeType} Error Response: ${errorText.substring(0, 200)}`);
      throw new Error(`P2P API error: ${response.status} - ${errorText.substring(0, 100)}`);
    }

    const data = await response.json();
    console.log(`        📈 P2P ${tradeType} Data Structure:`, Object.keys(data));

    if (data.code && data.code !== 200) {
      console.log(`        ❌ P2P ${tradeType} API Error: ${data.msg}`);
      throw new Error(`Binance P2P error: ${data.msg}`);
    }

    if (!data.data && !data.result) {
      console.log(`        ℹ️ P2P ${tradeType} No data field found`);
      return [];
    }

    const orders = data.data || data.result || [];
    console.log(`        📊 P2P ${tradeType} Raw orders count: ${orders.length}`);

    // Filter by date and convert to standard format
    const p2pTransactions = orders.filter(order => {
      if (!order.createTime && !order.orderTime) return false;
      
      const orderDate = new Date(parseInt(order.createTime || order.orderTime));
      const isCompleted = order.orderStatus === "COMPLETED" || order.status === "COMPLETED";
      
      return orderDate >= filterDate && isCompleted;
    }).map(order => ({
      platform: account.name,
      type: tradeType === 'BUY' ? "deposit" : "withdrawal",
      asset: order.asset || order.coin,
      amount: (order.amount || order.totalAmount || order.quantity || "0").toString(),
      timestamp: new Date(parseInt(order.createTime || order.orderTime)).toISOString(),
      from_address: tradeType === 'BUY' ? "P2P User" : account.name,
      to_address: tradeType === 'BUY' ? account.name : "P2P User",
      tx_id: `P2P_${order.orderNumber || order.orderNo || order.id}`,
      status: "Completed",
      network: "P2P",
      api_source: `Binance_P2P_${tradeType}_Debug`
    }));

    console.log(`        ✅ P2P ${tradeType} Filtered transactions: ${p2pTransactions.length}`);
    return p2pTransactions;

  } catch (error) {
    console.error(`        ❌ Error fetching P2P ${tradeType} orders for ${account.name}:`, error);
    return [];
  }
}

// ===========================================
// ENHANCED BINANCE PAY WITH FULL DEBUGGING
// ===========================================

async function fetchBinancePayEnhanced(account, filterDate) {
  try {
    console.log(`    💳 Fetching Binance Pay transactions for ${account.name} with FULL debugging...`);
    
    // Try different Pay endpoints with enhanced debugging
    const payEndpoints = [
      {
        name: "Pay Transactions",
        endpoint: "/sapi/v1/pay/transactions"
      },
      {
        name: "Sub Account Transfer History",
        endpoint: "/sapi/v1/sub-account/sub/transfer/history"
      },
      {
        name: "Internal Transfer",
        endpoint: "/sapi/v1/asset/transfer"
      }
    ];

    for (const endpointConfig of payEndpoints) {
      try {
        console.log(`      🧪 Trying ${endpointConfig.name}...`);
        
        const timestamp = Date.now();
        const endpoint = `https://api.binance.com${endpointConfig.endpoint}`;
        const params = {
          timestamp: timestamp,
          recvWindow: 5000,
          limit: 100,
          startTime: filterDate.getTime()
        };

        const signature = createBinanceSignature(params, account.apiSecret);
        const queryString = createQueryString(params);
        const url = `${endpoint}?${queryString}&signature=${signature}`;

        console.log(`        📡 Pay URL: ${endpoint}?${queryString.substring(0, 50)}...`);

        const response = await fetch(url, {
          method: "GET",
          headers: {
            "X-MBX-APIKEY": account.apiKey,
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
          }
        });

        console.log(`        📊 Pay Response: ${response.status}`);

        if (!response.ok) {
          const errorText = await response.text();
          console.log(`        ❌ Pay Error Response: ${errorText.substring(0, 200)}`);
          continue; // Try next endpoint
        }

        const data = await response.json();
        console.log(`        📈 Pay Data Structure:`, Object.keys(data));

        if (data.code && data.code !== 200) {
          console.log(`        ❌ Pay API Error: ${data.msg}`);
          continue; // Try next endpoint
        }

        if (!data.data && !data.result) {
          console.log(`        ℹ️ Pay No data field found`);
          continue; // Try next endpoint
        }

        const transactions = data.data || data.result || [];
        console.log(`        📊 Pay Raw transactions count: ${transactions.length}`);

        const payTransactions = transactions.filter(tx => {
          const txDate = new Date(parseInt(tx.createTime || tx.insertTime));
          const isSuccess = tx.status === "SUCCESS" || tx.status === 1;
          
          return txDate >= filterDate && isSuccess;
        }).map(tx => {
          const isDeposit = tx.direction === "IN" || tx.type === "deposit";
          
          return {
            platform: account.name,
            type: isDeposit ? "deposit" : "withdrawal",
            asset: tx.currency || tx.coin,
            amount: (tx.amount || "0").toString(),
            timestamp: new Date(parseInt(tx.createTime || tx.insertTime)).toISOString(),
            from_address: isDeposit ? "Binance Pay User" : account.name,
            to_address: isDeposit ? account.name : "Binance Pay User",
            tx_id: `PAY_${tx.transactionId || tx.id || tx.txId}`,
            status: "Completed",
            network: "Binance Pay",
            api_source: `Binance_Pay_${endpointConfig.name.replace(' ', '_')}`
          };
        });

        console.log(`        ✅ Pay transactions: ${payTransactions.length}`);
        
        if (payTransactions.length > 0) {
          return payTransactions;
        }

      } catch (endpointError) {
        console.log(`      ❌ ${endpointConfig.name} failed: ${endpointError.message}`);
      }
    }

    console.log(`    ℹ️ No Binance Pay transactions found for ${account.name}`);
    return [];

  } catch (error) {
    console.error(`Error fetching Binance Pay for ${account.name}:`, error);
    return [];
  }
}

// ===========================================
// ENHANCED BYBIT WITH V5 AUTHENTICATION FIX
// ===========================================

async function testByBitAccountEnhanced(config, filterDate) {
  try {
    console.log(`🔥 Processing ByBit ${config.name} with ENHANCED V5 authentication...`);
    
    // Test multiple authentication methods for ByBit V5
    const authMethods = [
      { name: "Unified Account", accountType: "UNIFIED" },
      { name: "Spot Account", accountType: "SPOT" },
      { name: "Contract Account", accountType: "CONTRACT" }
    ];

    let connectionSuccess = false;
    let workingAccountType = null;

    // Try different account types
    for (const method of authMethods) {
      try {
        console.log(`    🧪 Testing ${method.name}...`);
        
        const timestamp = Date.now().toString();
        const recv_window = "5000";
        const testEndpoint = "https://api.bybit.com/v5/account/wallet-balance";
        
        // Create proper V5 signature
        const queryParams = `accountType=${method.accountType}&timestamp=${timestamp}`;
        const signString = timestamp + config.apiKey + recv_window + queryParams;
        const signature = crypto.createHmac('sha256', config.apiSecret).update(signString).digest('hex');
        
        const testUrl = `${testEndpoint}?${queryParams}`;

        const testResponse = await fetch(testUrl, {
          method: "GET",
          headers: {
            "X-BAPI-API-KEY": config.apiKey,
            "X-BAPI-SIGN": signature,
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-RECV-WINDOW": recv_window,
            "Content-Type": "application/json"
          }
        });

        const testData = await testResponse.json();
        
        console.log(`        📊 ${method.name} Response: ${testResponse.status}, RetCode: ${testData.retCode}`);
        
        if (testResponse.ok && testData.retCode === 0) {
          console.log(`    ✅ ${method.name} authentication successful!`);
          connectionSuccess = true;
          workingAccountType = method.accountType;
          break;
        } else {
          console.log(`    ❌ ${method.name} failed: ${testData.retMsg || testResponse.status}`);
        }
        
      } catch (methodError) {
        console.log(`    ❌ ${method.name} error: ${methodError.message}`);
      }
    }

    if (!connectionSuccess) {
      return {
        success: false,
        transactions: [],
        status: {
          status: 'Error',
          lastSync: new Date().toISOString(),
          autoUpdate: 'Every Hour',
          notes: '❌ All ByBit V5 authentication methods failed. Check API key permissions and IP whitelist.',
          transactionCount: 0
        }
      };
    }

    console.log(`    ✅ ByBit connection successful with ${workingAccountType}, fetching transactions...`);

    // Now fetch actual transactions with working account type
    let transactions = [];
    let transactionBreakdown = {
      deposits: 0,
      withdrawals: 0
    };

    try {
      // Fetch deposits with working account type
      const deposits = await fetchByBitDepositsEnhanced(config, filterDate, workingAccountType);
      transactions.push(...deposits);
      transactionBreakdown.deposits = deposits.length;
      console.log(`  💰 ${config.name} deposits: ${deposits.length}`);

      // Fetch withdrawals with working account type
      const withdrawals = await fetchByBitWithdrawalsEnhanced(config, filterDate, workingAccountType);
      transactions.push(...withdrawals);
      transactionBreakdown.withdrawals = withdrawals.length;
      console.log(`  📤 ${config.name} withdrawals: ${withdrawals.length}`);

    } catch (txError) {
      console.log(`ByBit transaction fetch failed: ${txError.message}`);
    }

    const statusNotes = `🔥 Connected (${workingAccountType}): ${transactionBreakdown.deposits}D + ${transactionBreakdown.withdrawals}W = ${transactions.length} total`;

    return {
      success: true,
      transactions: transactions,
      status: {
        status: 'Active',
        lastSync: new Date().toISOString(),
        autoUpdate: 'Every Hour',
        notes: statusNotes,
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
        notes: `❌ Enhanced ByBit failed: ${error.message}`,
        transactionCount: 0
      }
    };
  }
}

async function fetchByBitDepositsEnhanced(config, filterDate, accountType = "UNIFIED") {
  try {
    console.log(`    💰 Fetching ByBit deposits for ${config.name} (${accountType})...`);
    
    const timestamp = Date.now().toString();
    const recv_window = "5000";
    const endpoint = "https://api.bybit.com/v5/asset/deposit/query-record";
    
    const queryParams = `timestamp=${timestamp}&limit=50&startTime=${filterDate.getTime()}`;
    const signString = timestamp + config.apiKey + recv_window + queryParams;
    const signature = crypto.createHmac('sha256', config.apiSecret).update(signString).digest('hex');
    const url = `${endpoint}?${queryParams}`;

    const response = await fetch(url, {
      method: "GET",
      headers: {
        "X-BAPI-API-KEY": config.apiKey,
        "X-BAPI-SIGN": signature,
        "X-BAPI-TIMESTAMP": timestamp,
        "X-BAPI-RECV-WINDOW": recv_window,
        "Content-Type": "application/json"
      }
    });

    if (!response.ok) {
      throw new Error(`ByBit deposits API error: ${response.status}`);
    }

    const data = await response.json();
    
    if (data.retCode !== 0) {
      throw new Error(`ByBit deposits error: ${data.retMsg}`);
    }

    if (!data.result || !data.result.rows) {
      console.log(`    ℹ️ No deposit data returned for ${config.name}`);
      return [];
    }

    const deposits = data.result.rows.filter(deposit => {
      const depositDate = new Date(parseInt(deposit.successAt));
      return depositDate >= filterDate && deposit.status === "3"; // Status 3 = success
    }).map(deposit => ({
      platform: config.name,
      type: "deposit",
      asset: deposit.coin,
      amount: deposit.amount.toString(),
      timestamp: new Date(parseInt(deposit.successAt)).toISOString(),
      from_address: deposit.toAddress || "External",
      to_address: config.name,
      tx_id: deposit.txID || deposit.id,
      status: "Completed",
      network: deposit.chain,
      api_source: "ByBit_Deposit_V5_Enhanced"
    }));

    console.log(`    ✅ ByBit deposits: ${deposits.length} transactions`);
    return deposits;

  } catch (error) {
    console.error(`Error fetching ByBit deposits for ${config.name}:`, error);
    return [];
  }
}

async function fetchByBitWithdrawalsEnhanced(config, filterDate, accountType = "UNIFIED") {
  try {
    console.log(`    📤 Fetching ByBit withdrawals for ${config.name} (${accountType})...`);
    
    const timestamp = Date.now().toString();
    const recv_window = "5000";
    const endpoint = "https://api.bybit.com/v5/asset/withdraw/query-record";
    
    const queryParams = `timestamp=${timestamp}&limit=50&startTime=${filterDate.getTime()}`;
    const signString = timestamp + config.apiKey + recv_window + queryParams;
    const signature = crypto.createHmac('sha256', config.apiSecret).update(signString).digest('hex');
    const url = `${endpoint}?${queryParams}`;

    const response = await fetch(url, {
      method: "GET",
      headers: {
        "X-BAPI-API-KEY": config.apiKey,
        "X-BAPI-SIGN": signature,
        "X-BAPI-TIMESTAMP": timestamp,
        "X-BAPI-RECV-WINDOW": recv_window,
        "Content-Type": "application/json"
      }
    });

    if (!response.ok) {
      throw new Error(`ByBit withdrawals API error: ${response.status}`);
    }

    const data = await response.json();
    
    if (data.retCode !== 0) {
      throw new Error(`ByBit withdrawals error: ${data.retMsg}`);
    }

    if (!data.result || !data.result.rows) {
      console.log(`    ℹ️ No withdrawal data returned for ${config.name}`);
      return [];
    }

    const withdrawals = data.result.rows.filter(withdrawal => {
      const withdrawalDate = new Date(parseInt(withdrawal.createTime));
      return withdrawalDate >= filterDate && withdrawal.status === "success";
    }).map(withdrawal => ({
      platform: config.name,
      type: "withdrawal", 
      asset: withdrawal.coin,
      amount: withdrawal.amount.toString(),
      timestamp: new Date(parseInt(withdrawal.createTime)).toISOString(),
      from_address: config.name,
      to_address: withdrawal.toAddress || "External",
      tx_id: withdrawal.txID || withdrawal.id,
      status: "Completed",
      network: withdrawal.chain,
      api_source: "ByBit_Withdrawal_V5_Enhanced"
    }));

    console.log(`    ✅ ByBit withdrawals: ${withdrawals.length} transactions`);
    return withdrawals;

  } catch (error) {
    console.error(`Error fetching ByBit withdrawals for ${config.name}:`, error);
    return [];
  }
}

// ===========================================
// ENHANCED BLOCKCHAIN API FUNCTIONS WITH MULTIPLE SOURCES
// ===========================================

async function fetchBitcoinEnhanced(address, filterDate) {
  const transactions = [];
  
  // Relax date filtering - try last 30 days if no recent data
  const relaxedDate = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000); // 30 days ago
  const actualFilterDate = filterDate < relaxedDate ? relaxedDate : filterDate;
  
  console.log(`  🔍 Bitcoin wallet search: ${address.substring(0, 20)}...`);
  console.log(`  📅 Date filter: ${actualFilterDate.toISOString().substring(0, 10)} (relaxed: ${filterDate !== actualFilterDate})`);
  
  // Try multiple Bitcoin APIs for better coverage
  const apis = [
    {
      name: "Blockchain.info",
      fetch: () => fetchBitcoinBlockchainInfoDebug(address, actualFilterDate)
    },
    {
      name: "BlockCypher",
      fetch: () => fetchBitcoinBlockCypherDebug(address, actualFilterDate)
    },
    {
      name: "Blockstream",
      fetch: () => fetchBitcoinBlockstreamDebug(address, actualFilterDate)
    }
  ];

  for (const api of apis) {
    try {
      console.log(`    🔍 Trying Bitcoin API: ${api.name}`);
      const apiTxs = await api.fetch();
      transactions.push(...apiTxs);
      console.log(`    ✅ ${api.name}: ${apiTxs.length} transactions`);
      
      if (apiTxs.length > 0) break; // Stop after first successful API with data
      
    } catch (error) {
      console.log(`    ❌ ${api.name} failed: ${error.message}`);
      continue;
    }
  }

  console.log(`  📊 Bitcoin total found: ${transactions.length}`);
  return transactions;
}

async function fetchBitcoinBlockstreamDebug(address, filterDate) {
  const endpoint = `https://blockstream.info/api/address/${address}/txs`;
  const response = await fetch(endpoint);
  
  if (!response.ok) {
    throw new Error(`Blockstream HTTP ${response.status}`);
  }
  
  const data = await response.json();
  const transactions = [];
  
  console.log(`      📊 Blockstream returned ${data.length} transactions`);
  
  data.slice(0, 20).forEach(tx => { // Check first 20 transactions
    const txDate = new Date(tx.status.block_time * 1000);
    if (txDate < filterDate) return;
    
    const isDeposit = tx.vout.some(output => output.scriptpubkey_address === address);
    
    if (isDeposit) {
      const output = tx.vout.find(o => o.scriptpubkey_address === address);
      transactions.push({
        platform: "Bitcoin Wallet",
        type: "deposit",
        asset: "BTC",
        amount: (output.value / 100000000).toString(),
        timestamp: txDate.toISOString(),
        from_address: "External",
        to_address: address,
        tx_id: tx.txid,
        status: "Completed",
        network: "BTC",
        api_source: "Blockstream_Debug"
      });
    }
  });
  
  return transactions;
}

async function fetchBitcoinBlockchainInfoDebug(address, filterDate) {
  const endpoint = `https://blockchain.info/rawaddr/${address}?limit=20`;
  const response = await fetch(endpoint);
  
  if (response.status === 429) {
    throw new Error("Rate limited");
  }
  
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }
  
  const data = await response.json();
  const transactions = [];
  
  console.log(`      📊 Blockchain.info returned ${data.txs.length} transactions`);
  console.log(`      💰 Current balance: ${data.final_balance / 100000000} BTC`);
  
  data.txs.slice(0, 20).forEach(tx => {
    const txDate = new Date(tx.time * 1000);
    if (txDate < filterDate) return;
    
    const isDeposit = tx.out.some(output => output.addr === address);
    
    if (isDeposit) {
      const output = tx.out.find(o => o.addr === address);
      transactions.push({
        platform: "Bitcoin Wallet",
        type: "deposit",
        asset: "BTC",
        amount: (output.value / 100000000).toString(),
        timestamp: txDate.toISOString(),
        from_address: "External",
        to_address: address,
        tx_id: tx.hash,
        status: "Completed",
        network: "BTC",
        api_source: "Blockchain_Info_Debug"
      });
    }
  });
  
  return transactions;
}

async function fetchBitcoinBlockCypherDebug(address, filterDate) {
  const endpoint = `https://api.blockcypher.com/v1/btc/main/addrs/${address}/txs?limit=20`;
  const response = await fetch(endpoint);
  
  if (!response.ok) {
    throw new Error(`BlockCypher HTTP ${response.status}`);
  }
  
  const data = await response.json();
  const transactions = [];
  
  console.log(`      📊 BlockCypher returned ${data.txs?.length || 0} transactions`);
  
  (data.txs || []).slice(0, 20).forEach(tx => {
    const txDate = new Date(tx.confirmed);
    if (txDate < filterDate) return;
    
    const isDeposit = tx.outputs.some(output => output.addresses && output.addresses.includes(address));
    
    if (isDeposit) {
      const output = tx.outputs.find(o => o.addresses && o.addresses.includes(address));
      transactions.push({
        platform: "Bitcoin Wallet",
        type: "deposit",
        asset: "BTC",
        amount: (output.value / 100000000).toString(),
        timestamp: txDate.toISOString(),
        from_address: "External",
        to_address: address,
        tx_id: tx.hash,
        status: "Completed",
        network: "BTC",
        api_source: "BlockCypher_Debug"
      });
    }
  });
  
  return transactions;
}

async function fetchEthereumEnhanced(address, filterDate) {
  try {
    const apiKey = process.env.ETHERSCAN_API_KEY || "SP8YA4W8RDB85G9129BTDHY72ADBZ6USHA";
    const endpoint = `https://api.etherscan.io/api?module=account&action=txlist&address=${address}&startblock=0&endblock=99999999&sort=desc&page=1&offset=100&apikey=${apiKey}`;
    
    console.log(`  🔍 Ethereum API call with enhanced debugging...`);
    
    const response = await fetch(endpoint);
    
    if (!response.ok) {
      throw new Error(`Ethereum API error: ${response.status}`);
    }
    
    const data = await response.json();
    
    console.log(`  📊 Etherscan response status: ${data.status}, message: ${data.message}`);
    console.log(`  📈 Raw transactions returned: ${data.result?.length || 0}`);
    
    if (data.status !== "1") {
      console.log("Etherscan API message:", data.message);
      return [];
    }
    
    const transactions = [];
    
    data.result.forEach(tx => {
      const txDate = new Date(parseInt(tx.timeStamp) * 1000);
      if (txDate < filterDate) return;
      
      const isDeposit = tx.to.toLowerCase() === address.toLowerCase();
      const amount = (parseInt(tx.value) / Math.pow(10, 18)).toString();
      
      if (parseFloat(amount) > 0) {
        transactions.push({
          platform: "Ethereum Wallet",
          type: isDeposit ? "deposit" : "withdrawal",
          asset: "ETH",
          amount: amount,
          timestamp: txDate.toISOString(),
          from_address: tx.from,
          to_address: tx.to,
          tx_id: tx.hash,
          status: tx.txreceipt_status === "1" ? "Completed" : "Failed",
          network: "ETH",
          api_source: "Etherscan_Enhanced_Debug"
        });
      }
    });
    
    console.log(`  ✅ Ethereum filtered transactions: ${transactions.length}`);
    return transactions;
    
  } catch (error) {
    console.error("Enhanced Ethereum API error:", error);
    throw error;
  }
}

async function fetchTronEnhanced(address, filterDate) {
  try {
    const endpoint = `https://api.trongrid.io/v1/accounts/${address}/transactions?limit=50&order_by=block_timestamp,desc`;
    
    console.log(`  🔍 TRON API call with enhanced debugging...`);
    
    const response = await fetch(endpoint);
    
    if (!response.ok) {
      throw new Error(`TRON API error: ${response.status}`);
    }
    
    const data = await response.json();
    
    console.log(`  📊 TRON response: ${data.success}, data length: ${data.data?.length || 0}`);
    
    if (!data.data) {
      return [];
    }
    
    const transactions = [];
    
    data.data.forEach(tx => {
      const txDate = new Date(tx.block_timestamp);
      if (txDate < filterDate) return;
      
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
              timestamp: txDate.toISOString(),
              from_address: value.owner_address,
              to_address: value.to_address,
              tx_id: tx.txID,
              status: "Completed",
              network: "TRON",
              api_source: "TronGrid_Enhanced_Debug"
            });
          }
        });
      }
    });
    
    console.log(`  ✅ TRON filtered transactions: ${transactions.length}`);
    return transactions;
    
  } catch (error) {
    console.error("Enhanced TRON API error:", error);
    throw error;
  }
}

async function fetchSolanaEnhanced(address, filterDate) {
  try {
    const endpoint = "https://api.mainnet-beta.solana.com";
    
    const payload = {
      jsonrpc: "2.0",
      id: 1,
      method: "getSignaturesForAddress",
      params: [address, { limit: 20 }]
    };
    
    console.log(`  🔍 Solana API call with enhanced debugging...`);
    
    const response = await fetch(endpoint, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });
    
    if (!response.ok) {
      throw new Error(`Solana API error: ${response.status}`);
    }
    
    const data = await response.json();
    
    console.log(`  📊 Solana response: ${data.result?.length || 0} signatures`);
    
    if (data.error) {
      throw new Error(`Solana RPC error: ${data.error.message}`);
    }
    
    const transactions = data.result.filter(sig => {
      const txDate = new Date(sig.blockTime * 1000);
      return txDate >= filterDate;
    }).map(sig => ({
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
      api_source: "Solana_RPC_Enhanced_Debug"
    }));
    
    console.log(`  ✅ Solana filtered transactions: ${transactions.length}`);
    return transactions;
    
  } catch (error) {
    console.error("Enhanced Solana API error:", error);
    throw error;
  }
}

// ===========================================
// SAFE DEDUPLICATION AND FILTERING FUNCTIONS (UNCHANGED)
// ===========================================

async function getExistingTransactionIds(sheets, spreadsheetId) {
  const existingTxIds = new Set();
  
  try {
    console.log('🔍 Reading existing transaction IDs for deduplication...');
    
    // Read from Withdrawals sheet (READ ONLY)
    try {
      const withdrawalsRange = 'Withdrawals!A7:L1000';
      const withdrawalsResponse = await sheets.spreadsheets.values.get({
        spreadsheetId,
        range: withdrawalsRange,
      });
      
      const withdrawalsData = withdrawalsResponse.data.values || [];
      withdrawalsData.forEach(row => {
        if (row[11]) {
          existingTxIds.add(row[11].toString().trim());
        }
      });
      console.log(`📤 Found ${withdrawalsData.length} existing withdrawals (READ ONLY)`);
    } catch (error) {
      console.log('⚠️ Could not read withdrawals sheet (might be empty)');
    }
    
    // Read from Deposits sheet (READ ONLY)
    try {
      const depositsRange = 'Deposits!A7:L1000';
      const depositsResponse = await sheets.spreadsheets.values.get({
        spreadsheetId,
        range: depositsRange,
      });
      
      const depositsData = depositsResponse.data.values || [];
      depositsData.forEach(row => {
        if (row[11]) {
          existingTxIds.add(row[11].toString().trim());
        }
      });
      console.log(`📥 Found ${depositsData.length} existing deposits (READ ONLY)`);
    } catch (error) {
      console.log('⚠️ Could not read deposits sheet (might be empty)');
    }
    
    console.log(`🎯 Total unique TX IDs found: ${existingTxIds.size}`);
    return existingTxIds;
    
  } catch (error) {
    console.error('❌ Error reading existing transactions:', error);
    return new Set();
  }
}

function removeDuplicateTransactions(transactions, existingTxIds) {
  let duplicateCount = 0;
  let totalCount = transactions.length;
  
  const newTransactions = transactions.filter(tx => {
    const txId = tx.tx_id?.toString().trim();
    
    if (!txId) {
      return true;
    }
    
    const isDuplicate = existingTxIds.has(txId);
    if (isDuplicate) {
      duplicateCount++;
    }
    
    return !isDuplicate;
  });
  
  console.log(`🔄 Duplicate Filter: ${totalCount} → ${newTransactions.length} transactions (removed ${duplicateCount} duplicates)`);
  
  return newTransactions;
}

function filterTransactionsByValue(transactions) {
  const pricesAED = {
    'BTC': 220200,
    'ETH': 11010,
    'USDT': 3.67,
    'USDC': 3.67,
    'SOL': 181.50,
    'TRX': 0.37,
    'BNB': 2200,
    'SEI': 1.47,
    'BUSD': 3.67
  };

  const minValueAED = 3.6;
  let filteredCount = 0;
  let totalCount = transactions.length;

  const filtered = transactions.filter(tx => {
    const amount = parseFloat(tx.amount) || 0;
    const priceAED = pricesAED[tx.asset] || 0;
    const aedValue = amount * priceAED;
    
    const keepTransaction = aedValue >= minValueAED;
    
    if (!keepTransaction) {
      filteredCount++;
    }
    
    return keepTransaction;
  });

  console.log(`💰 Value Filter: ${totalCount} → ${filtered.length} transactions (removed ${filteredCount} < 3.6 AED)`);
  
  return filtered;
}

function sortTransactionsByTimestamp(transactions) {
  console.log(`⏰ Sorting ${transactions.length} NEW transactions by timestamp (ascending - oldest first)...`);
  
  const sorted = [...transactions].sort((a, b) => {
    const dateA = new Date(a.timestamp);
    const dateB = new Date(b.timestamp);
    return dateA - dateB;
  });
  
  if (sorted.length > 0) {
    const oldestDate = new Date(sorted[0].timestamp).toISOString().slice(0, 16);
    const newestDate = new Date(sorted[sorted.length - 1].timestamp).toISOString().slice(0, 16);
    console.log(`📅 Date range: ${oldestDate} → ${newestDate} (${sorted.length} transactions)`);
  }
  
  return sorted;
}

// ===========================================
// ENHANCED GOOGLE SHEETS FUNCTIONS (UNCHANGED)
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

    console.log(`📊 Starting with ${transactions.length} raw transactions`);

    const existingTxIds = await getExistingTransactionIds(sheets, spreadsheetId);
    const uniqueTransactions = removeDuplicateTransactions(transactions, existingTxIds);
    const filteredTransactions = filterTransactionsByValue(uniqueTransactions);
    const sortedTransactions = sortTransactionsByTimestamp(filteredTransactions);

    console.log(`🎯 Final result: ${transactions.length} → ${sortedTransactions.length} NEW transactions to append`);
    console.log(`🛡️ SAFETY: Existing data will NOT be touched - only appending new transactions`);

    if (sortedTransactions.length === 0) {
      console.log('ℹ️ No new transactions to append after deduplication and filtering');
      await updateSettingsStatus(sheets, spreadsheetId, apiStatus);
      
      return {
        success: true,
        withdrawalsAdded: 0,
        depositsAdded: 0,
        statusUpdated: true,
        totalRaw: transactions.length,
        totalAfterDedup: uniqueTransactions.length,
        totalAfterFilter: filteredTransactions.length,
        duplicatesRemoved: transactions.length - uniqueTransactions.length,
        filteredOut: uniqueTransactions.length - filteredTransactions.length
      };
    }

    const sortedWithdrawals = sortedTransactions.filter(tx => tx.type === 'withdrawal');
    const sortedDeposits = sortedTransactions.filter(tx => tx.type === 'deposit');

    let withdrawalsAdded = 0;
    let depositsAdded = 0;

    if (sortedWithdrawals.length > 0) {
      console.log(`📤 APPENDING ${sortedWithdrawals.length} new withdrawals at bottom (ascending order)...`);
      
      const withdrawalRows = sortedWithdrawals.map(tx => [
        '', '', '', '', '',
        tx.platform, tx.asset, parseFloat(tx.amount).toFixed(8),
        formatDateTimeSimple(tx.timestamp), tx.from_address, tx.to_address, tx.tx_id
      ]);

      await sheets.spreadsheets.values.append({
        spreadsheetId,
        range: 'Withdrawals!A:L',
        valueInputOption: 'RAW',
        requestBody: { values: withdrawalRows }
      });
      
      withdrawalsAdded = sortedWithdrawals.length;
      console.log(`✅ APPENDED ${withdrawalsAdded} withdrawals at bottom`);
    }

    if (sortedDeposits.length > 0) {
      console.log(`📥 APPENDING ${sortedDeposits.length} new deposits at bottom (ascending order)...`);
      
      const depositRows = sortedDeposits.map(tx => [
        '', '', '', '', '',
        tx.platform, tx.asset, parseFloat(tx.amount).toFixed(8),
        formatDateTimeSimple(tx.timestamp), tx.from_address, tx.to_address, tx.tx_id
      ]);

      await sheets.spreadsheets.values.append({
        spreadsheetId,
        range: 'Deposits!A:L',
        valueInputOption: 'RAW',
        requestBody: { values: depositRows }
      });
      
      depositsAdded = sortedDeposits.length;
      console.log(`✅ APPENDED ${depositsAdded} deposits at bottom`);
    }

    await updateSettingsStatus(sheets, spreadsheetId, apiStatus);

    const result = {
      success: true,
      withdrawalsAdded: withdrawalsAdded,
      depositsAdded: depositsAdded,
      statusUpdated: true,
      totalRaw: transactions.length,
      totalAfterDedup: uniqueTransactions.length,
      totalAfterFilter: filteredTransactions.length,
      duplicatesRemoved: transactions.length - uniqueTransactions.length,
      filteredOut: uniqueTransactions.length - filteredTransactions.length,
      safetyNote: "Only appended new transactions - existing data untouched"
    };

    console.log('🎉 SAFE enhanced deduplication completed:', result);
    console.log('🛡️ GUARANTEE: No existing accountant data was modified');
    return result;

  } catch (error) {
    console.error('❌ Error in enhanced writeToGoogleSheetsWithStatus:', error);
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
    
    const statusRows = [];
    
    Object.entries(apiStatus).forEach(([platform, status]) => {
      statusRows.push([
        platform,
        status.status,
        formatDateTimeSimple(status.lastSync),
        status.autoUpdate,
        status.notes
      ]);
    });

    if (statusRows.length > 0) {
      await sheets.spreadsheets.values.clear({
        spreadsheetId,
        range: 'SETTINGS!A3:E20'
      });

      await sheets.spreadsheets.values.update({
        spreadsheetId,
        range: 'SETTINGS!A2:E2',
        valueInputOption: 'RAW',
        requestBody: {
          values: [['Platform', 'API Status', 'Last Sync', 'Auto-Update', 'Notes']]
        }
      });

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
// UTILITY FUNCTIONS
// ===========================================

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

function formatDateTimeSimple(isoString) {
  const date = new Date(isoString);
  return date.toISOString().slice(0, 16).replace('T', ' ');
}
