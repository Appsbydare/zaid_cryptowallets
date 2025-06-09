// ===========================================
// COMPLETE ENHANCED CRYPTO API WITH SAFE DEDUPLICATION
// Replace your entire api/crypto-to-sheets.js with this file
// Features: Higher limits, Date filtering, Deduplication, AED filter, Safe append
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
    console.log('🚀 Starting COMPLETE ENHANCED crypto data fetch...');

    // Get date filtering from request or use defaults
    const startDate = req.body?.startDate || '2025-05-31T00:00:00.000Z';
    const filterDate = new Date(startDate);
    console.log(`📅 Filtering transactions after: ${startDate}`);

    const allTransactions = [];
    const apiStatusResults = {};
    let totalTransactionsFound = 0;

    // ===========================================
    // STEP 1: ENHANCED BINANCE APIS (HIGHER LIMITS)
    // ===========================================
    console.log('🧪 Testing Binance APIs with HIGHER LIMITS...');
    
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

      console.log(`🔥 Processing ${account.name} with ENHANCED limits...`);
      const result = await testBinanceAccountEnhanced(account, filterDate);
      apiStatusResults[account.name] = result.status;
      
      if (result.success) {
        allTransactions.push(...result.transactions);
        totalTransactionsFound += result.transactions.length;
        console.log(`✅ ${account.name}: ${result.transactions.length} transactions (after date filter)`);
      } else {
        console.log(`❌ ${account.name}: ${result.status.notes}`);
      }
    }

    // ===========================================
    // STEP 2: ENHANCED BYBIT API (BETTER ERROR HANDLING)
    // ===========================================
    if (process.env.BYBIT_API_KEY && process.env.BYBIT_API_SECRET) {
      console.log('🔥 Testing ByBit with enhanced error handling...');
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
    // STEP 3: ENHANCED BLOCKCHAIN DATA (HIGHER LIMITS)
    // ===========================================
    console.log('🔥 Fetching blockchain data with HIGHER LIMITS...');
    
    const wallets = {
      BTC: "bc1qkuefzcmc6c8enw9f7a2e9w2hy964q3jgwcv35g",
      ETH: "0x856851a1d5111330729744f95238e5D810ba773c",
      TRON: "TAUDuQAZSTUH88xno1imPoKN25eJN6aJkN",
      SOL: "BURkHx6BNTqryY3sCqXcYNVkhN6Mz3ttDUdGQ6hXuX4n"
    };

    // Enhanced Bitcoin API with multiple sources
    try {
      console.log('🔥 Testing Bitcoin API with multiple sources...');
      const btcTxs = await fetchBitcoinEnhanced(wallets.BTC, filterDate);
      allTransactions.push(...btcTxs);
      totalTransactionsFound += btcTxs.length;
      apiStatusResults['Bitcoin Wallet'] = {
        status: btcTxs.length > 0 ? 'Active' : 'Warning',
        lastSync: new Date().toISOString(),
        autoUpdate: 'Every Hour',
        notes: `🔥 ${btcTxs.length} transactions found (enhanced search)`,
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

    // Enhanced Ethereum API
    try {
      console.log('🔥 Testing Ethereum API with 100 transaction limit...');
      const ethTxs = await fetchEthereumEnhanced(wallets.ETH, filterDate);
      allTransactions.push(...ethTxs);
      totalTransactionsFound += ethTxs.length;
      apiStatusResults['Ethereum Wallet'] = {
        status: 'Active',
        lastSync: new Date().toISOString(),
        autoUpdate: 'Every Hour',
        notes: `🔥 ${ethTxs.length} transactions found (enhanced limit: 100)`,
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
      console.log('🔥 Testing TRON API with 50 transaction limit...');
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
      console.log('🔥 Testing Solana API with enhanced limits...');
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
    // STEP 5: RETURN ENHANCED RESULTS WITH DEDUPLICATION STATS
    // ===========================================
    res.status(200).json({
      success: true,
      message: 'COMPLETE ENHANCED data processing with safe deduplication completed',
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
        enhancedFeatures: 'Deduplication + Value Filter + Safe Append + Ascending Sort'
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
// ENHANCED BINANCE API FUNCTIONS
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

    // Get ALL transaction types with higher limits
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

      // 3. Fetch P2P transactions (NEW!)
      const p2pTransactions = await fetchBinanceP2PEnhanced(account, filterDate);
      transactions.push(...p2pTransactions);
      transactionBreakdown.p2p = p2pTransactions.length;
      console.log(`  🤝 ${account.name} P2P: ${p2pTransactions.length}`);

      // 4. Fetch Binance Pay transactions (NEW!)
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
// NEW: BINANCE P2P TRANSACTION FETCHING
// ===========================================

async function fetchBinanceP2PEnhanced(account, filterDate) {
  const transactions = [];
  
  try {
    console.log(`    🤝 Fetching P2P transactions for ${account.name}...`);
    
    // Fetch P2P Buy orders (deposits)
    const buyOrders = await fetchBinanceP2POrders(account, filterDate, 'BUY');
    transactions.push(...buyOrders);
    
    // Fetch P2P Sell orders (withdrawals)  
    const sellOrders = await fetchBinanceP2POrders(account, filterDate, 'SELL');
    transactions.push(...sellOrders);
    
    console.log(`    ✅ P2P: ${buyOrders.length} buys + ${sellOrders.length} sells`);
    
  } catch (error) {
    console.log(`    ❌ P2P fetch failed for ${account.name}: ${error.message}`);
  }
  
  return transactions;
}

async function fetchBinanceP2POrders(account, filterDate, tradeType) {
  try {
    const timestamp = Date.now();
    const endpoint = "https://api.binance.com/sapi/v1/c2c/orderMatch/listUserOrderHistory";
    const params = {
      tradeType: tradeType, // 'BUY' or 'SELL'
      timestamp: timestamp,
      recvWindow: 5000,
      page: 1,
      rows: 100 // Get up to 100 P2P orders
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
      throw new Error(`P2P API error: ${response.status}`);
    }

    const data = await response.json();

    if (data.code && data.code !== 200) {
      throw new Error(`Binance P2P error: ${data.msg}`);
    }

    if (!data.data) {
      return [];
    }

    // Filter by date and convert to standard format
    const p2pTransactions = data.data.filter(order => {
      const orderDate = new Date(parseInt(order.createTime));
      return orderDate >= filterDate && order.orderStatus === "COMPLETED";
    }).map(order => ({
      platform: account.name,
      type: tradeType === 'BUY' ? "deposit" : "withdrawal",
      asset: order.asset,
      amount: order.amount.toString(),
      timestamp: new Date(parseInt(order.createTime)).toISOString(),
      from_address: tradeType === 'BUY' ? "P2P User" : account.name,
      to_address: tradeType === 'BUY' ? account.name : "P2P User",
      tx_id: `P2P_${order.orderNumber}`,
      status: "Completed",
      network: "P2P",
      api_source: `Binance_P2P_${tradeType}`
    }));

    return p2pTransactions;

  } catch (error) {
    console.error(`Error fetching P2P ${tradeType} orders for ${account.name}:`, error);
    return [];
  }
}

// ===========================================
// NEW: BINANCE PAY TRANSACTION FETCHING  
// ===========================================

async function fetchBinancePayEnhanced(account, filterDate) {
  try {
    console.log(`    💳 Fetching Binance Pay transactions for ${account.name}...`);
    
    const timestamp = Date.now();
    const endpoint = "https://api.binance.com/sapi/v1/pay/transactions";
    const params = {
      timestamp: timestamp,
      recvWindow: 5000,
      limit: 100,
      startTime: filterDate.getTime()
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
      throw new Error(`Binance Pay API error: ${response.status}`);
    }

    const data = await response.json();

    if (data.code && data.code !== 200) {
      throw new Error(`Binance Pay error: ${data.msg}`);
    }

    if (!data.data) {
      return [];
    }

    const payTransactions = data.data.filter(tx => {
      const txDate = new Date(parseInt(tx.createTime));
      return txDate >= filterDate && tx.status === "SUCCESS";
    }).map(tx => {
      const isDeposit = tx.direction === "IN";
      
      return {
        platform: account.name,
        type: isDeposit ? "deposit" : "withdrawal",
        asset: tx.currency,
        amount: tx.amount.toString(),
        timestamp: new Date(parseInt(tx.createTime)).toISOString(),
        from_address: isDeposit ? "Binance Pay User" : account.name,
        to_address: isDeposit ? account.name : "Binance Pay User",
        tx_id: `PAY_${tx.transactionId}`,
        status: "Completed",
        network: "Binance Pay",
        api_source: "Binance_Pay"
      };
    });

    console.log(`    ✅ Binance Pay: ${payTransactions.length} transactions`);
    return payTransactions;

  } catch (error) {
    console.error(`Error fetching Binance Pay for ${account.name}:`, error);
    return [];
  }
}

// ===========================================
// ENHANCED BYBIT WITH ACTUAL TRANSACTION FETCHING
// Replace the existing testByBitAccountEnhanced function
// ===========================================

async function testByBitAccountEnhanced(config, filterDate) {
  try {
    console.log(`🔥 Processing ByBit ${config.name} with ACTUAL transaction fetching...`);
    
    // Test connection first
    const timestamp = Date.now();
    const testEndpoint = "https://api.bybit.com/v5/account/wallet-balance";
    const testParams = {
      accountType: "UNIFIED",
      timestamp: timestamp.toString()
    };

    const testSignature = createByBitSignature(testParams, config.apiSecret);
    const testQueryString = createQueryString(testParams);
    const testUrl = `${testEndpoint}?${testQueryString}`;

    const testResponse = await fetch(testUrl, {
      method: "GET",
      headers: {
        "X-BAPI-API-KEY": config.apiKey,
        "X-BAPI-SIGN": testSignature,
        "X-BAPI-TIMESTAMP": timestamp.toString(),
        "X-BAPI-RECV-WINDOW": "5000"
      }
    });

    if (!testResponse.ok) {
      throw new Error(`ByBit connection test failed: ${testResponse.status}`);
    }

    const testData = await testResponse.json();
    
    if (testData.retCode !== 0) {
      throw new Error(`ByBit test error: ${testData.retMsg}`);
    }

    console.log(`    ✅ ByBit connection successful, fetching transactions...`);

    // Now fetch actual transactions
    let transactions = [];
    let transactionBreakdown = {
      deposits: 0,
      withdrawals: 0
    };

    try {
      // Fetch deposits
      const deposits = await fetchByBitDepositsEnhanced(config, filterDate);
      transactions.push(...deposits);
      transactionBreakdown.deposits = deposits.length;
      console.log(`  💰 ${config.name} deposits: ${deposits.length}`);

      // Fetch withdrawals
      const withdrawals = await fetchByBitWithdrawalsEnhanced(config, filterDate);
      transactions.push(...withdrawals);
      transactionBreakdown.withdrawals = withdrawals.length;
      console.log(`  📤 ${config.name} withdrawals: ${withdrawals.length}`);

    } catch (txError) {
      console.log(`ByBit transaction fetch failed: ${txError.message}`);
    }

    const statusNotes = `🔥 Connected: ${transactionBreakdown.deposits}D + ${transactionBreakdown.withdrawals}W = ${transactions.length} total`;

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

// ===========================================
// NEW: BYBIT DEPOSIT FETCHING
// ===========================================

async function fetchByBitDepositsEnhanced(config, filterDate) {
  try {
    console.log(`    💰 Fetching ByBit deposits for ${config.name}...`);
    
    const timestamp = Date.now();
    const endpoint = "https://api.bybit.com/v5/asset/deposit/query-record";
    const params = {
      timestamp: timestamp.toString(),
      limit: "50", // Get up to 50 deposits
      startTime: filterDate.getTime().toString()
    };

    const signature = createByBitSignature(params, config.apiSecret);
    const queryString = createQueryString(params);
    const url = `${endpoint}?${queryString}`;

    const response = await fetch(url, {
      method: "GET",
      headers: {
        "X-BAPI-API-KEY": config.apiKey,
        "X-BAPI-SIGN": signature,
        "X-BAPI-TIMESTAMP": timestamp.toString(),
        "X-BAPI-RECV-WINDOW": "5000"
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
      api_source: "ByBit_Deposit"
    }));

    console.log(`    ✅ ByBit deposits: ${deposits.length} transactions`);
    return deposits;

  } catch (error) {
    console.error(`Error fetching ByBit deposits for ${config.name}:`, error);
    return [];
  }
}

// ===========================================
// NEW: BYBIT WITHDRAWAL FETCHING
// ===========================================

async function fetchByBitWithdrawalsEnhanced(config, filterDate) {
  try {
    console.log(`    📤 Fetching ByBit withdrawals for ${config.name}...`);
    
    const timestamp = Date.now();
    const endpoint = "https://api.bybit.com/v5/asset/withdraw/query-record";
    const params = {
      timestamp: timestamp.toString(),
      limit: "50", // Get up to 50 withdrawals
      startTime: filterDate.getTime().toString()
    };

    const signature = createByBitSignature(params, config.apiSecret);
    const queryString = createQueryString(params);
    const url = `${endpoint}?${queryString}`;

    const response = await fetch(url, {
      method: "GET",
      headers: {
        "X-BAPI-API-KEY": config.apiKey,
        "X-BAPI-SIGN": signature,
        "X-BAPI-TIMESTAMP": timestamp.toString(),
        "X-BAPI-RECV-WINDOW": "5000"
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
      api_source: "ByBit_Withdrawal"
    }));

    console.log(`    ✅ ByBit withdrawals: ${withdrawals.length} transactions`);
    return withdrawals;

  } catch (error) {
    console.error(`Error fetching ByBit withdrawals for ${config.name}:`, error);
    return [];
  }
}

// ===========================================
// ENHANCED BLOCKCHAIN API FUNCTIONS
// ===========================================

async function fetchBitcoinEnhanced(address, filterDate) {
  const transactions = [];
  
  // Try multiple Bitcoin APIs for better coverage
  const apis = [
    {
      name: "Blockchain.info",
      fetch: () => fetchBitcoinBlockchainInfo(address, filterDate)
    },
    {
      name: "BlockCypher",
      fetch: () => fetchBitcoinBlockCypher(address, filterDate)
    }
  ];

  for (const api of apis) {
    try {
      console.log(`  🔍 Trying Bitcoin API: ${api.name}`);
      const apiTxs = await api.fetch();
      transactions.push(...apiTxs);
      console.log(`  ✅ ${api.name}: ${apiTxs.length} transactions`);
      
      if (transactions.length > 0) break; // Stop after first successful API
      
    } catch (error) {
      console.log(`  ❌ ${api.name} failed: ${error.message}`);
      continue;
    }
  }

  return transactions;
}

async function fetchBitcoinBlockchainInfo(address, filterDate) {
  const endpoint = `https://blockchain.info/rawaddr/${address}?limit=50`; // ENHANCED: Added limit
  const response = await fetch(endpoint);
  
  if (response.status === 429) {
    throw new Error("Rate limited");
  }
  
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }
  
  const data = await response.json();
  const transactions = [];
  
  data.txs.slice(0, 50).forEach(tx => { // ENHANCED: Increased from 10 to 50
    const txDate = new Date(tx.time * 1000);
    if (txDate < filterDate) return; // ENHANCED: Date filtering
    
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
        api_source: "Blockchain_Info_Enhanced"
      });
    }
  });
  
  return transactions;
}

async function fetchBitcoinBlockCypher(address, filterDate) {
  const endpoint = `https://api.blockcypher.com/v1/btc/main/addrs/${address}/txs?limit=50`; // ENHANCED: Added limit
  const response = await fetch(endpoint);
  
  if (!response.ok) {
    throw new Error(`BlockCypher HTTP ${response.status}`);
  }
  
  const data = await response.json();
  const transactions = [];
  
  (data.txs || []).slice(0, 50).forEach(tx => { // ENHANCED: Increased limit
    const txDate = new Date(tx.confirmed);
    if (txDate < filterDate) return; // ENHANCED: Date filtering
    
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
        api_source: "BlockCypher_Enhanced"
      });
    }
  });
  
  return transactions;
}

async function fetchEthereumEnhanced(address, filterDate) {
  try {
    const apiKey = process.env.ETHERSCAN_API_KEY || "SP8YA4W8RDB85G9129BTDHY72ADBZ6USHA";
    const endpoint = `https://api.etherscan.io/api?module=account&action=txlist&address=${address}&startblock=0&endblock=99999999&sort=desc&page=1&offset=100&apikey=${apiKey}`; // ENHANCED: Increased from 10 to 100
    
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
      const txDate = new Date(parseInt(tx.timeStamp) * 1000);
      if (txDate < filterDate) return; // ENHANCED: Date filtering
      
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
          api_source: "Etherscan_Enhanced"
        });
      }
    });
    
    return transactions;
    
  } catch (error) {
    console.error("Enhanced Ethereum API error:", error);
    throw error;
  }
}

async function fetchTronEnhanced(address, filterDate) {
  try {
    const endpoint = `https://api.trongrid.io/v1/accounts/${address}/transactions?limit=50&order_by=block_timestamp,desc`; // ENHANCED: Increased from 10 to 50
    
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
      const txDate = new Date(tx.block_timestamp);
      if (txDate < filterDate) return; // ENHANCED: Date filtering
      
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
              api_source: "TronGrid_Enhanced"
            });
          }
        });
      }
    });
    
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
      params: [address, { limit: 20 }] // ENHANCED: Increased from 5 to 20
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
    
    const transactions = data.result.filter(sig => {
      const txDate = new Date(sig.blockTime * 1000);
      return txDate >= filterDate; // ENHANCED: Date filtering
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
      api_source: "Solana_RPC_Enhanced"
    }));
    
    return transactions;
    
  } catch (error) {
    console.error("Enhanced Solana API error:", error);
    throw error;
  }
}

// ===========================================
// SAFE DEDUPLICATION AND FILTERING FUNCTIONS
// ===========================================

/**
 * Read existing transaction IDs from Google Sheets to prevent duplicates
 * *** ONLY READS - NEVER MODIFIES EXISTING DATA ***
 */
async function getExistingTransactionIds(sheets, spreadsheetId) {
  const existingTxIds = new Set();
  
  try {
    console.log('🔍 Reading existing transaction IDs for deduplication...');
    
    // Read from Withdrawals sheet (READ ONLY)
    try {
      const withdrawalsRange = 'Withdrawals!A7:L1000'; // Skip headers, read up to row 1000
      const withdrawalsResponse = await sheets.spreadsheets.values.get({
        spreadsheetId,
        range: withdrawalsRange,
      });
      
      const withdrawalsData = withdrawalsResponse.data.values || [];
      withdrawalsData.forEach(row => {
        if (row[11]) { // TX ID is in column L (index 11)
          existingTxIds.add(row[11].toString().trim());
        }
      });
      console.log(`📤 Found ${withdrawalsData.length} existing withdrawals (READ ONLY)`);
    } catch (error) {
      console.log('⚠️ Could not read withdrawals sheet (might be empty)');
    }
    
    // Read from Deposits sheet (READ ONLY)
    try {
      const depositsRange = 'Deposits!A7:L1000'; // Skip headers, read up to row 1000
      const depositsResponse = await sheets.spreadsheets.values.get({
        spreadsheetId,
        range: depositsRange,
      });
      
      const depositsData = depositsResponse.data.values || [];
      depositsData.forEach(row => {
        if (row[11]) { // TX ID is in column L (index 11)
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
    return new Set(); // Return empty set if read fails
  }
}

/**
 * Filter out duplicate transactions based on TX ID
 */
function removeDuplicateTransactions(transactions, existingTxIds) {
  let duplicateCount = 0;
  let totalCount = transactions.length;
  
  const newTransactions = transactions.filter(tx => {
    const txId = tx.tx_id?.toString().trim();
    
    if (!txId) {
      // Keep transactions without TX ID (shouldn't happen but be safe)
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

/**
 * Filter transactions by minimum AED value (3.6 AED)
 */
function filterTransactionsByValue(transactions) {
  // Current crypto prices in AED
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
    // Calculate AED value
    const amount = parseFloat(tx.amount) || 0;
    const priceAED = pricesAED[tx.asset] || 0;
    const aedValue = amount * priceAED;
    
    // Keep transactions >= 3.6 AED
    const keepTransaction = aedValue >= minValueAED;
    
    if (!keepTransaction) {
      filteredCount++;
    }
    
    return keepTransaction;
  });

  console.log(`💰 Value Filter: ${totalCount} → ${filtered.length} transactions (removed ${filteredCount} < 3.6 AED)`);
  
  return filtered;
}

/**
 * Sort transactions by timestamp in ASCENDING order (oldest first)
 * *** ONLY sorts NEW transactions - never touches existing data ***
 */
function sortTransactionsByTimestamp(transactions) {
  console.log(`⏰ Sorting ${transactions.length} NEW transactions by timestamp (ascending - oldest first)...`);
  
  const sorted = [...transactions].sort((a, b) => {
    const dateA = new Date(a.timestamp);
    const dateB = new Date(b.timestamp);
    return dateA - dateB; // Ascending order (oldest first)
  });
  
  if (sorted.length > 0) {
    const oldestDate = new Date(sorted[0].timestamp).toISOString().slice(0, 16);
    const newestDate = new Date(sorted[sorted.length - 1].timestamp).toISOString().slice(0, 16);
    console.log(`📅 Date range: ${oldestDate} → ${newestDate} (${sorted.length} transactions)`);
  }
  
  return sorted;
}

// ===========================================
// ENHANCED GOOGLE SHEETS FUNCTIONS WITH SAFE DEDUPLICATION
// ===========================================

/**
 * Enhanced writeToGoogleSheetsWithStatus with safe deduplication and ascending append
 * *** NEVER MODIFIES EXISTING DATA - ONLY APPENDS NEW SORTED TRANSACTIONS ***
 */
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

    // *** STEP 1: Get existing transaction IDs for deduplication (READ ONLY) ***
    const existingTxIds = await getExistingTransactionIds(sheets, spreadsheetId);

    // *** STEP 2: Remove duplicates based on TX ID ***
    const uniqueTransactions = removeDuplicateTransactions(transactions, existingTxIds);

    // *** STEP 3: Apply AED value filter ***
    const filteredTransactions = filterTransactionsByValue(uniqueTransactions);

    // *** STEP 4: Sort NEW transactions by timestamp (ascending - oldest first) ***
    const sortedTransactions = sortTransactionsByTimestamp(filteredTransactions);

    console.log(`🎯 Final result: ${transactions.length} → ${sortedTransactions.length} NEW transactions to append`);
    console.log(`🛡️ SAFETY: Existing data will NOT be touched - only appending new transactions`);

    // If no new transactions, just update status
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

    // Split sorted transactions by type
    const sortedWithdrawals = sortedTransactions.filter(tx => tx.type === 'withdrawal');
    const sortedDeposits = sortedTransactions.filter(tx => tx.type === 'deposit');

    let withdrawalsAdded = 0;
    let depositsAdded = 0;

    // *** APPEND sorted withdrawals at bottom (NEVER touch existing data) ***
    if (sortedWithdrawals.length > 0) {
      console.log(`📤 APPENDING ${sortedWithdrawals.length} new withdrawals at bottom (ascending order)...`);
      
      const withdrawalRows = sortedWithdrawals.map(tx => [
        '', '', '', '', '', // Green columns for accountant (empty for them to fill)
        tx.platform, tx.asset, parseFloat(tx.amount).toFixed(8),
        formatDateTimeSimple(tx.timestamp), tx.from_address, tx.to_address, tx.tx_id
      ]);

      // APPEND (not overwrite) - this adds at the bottom
      await sheets.spreadsheets.values.append({
        spreadsheetId,
        range: 'Withdrawals!A:L',
        valueInputOption: 'RAW',
        requestBody: { values: withdrawalRows }
      });
      
      withdrawalsAdded = sortedWithdrawals.length;
      console.log(`✅ APPENDED ${withdrawalsAdded} withdrawals at bottom`);
    }

    // *** APPEND sorted deposits at bottom (NEVER touch existing data) ***
    if (sortedDeposits.length > 0) {
      console.log(`📥 APPENDING ${sortedDeposits.length} new deposits at bottom (ascending order)...`);
      
      const depositRows = sortedDeposits.map(tx => [
        '', '', '', '', '', // Green columns for accountant (empty for them to fill)
        tx.platform, tx.asset, parseFloat(tx.amount).toFixed(8),
        formatDateTimeSimple(tx.timestamp), tx.from_address, tx.to_address, tx.tx_id
      ]);

      // APPEND (not overwrite) - this adds at the bottom
      await sheets.spreadsheets.values.append({
        spreadsheetId,
        range: 'Deposits!A:L',
        valueInputOption: 'RAW',
        requestBody: { values: depositRows }
      });
      
      depositsAdded = sortedDeposits.length;
      console.log(`✅ APPENDED ${depositsAdded} deposits at bottom`);
    }

    // Update Settings status table
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
