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

// ===========================================
// COMPLETE BLOCKCHAIN API FUNCTIONS FOR VERCEL
// Add this to your api/crypto-to-sheets.js file
// ===========================================

/**
 * COMPLETE: Fetch blockchain data with all wallet implementations
 */
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
  console.log("🧪 Testing Bitcoin API...");
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
    console.log(`✅ Bitcoin: ${btcTxs.length} transactions`);
  } catch (error) {
    apiStatus['Bitcoin Wallet'] = {
      status: 'Error',
      lastSync: new Date().toISOString(),
      autoUpdate: 'Every Hour',
      notes: `❌ ${error.message}`,
      transactionCount: 0
    };
    console.error(`❌ Bitcoin error:`, error.message);
  }

  // Ethereum transactions
  console.log("🧪 Testing Ethereum API...");
  try {
    const ethTxs = await fetchEthereumTransactionsVercel(wallets.ETH);
    transactions.push(...ethTxs);
    apiStatus['Ethereum Wallet'] = {
      status: 'Active',
      lastSync: new Date().toISOString(),
      autoUpdate: 'Every Hour',
      notes: `✅ ${ethTxs.length} transactions found`,
      transactionCount: ethTxs.length
    };
    console.log(`✅ Ethereum: ${ethTxs.length} transactions`);
  } catch (error) {
    apiStatus['Ethereum Wallet'] = {
      status: 'Error',
      lastSync: new Date().toISOString(),
      autoUpdate: 'Every Hour',
      notes: `❌ ${error.message}`,
      transactionCount: 0
    };
    console.error(`❌ Ethereum error:`, error.message);
  }

  // TRON transactions
  console.log("🧪 Testing TRON API...");
  try {
    const tronTxs = await fetchTronTransactionsVercel(wallets.TRON);
    transactions.push(...tronTxs);
    apiStatus['TRON Wallet'] = {
      status: 'Active',
      lastSync: new Date().toISOString(),
      autoUpdate: 'Every Hour',
      notes: `✅ ${tronTxs.length} transactions found`,
      transactionCount: tronTxs.length
    };
    console.log(`✅ TRON: ${tronTxs.length} transactions`);
  } catch (error) {
    apiStatus['TRON Wallet'] = {
      status: 'Error',
      lastSync: new Date().toISOString(),
      autoUpdate: 'Every Hour',
      notes: `❌ ${error.message}`,
      transactionCount: 0
    };
    console.error(`❌ TRON error:`, error.message);
  }

  // Solana transactions
  console.log("🧪 Testing Solana API...");
  try {
    const solTxs = await fetchSolanaTransactionsVercel(wallets.SOL);
    transactions.push(...solTxs);
    apiStatus['Solana Wallet'] = {
      status: 'Active',
      lastSync: new Date().toISOString(),
      autoUpdate: 'Every Hour',
      notes: `✅ ${solTxs.length} transactions found`,
      transactionCount: solTxs.length
    };
    console.log(`✅ Solana: ${solTxs.length} transactions`);
  } catch (error) {
    apiStatus['Solana Wallet'] = {
      status: 'Error',
      lastSync: new Date().toISOString(),
      autoUpdate: 'Every Hour',
      notes: `❌ ${error.message}`,
      transactionCount: 0
    };
    console.error(`❌ Solana error:`, error.message);
  }

  return {
    transactions: transactions,
    apiStatus: apiStatus
  };
}

/**
 * Fetch Ethereum transactions using Etherscan API
 */
async function fetchEthereumTransactionsVercel(address) {
  try {
    // Use your Etherscan API key
    const apiKey = "SP8YA4W8RDB85G9129BTDHY72ADBZ6USHA";
    const endpoint = `https://api.etherscan.io/api?module=account&action=txlist&address=${address}&startblock=0&endblock=99999999&sort=desc&apikey=${apiKey}`;
    
    const response = await fetch(endpoint);
    
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${await response.text()}`);
    }
    
    const data = await response.json();
    
    if (data.status !== "1") {
      console.error("Etherscan API error:", data.message);
      return [];
    }
    
    const transactions = [];
    
    data.result.slice(0, 50).forEach(tx => {
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
    
    // Also fetch ERC-20 token transactions
    const tokenTransactions = await fetchEthereumTokenTransactionsVercel(address, apiKey);
    transactions.push(...tokenTransactions);
    
    return transactions;
    
  } catch (error) {
    console.error("Error fetching Ethereum transactions:", error);
    throw error;
  }
}

/**
 * Fetch Ethereum ERC-20 token transactions
 */
async function fetchEthereumTokenTransactionsVercel(address, apiKey) {
  try {
    const endpoint = `https://api.etherscan.io/api?module=account&action=tokentx&address=${address}&startblock=0&endblock=99999999&sort=desc&apikey=${apiKey}`;
    
    const response = await fetch(endpoint);
    
    if (!response.ok) {
      return [];
    }
    
    const data = await response.json();
    
    if (data.status !== "1") {
      return [];
    }
    
    const transactions = [];
    
    data.result.slice(0, 50).forEach(tx => {
      const isDeposit = tx.to.toLowerCase() === address.toLowerCase();
      const decimals = parseInt(tx.tokenDecimal);
      const amount = (parseInt(tx.value) / Math.pow(10, decimals)).toString();
      
      if (parseFloat(amount) > 0) {
        transactions.push({
          platform: "Ethereum Wallet",
          type: isDeposit ? "deposit" : "withdrawal",
          asset: tx.tokenSymbol,
          amount: amount,
          timestamp: new Date(parseInt(tx.timeStamp) * 1000).toISOString(),
          from_address: tx.from,
          to_address: tx.to,
          tx_id: tx.hash,
          status: "Completed",
          network: "ERC20",
          api_source: "Etherscan_Token"
        });
      }
    });
    
    return transactions;
    
  } catch (error) {
    console.error("Error fetching Ethereum token transactions:", error);
    return [];
  }
}

/**
 * Fetch TRON transactions using TronGrid API
 */
async function fetchTronTransactionsVercel(address) {
  try {
    const endpoint = `https://api.trongrid.io/v1/accounts/${address}/transactions`;
    
    const response = await fetch(endpoint);
    
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${await response.text()}`);
    }
    
    const data = await response.json();
    
    if (!data.data) {
      console.error("TronGrid API error");
      return [];
    }
    
    const transactions = [];
    
    data.data.slice(0, 50).forEach(tx => {
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
    
    // Also fetch TRC-20 token transactions
    const tokenTransactions = await fetchTronTokenTransactionsVercel(address);
    transactions.push(...tokenTransactions);
    
    return transactions;
    
  } catch (error) {
    console.error("Error fetching TRON transactions:", error);
    throw error;
  }
}

/**
 * Fetch TRON TRC-20 token transactions
 */
async function fetchTronTokenTransactionsVercel(address) {
  try {
    const endpoint = `https://api.trongrid.io/v1/accounts/${address}/transactions/trc20`;
    
    const response = await fetch(endpoint);
    
    if (!response.ok) {
      return [];
    }
    
    const data = await response.json();
    
    if (!data.data) {
      return [];
    }
    
    const transactions = [];
    
    data.data.slice(0, 50).forEach(tx => {
      const isDeposit = tx.to === address;
      const decimals = parseInt(tx.token_info.decimals);
      const amount = (parseInt(tx.value) / Math.pow(10, decimals)).toString();
      
      if (parseFloat(amount) > 0) {
        transactions.push({
          platform: "TRON Wallet",
          type: isDeposit ? "deposit" : "withdrawal",
          asset: tx.token_info.symbol,
          amount: amount,
          timestamp: new Date(tx.block_timestamp).toISOString(),
          from_address: tx.from,
          to_address: tx.to,
          tx_id: tx.transaction_id,
          status: "Completed",
          network: "TRC20",
          api_source: "TronGrid_Token"
        });
      }
    });
    
    return transactions;
    
  } catch (error) {
    console.error("Error fetching TRON token transactions:", error);
    return [];
  }
}

/**
 * Fetch Solana transactions using Solana RPC API
 */
async function fetchSolanaTransactionsVercel(address) {
  try {
    const endpoint = "https://api.mainnet-beta.solana.com";
    
    const payload = {
      jsonrpc: "2.0",
      id: 1,
      method: "getSignaturesForAddress",
      params: [address, { limit: 50 }]
    };
    
    const response = await fetch(endpoint, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });
    
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${await response.text()}`);
    }
    
    const data = await response.json();
    
    if (data.error) {
      console.error("Solana RPC error:", data.error);
      return [];
    }
    
    const transactions = [];
    
    // Process first few signatures to get transaction details
    for (const signature of data.result.slice(0, 10)) {
      try {
        const txDetails = await getSolanaTransactionDetailsVercel(signature.signature);
        if (txDetails) {
          transactions.push(...txDetails);
        }
      } catch (error) {
        console.log(`Error getting Solana tx details for ${signature.signature}:`, error.message);
        continue;
      }
    }
    
    return transactions;
    
  } catch (error) {
    console.error("Error fetching Solana transactions:", error);
    throw error;
  }
}

/**
 * Get detailed Solana transaction information
 */
async function getSolanaTransactionDetailsVercel(signature) {
  try {
    const endpoint = "https://api.mainnet-beta.solana.com";
    
    const payload = {
      jsonrpc: "2.0",
      id: 1,
      method: "getTransaction",
      params: [signature, { encoding: "json", maxSupportedTransactionVersion: 0 }]
    };
    
    const response = await fetch(endpoint, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });
    
    if (!response.ok) {
      return null;
    }
    
    const data = await response.json();
    
    if (data.error || !data.result) {
      return null;
    }
    
    const tx = data.result;
    const transactions = [];
    
    if (tx.meta && tx.meta.preBalances && tx.meta.postBalances) {
      const accountKeys = tx.transaction.message.accountKeys;
      
      for (let i = 0; i < accountKeys.length; i++) {
        const preBalance = tx.meta.preBalances[i];
        const postBalance = tx.meta.postBalances[i];
        const balanceChange = postBalance - preBalance;
        
        if (Math.abs(balanceChange) > 0) {
          const amount = Math.abs(balanceChange) / 1000000000;
          
          transactions.push({
            platform: "Solana Wallet",
            type: balanceChange > 0 ? "deposit" : "withdrawal",
            asset: "SOL",
            amount: amount.toString(),
            timestamp: new Date(tx.blockTime * 1000).toISOString(),
            from_address: balanceChange > 0 ? "External" : accountKeys[i],
            to_address: balanceChange > 0 ? accountKeys[i] : "External",
            tx_id: signature,
            status: tx.meta.err ? "Failed" : "Completed",
            network: "SOL",
            api_source: "Solana_RPC"
          });
        }
      }
    }
    
    return transactions;
    
  } catch (error) {
    console.error("Error getting Solana transaction details:", error);
    return null;
  }
}

/**
 * ENHANCED: Fetch Bitcoin transactions with rate limiting
 */
async function fetchBitcoinTransactionsVercel(address) {
  try {
    // Add delay to avoid rate limiting
    await new Promise(resolve => setTimeout(resolve, 1000)); // 1 second delay
    
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
