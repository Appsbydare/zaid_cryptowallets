// ===========================================
// VERCEL API WITH SIMPLE GOOGLE SHEETS WRITING
// Replace your api/crypto-to-sheets.js with this
// ===========================================

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
    console.log('🚀 Starting crypto data fetch with sheets writing...');

    const allTransactions = [];
    const apiStatusResults = {};

    // ===========================================
    // STEP 1: FETCH BLOCKCHAIN DATA
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

    // ===========================================
    // STEP 2: WRITE TO GOOGLE SHEETS (SIMPLE)
    // ===========================================
    console.log(`📊 Writing ${allTransactions.length} transactions to Google Sheets...`);
    
    let sheetsResult = { success: false, withdrawalsAdded: 0, depositsAdded: 0 };
    
    if (allTransactions.length > 0) {
      try {
        sheetsResult = await writeToGoogleSheetsSimple(allTransactions);
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
    }

    // ===========================================
    // STEP 3: RETURN RESULTS
    // ===========================================
    res.status(200).json({
      success: true,
      message: 'Data successfully processed',
      transactions: allTransactions.length,
      sheetsResult: sheetsResult,
      apiStatus: apiStatusResults,
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
// SIMPLE GOOGLE SHEETS INTEGRATION
// ===========================================

async function writeToGoogleSheetsSimple(transactions) {
  try {
    console.log('🔑 Setting up Google Sheets authentication...');
    
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

    console.log('📝 Separating withdrawals and deposits...');
    
    // Separate withdrawals and deposits
    const withdrawals = transactions.filter(tx => tx.type === 'withdrawal');
    const deposits = transactions.filter(tx => tx.type === 'deposit');

    console.log(`📤 ${withdrawals.length} withdrawals, 📥 ${deposits.length} deposits`);

    let withdrawalsAdded = 0;
    let depositsAdded = 0;

    // Write withdrawals to Withdrawals sheet
    if (withdrawals.length > 0) {
      console.log('📤 Writing withdrawals...');
      
      const withdrawalRows = withdrawals.map(tx => [
        '', // Client (accountant fills)
        '', // Amount (AED) (accountant fills)  
        '', // Amount (USDT) (accountant fills)
        '', // Sell Rate (accountant fills)
        '', // Remark (accountant fills)
        tx.platform, // Platform (auto)
        tx.asset, // Asset (auto)
        parseFloat(tx.amount).toFixed(8), // Amount (auto)
        formatDateTimeSimple(tx.timestamp), // Timestamp (auto)
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
      
      withdrawalsAdded = withdrawals.length;
      console.log(`✅ Added ${withdrawalsAdded} withdrawals`);
    }

    // Write deposits to Deposits sheet
    if (deposits.length > 0) {
      console.log('📥 Writing deposits...');
      
      const depositRows = deposits.map(tx => [
        '', // Client (accountant fills)
        '', // AED (accountant fills)
        '', // USDT (accountant fills)  
        '', // Rate (accountant fills)
        '', // Remarks (accountant fills)
        tx.platform, // Platform (auto)
        tx.asset, // Asset (auto)
        parseFloat(tx.amount).toFixed(8), // Amount (auto)
        formatDateTimeSimple(tx.timestamp), // Timestamp (auto)
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
      
      depositsAdded = deposits.length;
      console.log(`✅ Added ${depositsAdded} deposits`);
    }

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

// ===========================================
// BLOCKCHAIN API FUNCTIONS (SAME AS BEFORE)
// ===========================================

/**
 * Simple Bitcoin API fetch
 */
async function fetchBitcoinSimple(address) {
  try {
    console.log(`Fetching Bitcoin for ${address}...`);
    
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
    
    // Process only first 10 transactions to avoid timeout
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
    
    console.log(`Bitcoin found ${transactions.length} transactions`);
    return transactions;
    
  } catch (error) {
    console.error("Bitcoin API error:", error);
    throw error;
  }
}

/**
 * Simple Ethereum API fetch
 */
async function fetchEthereumSimple(address) {
  try {
    console.log(`Fetching Ethereum for ${address}...`);
    
    const apiKey = "SP8YA4W8RDB85G9129BTDHY72ADBZ6USHA";
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
    
    console.log(`Ethereum found ${transactions.length} transactions`);
    return transactions;
    
  } catch (error) {
    console.error("Ethereum API error:", error);
    throw error;
  }
}

/**
 * Simple TRON API fetch
 */
async function fetchTronSimple(address) {
  try {
    console.log(`Fetching TRON for ${address}...`);
    
    const endpoint = `https://api.trongrid.io/v1/accounts/${address}/transactions?limit=10`;
    
    const response = await fetch(endpoint);
    
    if (!response.ok) {
      throw new Error(`TRON API error: ${response.status}`);
    }
    
    const data = await response.json();
    
    if (!data.data) {
      console.log("TronGrid API: no data");
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
    
    console.log(`TRON found ${transactions.length} transactions`);
    return transactions;
    
  } catch (error) {
    console.error("TRON API error:", error);
    throw error;
  }
}

// ===========================================
// UTILITY FUNCTIONS
// ===========================================

function formatDateTimeSimple(isoString) {
  const date = new Date(isoString);
  return date.toISOString().slice(0, 16).replace('T', ' ');
}
