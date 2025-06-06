// ===========================================
// SIMPLE WORKING VERSION - api/crypto-to-sheets.js
// ===========================================

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
    console.log('🚀 Starting simple crypto data fetch...');

    const allTransactions = [];
    const apiStatusResults = {};

    // ===========================================
    // STEP 1: TEST BASIC FUNCTIONALITY FIRST
    // ===========================================
    
    console.log('✅ Basic function structure working');

    // ===========================================
    // STEP 2: FETCH BLOCKCHAIN DATA (SIMPLE)
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
    // STEP 3: RETURN RESULTS (WITHOUT SHEETS WRITE FOR NOW)
    // ===========================================

    res.status(200).json({
      success: true,
      message: 'Data successfully fetched (Google Sheets write disabled for testing)',
      transactions: allTransactions.length,
      details: allTransactions,
      apiStatus: apiStatusResults,
      timestamp: new Date().toISOString()
    });

  } catch (error) {
    console.error('❌ Vercel Error:', error);
    
    res.status(500).json({
      success: false,
      error: error.message,
      stack: error.stack,
      timestamp: new Date().toISOString()
    });
  }
}

// ===========================================
// SIMPLE BLOCKCHAIN API FUNCTIONS
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
