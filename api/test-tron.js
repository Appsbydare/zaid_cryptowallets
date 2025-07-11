// Test endpoint for TRON wallet debugging
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
    console.log('🧪 Testing TRON wallet API...');
    
    const tronAddress = "TAUDuQAZSTUH88xno1imPoKN25eJN6aJkN";
    const endpoint = `https://api.trongrid.io/v1/accounts/${tronAddress}/transactions?limit=50&order_by=block_timestamp,desc`;
    
    console.log(`🔗 Testing TRON API endpoint: ${endpoint}`);
    
    const response = await fetch(endpoint);
    const statusCode = response.status;
    
    console.log(`📡 TRON API Response Status: ${statusCode}`);
    
    if (!response.ok) {
      throw new Error(`TRON API error: ${response.status} - ${response.statusText}`);
    }
    
    const data = await response.json();
    
    console.log('📊 TRON API Response Data:', JSON.stringify(data, null, 2));
    
    // Test the same logic as in the main function
    const filterDate = new Date();
    filterDate.setDate(filterDate.getDate() - 7); // Last 7 days
    
    console.log(`📅 Filtering transactions after: ${filterDate.toISOString()}`);
    
    if (!data.data) {
      console.log('⚠️ No data field in TRON API response');
      res.status(200).json({
        success: true,
        message: 'TRON API test completed',
        statusCode: statusCode,
        hasData: false,
        data: data,
        transactions: []
      });
      return;
    }
    
    console.log(`📊 Found ${data.data.length} raw transactions from TRON API`);
    
    const transactions = [];
    
    data.data.forEach((tx, index) => {
      console.log(`🔍 Processing transaction ${index + 1}:`, {
        txID: tx.txID,
        block_timestamp: tx.block_timestamp,
        date: new Date(tx.block_timestamp).toISOString()
      });
      
      const txDate = new Date(tx.block_timestamp);
      if (txDate < filterDate) {
        console.log(`⏰ Skipping transaction ${index + 1} - too old`);
        return;
      }
      
      if (tx.raw_data && tx.raw_data.contract) {
        tx.raw_data.contract.forEach((contract, contractIndex) => {
          console.log(`📋 Contract ${contractIndex + 1} type: ${contract.type}`);
          
          if (contract.type === "TransferContract") {
            const value = contract.parameter.value;
            const isDeposit = value.to_address === tronAddress;
            const amount = (value.amount / 1000000).toString();
            
            console.log(`💰 Transfer contract found:`, {
              from: value.owner_address,
              to: value.to_address,
              amount: amount,
              isDeposit: isDeposit
            });
            
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
              api_source: "TronGrid"
            });
          }
        });
      }
    });
    
    console.log(`✅ Processed ${transactions.length} valid transactions`);
    
    res.status(200).json({
      success: true,
      message: 'TRON API test completed',
      statusCode: statusCode,
      hasData: true,
      rawTransactionCount: data.data.length,
      processedTransactionCount: transactions.length,
      filterDate: filterDate.toISOString(),
      transactions: transactions,
      debug: {
        endpoint: endpoint,
        address: tronAddress
      }
    });
    
  } catch (error) {
    console.error('❌ Error testing TRON API:', error);
    res.status(500).json({
      success: false,
      error: error.message,
      message: 'TRON API test failed'
    });
  }
} 