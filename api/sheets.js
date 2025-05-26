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
    // Simple test data - no Google Sheets integration yet
    const testData = {
      transactions: [
        {
          id: 1,
          timestamp: "2025-05-26 14:30:00",
          platform: "Binance (GC)",
          type: "deposit",
          asset: "USDT",
          amount: "2500.00",
          amount_aed: "9175.00",
          rate: "3.67",
          client: "Al Mahmoud Trading",
          remarks: "P2P Trading",
          from_address: "P2P Trade",
          to_address: "Binance Spot",
          tx_id: "BN_P2P_001",
          status: "Completed",
          network: "TRC20"
        },
        {
          id: 2,
          timestamp: "2025-05-26 16:20:00",
          platform: "Binance (Main)",
          type: "withdrawal",
          asset: "USDT",
          amount: "5000.00",
          amount_aed: "18350.00",
          rate: "3.67",
          client: "Trading Capital",
          remarks: "Fund Transfer",
          from_address: "Binance Spot",
          to_address: "TAUDu...6aJkN",
          tx_id: "BN_INTERNAL_002",
          status: "Completed",
          network: "TRC20"
        },
        {
          id: 3,
          timestamp: "2025-05-25 11:15:00",
          platform: "ByBit (CV)",
          type: "deposit",
          asset: "SOL",
          amount: "150.00",
          amount_aed: "27225.00",
          rate: "181.50",
          client: "Solana Investment",
          remarks: "Portfolio Diversification",
          from_address: "External Wallet",
          to_address: "ByBit Spot",
          tx_id: "BB_SOL_001",
          status: "Completed",
          network: "SOL"
        }
      ],
      walletBalances: {
        "Binance (GC)": { BTC: 0.25, ETH: 2.1, USDT: 15000, SOL: 0 },
        "Binance (Main)": { BTC: 0.1, ETH: 5.5, USDT: 25000, SOL: 45 },
        "Binance (CV)": { BTC: 0.05, ETH: 1.2, USDT: 8000, SOL: 20 },
        "ByBit (CV)": { BTC: 0, ETH: 0.8, USDT: 12000, SOL: 125 }
      },
      lastUpdated: new Date().toISOString(),
      status: "API Connection Working ✅"
    };

    console.log('API called successfully');
    res.status(200).json(testData);

  } catch (error) {
    console.error('API Error:', error);
    res.status(500).json({ 
      error: 'API Error',
      message: error.message,
      timestamp: new Date().toISOString()
    });
  }
}
