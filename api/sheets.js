import { GoogleSpreadsheet } from 'google-spreadsheet';

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') {
    res.status(200).end();
    return;
  }

  try {
    const doc = new GoogleSpreadsheet(process.env.GOOGLE_SHEET_ID);

    await doc.useServiceAccountAuth({
      client_email: process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL,
      private_key: process.env.GOOGLE_PRIVATE_KEY?.replace(/\\n/g, '\n'),
    });

    await doc.loadInfo();

    const sheet = doc.sheetsByTitle['FORMATTED_TRANSACTIONS'];
    const rows = await sheet.getRows();

    const transactions = rows.map((row, index) => ({
      id: index + 1,
      timestamp: row.get('Date & Time') || '',
      platform: row.get('Platform') || '',
      type: row.get('Type')?.toLowerCase() || '',
      asset: row.get('Asset') || '',
      amount: row.get('Amount') || '0',
      amount_aed: row.get('AED Value')?.replace('AED ', '').replace(',', '') || '0',
      rate: row.get('Rate') || '0',
      client: row.get('Client') || '',
      remarks: row.get('Remarks') || '',
      from_address: row.get('From Address') || '',
      to_address: row.get('To Address') || '',
      tx_id: row.get('TX ID') || '',
      status: 'Completed',
      network: 'Unknown'
    }));

    const walletBalances = {
      "Binance (GC)": { BTC: 0.25, ETH: 2.1, USDT: 15000, SOL: 0 },
      "Binance (Main)": { BTC: 0.1, ETH: 5.5, USDT: 25000, SOL: 45 },
      "Binance (CV)": { BTC: 0.05, ETH: 1.2, USDT: 8000, SOL: 20 },
      "ByBit (CV)": { BTC: 0, ETH: 0.8, USDT: 12000, SOL: 125 }
    };

    res.status(200).json({
      transactions,
      walletBalances,
      lastUpdated: new Date().toISOString()
    });

  } catch (error) {
    console.error('Google Sheets Error:', error);
    res.status(500).json({ error: 'Failed to fetch from Google Sheets', message: error.message });
  }
}
