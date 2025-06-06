import React, { useState, useEffect } from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, BarChart, Bar, PieChart, Pie, Cell } from 'recharts';
import { Download, RefreshCw, DollarSign, Circle, AlertTriangle, Wallet, TrendingUp, TrendingDown, Eye, EyeOff, Settings, Filter, HelpCircle, Info, Users, FileText, GitBranch, Calendar, Sun, Moon } from 'lucide-react';

const CryptoTrackerProduction = () => {
  const [transactionData, setTransactionData] = useState([]);
  const [walletBalances, setWalletBalances] = useState({});
  const [cryptoPrices, setCryptoPrices] = useState({});
  const [apiStatus, setApiStatus] = useState({});
  const [isLoading, setIsLoading] = useState(true);
  const [lastUpdated, setLastUpdated] = useState(null);
  const [activeTab, setActiveTab] = useState('dashboard');
  const [selectedPlatform, setSelectedPlatform] = useState('All');
  const [selectedDateRange, setSelectedDateRange] = useState('7d');
  const [showBalances, setShowBalances] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState(null);
  const [darkMode, setDarkMode] = useState(false);
  const [dateFilter, setDateFilter] = useState({ from: '', to: '', preset: 'YTD' });

  // Fetch data from Google Sheets via API
  const fetchDataFromSheets = async () => {
    setRefreshing(true);
    setError(null);
    
    try {
      const response = await fetch('/api/sheets');
      
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      
      const data = await response.json();
      
      // Set the data from API
      setTransactionData(data.transactions || []);
      setWalletBalances(data.walletBalances || {});
      setCryptoPrices(data.cryptoPrices || {});
      setApiStatus(data.apiStatus || {});
      setLastUpdated(new Date(data.lastUpdated).toLocaleString());
      setIsLoading(false);
      setRefreshing(false);
      
    } catch (error) {
      console.error('Error fetching data from API:', error);
      setError(error.message);
      setIsLoading(false);
      setRefreshing(false);
      
      // Set empty data on error
      setTransactionData([]);
      setWalletBalances({});
      setCryptoPrices({});
      setApiStatus({});
      setLastUpdated('Failed to load');
    }
  };

  useEffect(() => {
    fetchDataFromSheets();
  }, []);

  // Calculate totals in AED (accepts filtered transactions)
  const getTotalValues = (transactions = transactionData) => {
    let totalDeposits = 0;
    let totalWithdrawals = 0;
    
    transactions.forEach(tx => {
      const aedValue = parseFloat(tx.amount_aed) || 0;
      if (tx.type === "deposit") {
        totalDeposits += aedValue;
      } else {
        totalWithdrawals += aedValue;
      }
    });
    
    return {
      deposits: totalDeposits,
      withdrawals: totalWithdrawals,
      balance: totalDeposits - totalWithdrawals
    };
  };

  // Calculate total portfolio value using live prices
  const getPortfolioValue = () => {
    let totalValue = 0;
    
    Object.entries(walletBalances).forEach(([wallet, assets]) => {
      // Skip if wallet has error
      if (assets.error) {
        return;
      }
      
      Object.entries(assets).forEach(([asset, amount]) => {
        const price = cryptoPrices[asset]?.AED || 0;
        totalValue += (amount * price);
      });
    });

    return totalValue;
  };

  // Apply date filtering
  const getFilteredTransactions = () => {
    let filtered = transactionData;
    
    // Apply platform filter
    if (selectedPlatform !== 'All') {
      filtered = filtered.filter(tx => tx.platform === selectedPlatform);
    }
    
    // Apply date filter
    if (dateFilter.from || dateFilter.to || dateFilter.preset !== 'All') {
      const now = new Date();
      let startDate, endDate;
      
      if (dateFilter.preset === 'YTD') {
        startDate = new Date(now.getFullYear(), 0, 1);
        endDate = now;
      } else if (dateFilter.preset === 'MTD') {
        startDate = new Date(now.getFullYear(), now.getMonth(), 1);
        endDate = now;
      } else if (dateFilter.from && dateFilter.to) {
        startDate = new Date(dateFilter.from);
        endDate = new Date(dateFilter.to);
      }
      
      if (startDate && endDate) {
        filtered = filtered.filter(tx => {
          const txDate = new Date(tx.timestamp);
          return txDate >= startDate && txDate <= endDate;
        });
      }
    }
    
    return filtered;
  };

  const portfolioValue = getPortfolioValue();
  const platforms = ['All', ...new Set(transactionData.map(tx => tx.platform))];
  const filteredTransactions = getFilteredTransactions();
  const totals = getTotalValues(filteredTransactions);

  // Prepare chart data
  const preparePlatformChart = () => {
    const platformData = {};
    
    filteredTransactions.forEach(tx => {
      if (!platformData[tx.platform]) {
        platformData[tx.platform] = { 
          name: tx.platform, 
          deposits: 0, 
          withdrawals: 0 
        };
      }
      
      const aedValue = parseFloat(tx.amount_aed) || 0;
      if (tx.type === "deposit") {
        platformData[tx.platform].deposits += aedValue;
      } else {
        platformData[tx.platform].withdrawals += aedValue;
      }
    });
    
    return Object.values(platformData);
  };

  const prepareAssetDistribution = () => {
    const assetTotals = {};
    Object.entries(walletBalances).forEach(([wallet, assets]) => {
      // Skip if wallet has error
      if (assets.error) {
        return;
      }
      
      Object.entries(assets).forEach(([asset, amount]) => {
        if (!assetTotals[asset]) assetTotals[asset] = 0;
        
        // Use live prices
        const price = cryptoPrices[asset]?.AED || 0;
        assetTotals[asset] += amount * price;
      });
    });

    const colors = ['#3B82F6', '#10B981', '#F59E0B', '#EF4444', '#8B5CF6', '#06B6D4'];
    return Object.entries(assetTotals)
      .filter(([asset, value]) => value > 0)
      .map(([asset, value], index) => ({
        name: asset,
        value: value,
        color: colors[index % colors.length]
      }));
  };

  const prepareDailyVolumeChart = () => {
    const dailyData = {};
    
    filteredTransactions.forEach(tx => {
      const date = tx.timestamp.split(' ')[0];
      if (!dailyData[date]) {
        dailyData[date] = { name: date, deposits: 0, withdrawals: 0 };
      }
      
      const aedValue = parseFloat(tx.amount_aed) || 0;
      if (tx.type === "deposit") {
        dailyData[date].deposits += aedValue;
      } else {
        dailyData[date].withdrawals += aedValue;
      }
    });
    
    const sortedData = Object.values(dailyData).sort((a, b) => {
      return new Date(a.name) - new Date(b.name);
    });
    
    return sortedData;
  };

  // Get status color for API connections
  const getStatusColor = (status) => {
    switch (status) {
      case 'Active': return 'text-green-500';
      case 'Error': return 'text-red-500';
      case 'Missing Credentials': return 'text-yellow-500';
      default: return 'text-gray-500';
    }
  };

  const getStatusIcon = (status) => {
    switch (status) {
      case 'Active': return 'text-green-500';
      case 'Error': return 'text-red-500';
      case 'Missing Credentials': return 'text-yellow-500';
      default: return 'text-gray-500';
    }
  };

  // Theme classes
  const themeClasses = darkMode ? 'bg-gray-900 text-white' : 'bg-gray-50 text-gray-900';
  const cardClasses = darkMode ? 'bg-gray-800 text-white' : 'bg-white text-gray-900';
  const headerClasses = darkMode ? 'bg-gray-800' : 'bg-gradient-to-r from-blue-600 to-indigo-700';

  // Loading state
  if (isLoading) {
    return (
      <div className={`flex flex-col items-center justify-center min-h-screen ${themeClasses} p-4`}>
        <div className="text-2xl font-bold mb-4">Loading Your Crypto Portfolio...</div>
        <div className="w-16 h-16 border-4 border-t-blue-500 border-blue-200 rounded-full animate-spin"></div>
        <p className="mt-4">Fetching real-time data from all sources...</p>
      </div>
    );
  }

  // Error state
  if (error && transactionData.length === 0) {
    return (
      <div className={`flex flex-col items-center justify-center min-h-screen ${themeClasses} p-4`}>
        <div className="text-2xl font-bold text-red-600 mb-4">Connection Error</div>
        <div className="mb-4">Unable to fetch data from APIs</div>
        <div className="text-sm mb-6">Error: {error}</div>
        <button 
          onClick={fetchDataFromSheets}
          className="px-6 py-3 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition"
        >
          Retry Connection
        </button>
      </div>
    );
  }

  return (
    <div className={`min-h-screen ${themeClasses}`}>
      {/* Header */}
      <div className={`${headerClasses} p-6 shadow-lg`}>
        <div className="max-w-7xl mx-auto">
          <div className="flex justify-between items-start">
            <div>
              <h1 className="text-3xl font-bold text-white">Crypto Portfolio Tracker</h1>
              <p className="text-blue-100 mt-2">Real-time tracking across all your wallets and exchanges</p>
            </div>
            <div className="flex items-center space-x-4">
              {/* Date Filter */}
              <div className="flex items-center space-x-2">
                <select
                  className="px-3 py-2 bg-blue-500 text-white rounded-lg text-sm"
                  value={dateFilter.preset}
                  onChange={(e) => setDateFilter({...dateFilter, preset: e.target.value})}
                >
                  <option value="All">All Time</option>
                  <option value="YTD">YTD</option>
                  <option value="MTD">MTD</option>
                  <option value="Custom">Custom</option>
                </select>
                
                {dateFilter.preset === 'Custom' && (
                  <>
                    <input
                      type="date"
                      className="px-2 py-1 bg-blue-500 text-white rounded text-sm"
                      value={dateFilter.from}
                      onChange={(e) => setDateFilter({...dateFilter, from: e.target.value})}
                    />
                    <input
                      type="date"
                      className="px-2 py-1 bg-blue-500 text-white rounded text-sm"
                      value={dateFilter.to}
                      onChange={(e) => setDateFilter({...dateFilter, to: e.target.value})}
                    />
                  </>
                )}
              </div>

              <button 
                onClick={() => setDarkMode(!darkMode)}
                className="flex items-center px-3 py-2 bg-blue-500 text-white rounded-lg hover:bg-blue-400 transition"
              >
                {darkMode ? <Sun size={16} /> : <Moon size={16} />}
                <span className="ml-2">{darkMode ? 'Light' : 'Dark'}</span>
              </button>
              
              <button 
                onClick={() => setShowBalances(!showBalances)}
                className="flex items-center px-3 py-2 bg-blue-500 text-white rounded-lg hover:bg-blue-400 transition"
              >
                {showBalances ? <EyeOff size={16} /> : <Eye size={16} />}
                <span className="ml-2">{showBalances ? 'Hide' : 'Show'} Balances</span>
              </button>
              
              <button 
                onClick={fetchDataFromSheets}
                disabled={refreshing}
                className="flex items-center px-3 py-2 bg-green-500 text-white rounded-lg hover:bg-green-400 transition disabled:opacity-50"
              >
                <RefreshCw size={16} className={refreshing ? 'animate-spin' : ''} />
                <span className="ml-2">Refresh</span>
              </button>
            </div>
          </div>
          
          <div className="flex mt-6 text-sm">
            {[
              { id: 'dashboard', label: 'Dashboard', icon: TrendingUp },
              { id: 'transactions', label: 'Transactions', icon: Circle },
              { id: 'wallets', label: 'Wallets', icon: Wallet },
              { id: 'settings', label: 'Settings', icon: Settings },
              { id: 'help', label: 'Help & Info', icon: HelpCircle }
            ].map(tab => (
              <button 
                key={tab.id}
                className={`flex items-center px-4 py-2 font-medium rounded-t-lg transition mr-2 ${
                  activeTab === tab.id 
                    ? `${cardClasses} text-blue-700` 
                    : 'bg-blue-500 text-white hover:bg-blue-400'
                }`}
                onClick={() => setActiveTab(tab.id)}
              >
                <tab.icon size={16} className="mr-2" />
                {tab.label}
              </button>
            ))}
          </div>
        </div>
      </div>
      
      <div className="max-w-7xl mx-auto p-6">
        {/* Status Bar */}
        <div className={`${cardClasses} p-4 rounded-lg shadow mb-6 flex justify-between items-center`}>
          <div className="flex items-center">
            <Circle size={12} className={error ? "text-red-500" : "text-green-500"} fill="currentColor" />
            <span className="ml-2">{error ? 'Connection issues detected' : 'Connected to Live APIs • Real-time sync active'}</span>
          </div>
          <div className="flex items-center text-gray-500 text-sm">
            <span>Last updated: {lastUpdated}</span>
          </div>
        </div>
        
        {activeTab === 'dashboard' && (
          <>
            {/* Portfolio Summary Cards */}
            <div className="grid grid-cols-1 md:grid-cols-4 gap-6 mb-6">
              <div className={`${cardClasses} p-6 rounded-lg shadow-md`}>
                <div className="flex items-center mb-2">
                  <div className="p-2 bg-blue-100 rounded-lg mr-3">
                    <Wallet size={24} className="text-blue-600" />
                  </div>
                  <h3 className="text-lg font-medium">Portfolio Value</h3>
                </div>
                <p className="text-3xl font-bold">
                  {showBalances ? `AED ${portfolioValue.toLocaleString(undefined, { maximumFractionDigits: 2 })}` : '••••••'}
                </p>
                <p className="text-sm text-gray-500 mt-1">Current holdings</p>
              </div>
              
              <div className={`${cardClasses} p-6 rounded-lg shadow-md`}>
                <div className="flex items-center mb-2">
                  <div className="p-2 bg-green-100 rounded-lg mr-3">
                    <TrendingUp size={24} className="text-green-600" />
                  </div>
                  <h3 className="text-lg font-medium">Total Deposits</h3>
                </div>
                <p className="text-3xl font-bold text-green-600">
                  {showBalances ? `AED ${totals.deposits.toLocaleString(undefined, { maximumFractionDigits: 2 })}` : '••••••'}
                </p>
                <p className="text-sm text-gray-500 mt-1">All-time inflows</p>
              </div>
              
              <div className={`${cardClasses} p-6 rounded-lg shadow-md`}>
                <div className="flex items-center mb-2">
                  <div className="p-2 bg-red-100 rounded-lg mr-3">
                    <TrendingDown size={24} className="text-red-600" />
                  </div>
                  <h3 className="text-lg font-medium">Total Withdrawals</h3>
                </div>
                <p className="text-3xl font-bold text-red-600">
                  {showBalances ? `AED ${totals.withdrawals.toLocaleString(undefined, { maximumFractionDigits: 2 })}` : '••••••'}
                </p>
                <p className="text-sm text-gray-500 mt-1">All-time outflows</p>
              </div>
              
              <div className={`${cardClasses} p-6 rounded-lg shadow-md`}>
                <div className="flex items-center mb-2">
                  <div className="p-2 bg-purple-100 rounded-lg mr-3">
                    <DollarSign size={24} className="text-purple-600" />
                  </div>
                  <h3 className="text-lg font-medium">Net Flow</h3>
                </div>
                <p className={`text-3xl font-bold ${totals.balance >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                  {showBalances ? `AED ${totals.balance.toLocaleString(undefined, { maximumFractionDigits: 2 })}` : '••••••'}
                </p>
                <p className="text-sm text-gray-500 mt-1">Net position</p>
              </div>
            </div>
            
            {/* Charts */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-6">
              <div className={`${cardClasses} p-6 rounded-lg shadow-md`}>
                <h3 className="text-lg font-medium mb-4">Platform Activity (AED)</h3>
                <div className="h-64">
                  {preparePlatformChart().length > 0 ? (
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart data={preparePlatformChart()}>
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis 
                          dataKey="name" 
                          fontSize={12}
                          angle={-45}
                          textAnchor="end"
                          height={80}
                        />
                        <YAxis 
                          fontSize={12}
                          tickFormatter={(value) => `${(value / 1000).toFixed(0)}K`}
                        />
                        <Tooltip formatter={(value) => [`AED ${value.toLocaleString()}`, 'Value']} />
                        <Legend />
                        <Bar dataKey="deposits" name="Deposits" fill="#10B981" />
                        <Bar dataKey="withdrawals" name="Withdrawals" fill="#EF4444" />
                      </BarChart>
                    </ResponsiveContainer>
                  ) : (
                    <div className="flex items-center justify-center h-full text-gray-500">
                      No transaction data available
                    </div>
                  )}
                </div>
              </div>
              
              <div className={`${cardClasses} p-6 rounded-lg shadow-md`}>
                <h3 className="text-lg font-medium mb-4">Asset Distribution</h3>
                <div className="h-64">
                  {prepareAssetDistribution().length > 0 ? (
                    <ResponsiveContainer width="100%" height="100%">
                      <PieChart>
                        <Pie
                          data={prepareAssetDistribution()}
                          cx="50%"
                          cy="50%"
                          innerRadius={40}
                          outerRadius={100}
                          fill="#8884d8"
                          dataKey="value"
                          label={({name, percent}) => `${name} ${(percent * 100).toFixed(0)}%`}
                        >
                          {prepareAssetDistribution().map((entry, index) => (
                            <Cell key={`cell-${index}`} fill={entry.color} />
                          ))}
                        </Pie>
                        <Tooltip formatter={(value) => [`AED ${value.toLocaleString()}`, 'Value']} />
                      </PieChart>
                    </ResponsiveContainer>
                  ) : (
                    <div className="flex items-center justify-center h-full text-gray-500">
                      No wallet balance data available
                    </div>
                  )}
                </div>
              </div>
            </div>

            {/* Daily Transaction Volume Chart */}
            <div className={`${cardClasses} p-6 rounded-lg shadow-md mb-6`}>
              <h3 className="text-lg font-medium mb-4">Daily Transaction Volume</h3>
              <div className="h-80">
                {prepareDailyVolumeChart().length > 0 ? (
                  <ResponsiveContainer width="100%" height="100%">
                    <LineChart data={prepareDailyVolumeChart()}>
                      <CartesianGrid strokeDasharray="3 3" />
                      <XAxis 
                        dataKey="name" 
                        fontSize={12}
                        tickFormatter={(value) => {
                          const date = new Date(value);
                          return `${date.getMonth() + 1}-${date.getDate()}`;
                        }}
                      />
                      <YAxis 
                        fontSize={12}
                        tickFormatter={(value) => `${(value / 1000).toFixed(0)}K`}
                      />
                      <Tooltip 
                        formatter={(value) => [`AED ${value.toLocaleString()}`, '']}
                        labelFormatter={(label) => `Date: ${label}`}
                      />
                      <Legend />
                      <Line 
                        type="monotone" 
                        dataKey="deposits" 
                        stroke="#10B981" 
                        strokeWidth={3}
                        fill="#10B981"
                        fillOpacity={0.1}
                        name="Deposits"
                        dot={{ fill: '#10B981', strokeWidth: 2, r: 6 }}
                        activeDot={{ r: 8, stroke: '#10B981', strokeWidth: 2 }}
                      />
                      <Line 
                        type="monotone" 
                        dataKey="withdrawals" 
                        stroke="#EF4444" 
                        strokeWidth={3}
                        fill="#EF4444"
                        fillOpacity={0.1}
                        name="Withdrawals"
                        dot={{ fill: '#EF4444', strokeWidth: 2, r: 6 }}
                        activeDot={{ r: 8, stroke: '#EF4444', strokeWidth: 2 }}
                      />
                    </LineChart>
                  </ResponsiveContainer>
                ) : (
                  <div className="flex items-center justify-center h-full text-gray-500">
                    No daily transaction data available
                  </div>
                )}
              </div>
            </div>
          </>
        )}
        
        {activeTab === 'transactions' && (
          <div className={`${cardClasses} p-6 rounded-lg shadow-md`}>
            <div className="flex flex-col sm:flex-row justify-between items-start sm:items-center mb-6">
              <h3 className="text-xl font-medium mb-2 sm:mb-0">Transaction History</h3>
              
              <div className="flex flex-col sm:flex-row items-start sm:items-center space-y-2 sm:space-y-0 sm:space-x-4">
                <div>
                  <label className="block text-sm font-medium mb-1">Platform</label>
                  <select
                    className="block w-full pl-3 pr-10 py-2 text-base border-gray-300 focus:outline-none focus:ring-blue-500 focus:border-blue-500 sm:text-sm rounded-md"
                    value={selectedPlatform}
                    onChange={(e) => setSelectedPlatform(e.target.value)}
                  >
                    {platforms.map((platform) => (
                      <option key={platform} value={platform}>{platform}</option>
                    ))}
                  </select>
                </div>
                
                <button className="flex items-center px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition">
                  <Download size={16} className="mr-2" />
                  Export CSV
                </button>
              </div>
            </div>
            
            <div className="overflow-x-auto">
              {filteredTransactions.length > 0 ? (
                <table className="min-w-full">
                  <thead>
                    <tr className={darkMode ? 'bg-gray-700' : 'bg-gray-50'}>
                      <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Date & Time</th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Platform</th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Type</th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Asset</th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Amount</th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">AED Value</th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Client</th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Remarks</th>
                    </tr>
                  </thead>
                  <tbody className={`divide-y ${darkMode ? 'divide-gray-600' : 'divide-gray-200'}`}>
                    {filteredTransactions.map((tx) => (
                      <tr key={tx.id} className={`hover:${darkMode ? 'bg-gray-700' : 'bg-gray-50'}`}>
                        <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">{tx.timestamp}</td>
                        <td className="px-6 py-4 whitespace-nowrap text-sm">{tx.platform}</td>
                        <td className="px-6 py-4 whitespace-nowrap text-sm">
                          <span className={`px-2 inline-flex text-xs leading-5 font-semibold rounded-full ${
                            tx.type === 'deposit' ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-800'
                          }`}>
                            {tx.type}
                          </span>
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-sm font-medium">{tx.asset}</td>
                        <td className="px-6 py-4 whitespace-nowrap text-sm">{parseFloat(tx.amount).toFixed(8)}</td>
                        <td className="px-6 py-4 whitespace-nowrap text-sm font-medium">
                          {showBalances ? `AED ${parseFloat(tx.amount_aed).toLocaleString()}` : '••••••'}
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">{tx.client}</td>
                        <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">{tx.remarks}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              ) : (
                <div className="text-center py-12">
                  <p className="text-gray-500">No transactions found for selected filters.</p>
                </div>
              )}
            </div>
          </div>
        )}
        
        {activeTab === 'wallets' && (
          <div className="space-y-6">
            <div className={`${cardClasses} p-6 rounded-lg shadow-md`}>
              <h3 className="text-xl font-medium mb-4">Live Wallet Balances</h3>
              
              {Object.keys(walletBalances).length > 0 ? (
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                  {Object.entries(walletBalances).map(([wallet, assets]) => (
                    <div key={wallet} className={`border rounded-lg p-4 ${darkMode ? 'border-gray-600' : 'border-gray-200'}`}>
                      <h4 className="font-medium mb-3">{wallet}</h4>
                      
                      {assets.error ? (
                        <div className="text-red-500 text-sm">
                          Error: {assets.error}
                        </div>
                      ) : (
                        <div className="space-y-2">
                          {Object.entries(assets).map(([asset, amount]) => {
                            const price = cryptoPrices[asset]?.AED || 0;
                            const value = amount * price;
                            
                            return (
                              <div key={asset} className="flex justify-between items-center">
                                <span className="text-sm text-gray-600">{asset}</span>
                                <div className="text-right">
                                  <div className="text-sm font-medium">
                                    {showBalances ? amount.toFixed(8) : '••••••'}
                                  </div>
                                  <div className="text-xs text-gray-500">
                                    {showBalances ? `AED ${value.toLocaleString()}` : '••••••'}
                                  </div>
                                </div>
                              </div>
                            );
                          })}
                        </div>
                      )}
                    </div>
                  ))}
                </div>
              ) : (
                <div className="text-center py-12">
                  <p className="text-gray-500">Loading wallet balances...</p>
                </div>
              )}
            </div>
          </div>
        )}
        
        {activeTab === 'settings' && (
          <div className={`${cardClasses} p-6 rounded-lg shadow-md`}>
            <h3 className="text-xl font-medium mb-4">System Configuration</h3>
            
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              <div>
                <h4 className="text-lg font-medium mb-3">Connected Accounts</h4>
                <div className="space-y-3">
                  {Object.entries(apiStatus)
                    .filter(([name]) => name.includes('Binance') || name.includes('ByBit'))
                    .map(([account, status]) => (
                    <div key={account} className={`flex justify-between items-center p-3 border rounded ${darkMode ? 'border-gray-600' : 'border-gray-200'}`}>
                      <div>
                        <span className="font-medium">{account}</span>
                        <p className="text-sm text-gray-500">
                          {status === 'Active' ? 'Connected & Active' : status === 'Error' ? 'Connection Error' : 'Setup Required'}
                        </p>
                      </div>
                      <Circle size={12} className={getStatusIcon(status)} fill="currentColor" />
                    </div>
                  ))}
                </div>
              </div>
              
              <div>
                <h4 className="text-lg font-medium mb-3">Blockchain Wallets</h4>
                <div className="space-y-3">
                  {Object.entries(apiStatus)
                    .filter(([name]) => name.includes('Wallet'))
                    .map(([wallet, status]) => (
                    <div key={wallet} className={`flex justify-between items-center p-3 border rounded ${darkMode ? 'border-gray-600' : 'border-gray-200'}`}>
                      <div>
                        <span className="font-medium">{wallet}</span>
                        <p className="text-sm text-gray-500">
                          {status === 'Active' ? 'Monitoring Active' : 'Connection Error'}
                        </p>
                      </div>
                      <Circle size={12} className={getStatusIcon(status)} fill="currentColor" />
                    </div>
                  ))}
                </div>
              </div>
            </div>
          </div>
        )}
        
        {activeTab === 'help' && (
          <div className="space-y-6">
            {/* Company Information */}
            <div className={`${cardClasses} p-6 rounded-lg shadow-md`}>
              <div className="flex items-center mb-4">
                <Users size={24} className="text-blue-600 mr-3" />
                <h3 className="text-xl font-medium">Developer Information</h3>
              </div>
              
              <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                <div>
                  <h4 className="text-lg font-medium mb-3">Development Team</h4>
                  <div className="space-y-2">
                    <p><span className="font-medium">Developer:</span> Darshana</p>
                    <p><span className="font-medium">Platform:</span> Fiverr Professional</p>
                    <p><span className="font-medium">Specialization:</span> API Integration & Automation</p>
                    <p><span className="font-medium">Experience:</span> 5+ years in Crypto & Financial APIs</p>
                  </div>
                  
                  <div className="mt-4 pt-4 border-t border-gray-200">
                    <h5 className="text-md font-medium mb-2">Our Websites</h5>
                    <div className="space-y-2">
                      <div>
                        <a 
                          href="https://www.fiverr.com/sellers/xlsolutions/" 
                          target="_blank" 
                          rel="noopener noreferrer"
                          className="text-blue-600 hover:text-blue-800 underline text-sm"
                        >
                          🔗 Fiverr Professional Profile
                        </a>
                        <p className="text-xs text-gray-500 ml-4">Professional services & portfolio</p>
                      </div>
                      <div>
                        <a 
                          href="https://www.anydata.lk/" 
                          target="_blank" 
                          rel="noopener noreferrer"
                          className="text-blue-600 hover:text-blue-800 underline text-sm"
                        >
                          🔗 AnyData.lk - Data Solutions
                        </a>
                        <p className="text-xs text-gray-500 ml-4">Specialized data automation services</p>
                      </div>
                    </div>
                  </div>
                </div>
                
                <div>
                  <h4 className="text-lg font-medium mb-3">Support & Contact</h4>
                  <div className="space-y-2">
                    <p><span className="font-medium">Support Period:</span> 30 days post-delivery</p>
                    <p><span className="font-medium">Response Time:</span> Within 24 hours</p>
                    <p><span className="font-medium">Maintenance:</span> Available for long-term contracts</p>
                    <p><span className="font-medium">Updates:</span> Feature enhancements available</p>
                  </div>
                </div>
              </div>
            </div>

            {/* Features */}
            <div className={`${cardClasses} p-6 rounded-lg shadow-md`}>
              <div className="flex items-center mb-4">
                <FileText size={24} className="text-green-600 mr-3" />
                <h3 className="text-xl font-medium">📖 System Features</h3>
              </div>
              
              <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                <div>
                  <h4 className="text-lg font-medium mb-3">✨ New Features</h4>
                  <ul className="list-disc list-inside space-y-2 text-sm">
                    <li><strong>🔄 Live Data:</strong> Real-time wallet balances and crypto prices</li>
                    <li><strong>📊 Live Portfolio:</strong> Portfolio value updates automatically</li>
                    <li><strong>🎯 Date Filtering:</strong> YTD, MTD, and custom date ranges</li>
                    <li><strong>🌙 Dark Mode:</strong> Toggle between light and dark themes</li>
                    <li><strong>📈 Improved Charts:</strong> Better visibility and real-time data</li>
                    <li><strong>🔗 API Status:</strong> Real connection status monitoring</li>
                  </ul>
                </div>
                
                <div>
                  <h4 className="text-lg font-medium mb-3">🔧 Technical Updates</h4>
                  <ul className="list-disc list-inside space-y-2 text-sm">
                    <li><strong>💰 Live Prices:</strong> CoinGecko API integration for AED/USD rates</li>
                    <li><strong>🏦 Exchange APIs:</strong> Real-time balance fetching from all exchanges</li>
                    <li><strong>⛓️ Blockchain APIs:</strong> Live wallet balance monitoring</li>
                    <li><strong>🛡️ Error Handling:</strong> Graceful handling of API failures</li>
                    <li><strong>📱 Responsive:</strong> Better mobile and tablet experience</li>
                    <li><strong>⚡ Performance:</strong> Optimized loading and refresh speeds</li>
                  </ul>
                </div>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

export default CryptoTrackerProduction;
