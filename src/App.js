import React from 'react';

const CryptoTrackerProduction = () => {
  return (
    <div className="min-h-screen bg-gradient-to-br from-gray-900 to-gray-800 flex items-center justify-center p-4">
      <div className="text-center">
        <div className="bg-red-600 text-white rounded-full w-24 h-24 flex items-center justify-center mx-auto mb-6">
          <svg className="w-12 h-12" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
          </svg>
        </div>
        
        <h1 className="text-4xl font-bold text-white mb-4">
          Dashboard Deleted
        </h1>
        
        <p className="text-xl text-gray-300 mb-8 max-w-md mx-auto">
          This dashboard has been removed as requested. 
          The data fetching and API services continue to operate normally.
        </p>
        
        <div className="bg-gray-800 rounded-lg p-6 max-w-md mx-auto">
          <h2 className="text-lg font-semibold text-white mb-3">
            Services Still Active:
          </h2>
          <ul className="text-gray-300 space-y-2 text-left">
            <li className="flex items-center">
              <span className="w-2 h-2 bg-green-500 rounded-full mr-3"></span>
              Data fetching from crypto exchanges
            </li>
            <li className="flex items-center">
              <span className="w-2 h-2 bg-green-500 rounded-full mr-3"></span>
              Google Sheets integration
            </li>
            <li className="flex items-center">
              <span className="w-2 h-2 bg-green-500 rounded-full mr-3"></span>
              API endpoints for data processing
            </li>
            <li className="flex items-center">
              <span className="w-2 h-2 bg-green-500 rounded-full mr-3"></span>
              Automated transaction tracking
            </li>
          </ul>
        </div>
        
        <p className="text-sm text-gray-400 mt-8">
          Last updated: {new Date().toLocaleDateString()}
        </p>
      </div>
    </div>
  );
};

export default CryptoTrackerProduction;
