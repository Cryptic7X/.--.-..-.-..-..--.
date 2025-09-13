#!/usr/bin/env python3
"""
Multi-Exchange Handler with Failover Support
- BingX primary (with API authentication)
- KuCoin fallback (public API)
- Enhanced error handling and rate limiting
"""

import ccxt
import time
import pandas as pd
import os

class MultiExchangeHandler:
    def __init__(self, exchanges_config):
        self.exchanges = self.init_exchanges(exchanges_config)
    
    def init_exchanges(self, config):
        """Initialize exchange connections"""
        exchanges = []
        
        # BingX (Primary)
        try:
            bingx_config = {
                'apiKey': os.getenv('BINGX_API_KEY', ''),
                'secret': os.getenv('BINGX_SECRET_KEY', ''),
                'rateLimit': config['primary']['rate_limit'],
                'enableRateLimit': True,
                'timeout': config['primary']['timeout'] * 1000,  # Convert to milliseconds
                'sandbox': False,
            }
            
            bingx = ccxt.bingx(bingx_config)
            exchanges.append(('BingX', bingx))
            print("✅ BingX exchange initialized")
            
        except Exception as e:
            print(f"⚠️ BingX initialization failed: {e}")
        
        # KuCoin (Fallback)
        try:
            kucoin_config = {
                'rateLimit': config['fallback']['rate_limit'],
                'enableRateLimit': True,
                'timeout': config['fallback']['timeout'] * 1000,  # Convert to milliseconds
                'sandbox': False,
            }
            
            kucoin = ccxt.kucoin(kucoin_config)
            exchanges.append(('KuCoin', kucoin))
            print("✅ KuCoin exchange initialized")
            
        except Exception as e:
            print(f"⚠️ KuCoin initialization failed: {e}")
        
        if not exchanges:
            raise ValueError("No exchanges available - check configuration")
        
        return exchanges
    
    def fetch_ohlcv(self, symbol, timeframe, limit=200):
        """
        Fetch OHLCV data with exchange failover
        Returns (DataFrame, exchange_name) or (None, None)
        """
        trading_pair = f"{symbol}/USDT"
        
        for exchange_name, exchange in self.exchanges:
            try:
                print(f"🔍 Fetching {symbol} {timeframe} data from {exchange_name}...")
                
                # Fetch OHLCV data
                ohlcv_data = exchange.fetch_ohlcv(
                    symbol=trading_pair,
                    timeframe=timeframe,
                    limit=limit
                )
                
                if not ohlcv_data or len(ohlcv_data) < 50:
                    print(f"⚠️ Insufficient data from {exchange_name}: {len(ohlcv_data)} candles")
                    continue
                
                # Convert to DataFrame
                df = pd.DataFrame(
                    ohlcv_data,
                    columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']
                )
                
                # Convert timestamp to datetime
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                df.set_index('timestamp', inplace=True)
                
                # Basic data validation
                if df['close'].iloc[-1] <= 0:
                    print(f"⚠️ Invalid price data from {exchange_name}")
                    continue
                
                # Convert to IST for display (add 5:30)
                df.index = df.index + pd.Timedelta(hours=5, minutes=30)
                
                print(f"✅ Fetched {len(df)} candles from {exchange_name}")
                return df, exchange_name
                
            except ccxt.NetworkError as e:
                print(f"🌐 Network error with {exchange_name}: {e}")
                continue
            except ccxt.ExchangeError as e:
                print(f"🏛️ Exchange error with {exchange_name}: {e}")
                continue
            except Exception as e:
                print(f"❌ Unexpected error with {exchange_name}: {e}")
                continue
        
        print(f"❌ Failed to fetch {symbol} data from all exchanges")
        return None, None
    
    def test_connectivity(self):
        """Test connectivity to all configured exchanges"""
        results = {}
        
        for exchange_name, exchange in self.exchanges:
            try:
                # Test with a simple BTC fetch
                markets = exchange.load_markets()
                if 'BTC/USDT' in markets:
                    results[exchange_name] = True
                    print(f"✅ {exchange_name} connectivity: OK")
                else:
                    results[exchange_name] = False
                    print(f"⚠️ {exchange_name} connectivity: BTC/USDT not available")
            except Exception as e:
                results[exchange_name] = False
                print(f"❌ {exchange_name} connectivity: Failed ({e})")
        
        return results
