#!/usr/bin/env python3
"""
12-Hour Multi-Timeframe CipherB Analyzer
Part of cascading suppression system
"""

import os
import sys
import time
import json
import ccxt
import pandas as pd
import yaml
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(__file__))
from indicators.cipherb_exact import detect_exact_cipherb_signals
from alerts.telegram_multi import send_multi_alert
from alerts.suppression_multi import MultiTimeframeSuppressor

def get_ist_time():
    """Convert UTC to IST"""
    utc_now = datetime.utcnow()
    return utc_now + timedelta(hours=5, minutes=30)

class Analyzer12h:
    def __init__(self):
        self.config = self.load_config()
        self.suppressor = MultiTimeframeSuppressor()
        self.exchanges = self.init_exchanges()
        self.market_data = self.load_market_data()
        self.timeframe = '12h'
    
    def load_config(self):
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config_multi.yaml')
        with open(config_path) as f:
            return yaml.safe_load(f)
    
    def load_market_data(self):
        """Load filtered coins from cache"""
        cache_file = os.path.join(os.path.dirname(__file__), '..', 'cache', 'market_data_multi.json')
        
        if not os.path.exists(cache_file):
            print("❌ Market data cache not found - run data fetcher first")
            return []
        
        with open(cache_file) as f:
            data = json.load(f)
        
        coins = data.get('coins', [])
        print(f"📊 Loaded {len(coins)} coins for 12h analysis")
        return coins
    
    def init_exchanges(self):
        exchanges = []
        
        # Primary: BingX
        try:
            bingx = ccxt.bingx({
                'apiKey': os.getenv('BINGX_API_KEY', ''),
                'secret': os.getenv('BINGX_SECRET_KEY', ''),
                'rateLimit': 300,
                'enableRateLimit': True,
                'timeout': 30000,
            })
            exchanges.append(('BingX', bingx))
        except Exception as e:
            print(f"⚠️ BingX initialization failed: {e}")
        
        # Fallback: KuCoin
        try:
            kucoin = ccxt.kucoin({
                'rateLimit': 500,
                'enableRateLimit': True,
                'timeout': 30000,
            })
            exchanges.append(('KuCoin', kucoin))
        except Exception as e:
            print(f"⚠️ KuCoin initialization failed: {e}")
        
        return exchanges
    
    def fetch_ohlcv_data(self, symbol, timeframe='12h'):
        """Fetch OHLCV data with exchange fallback"""
        for exchange_name, exchange in self.exchanges:
            try:
                ohlcv = exchange.fetch_ohlcv(f"{symbol}/USDT", timeframe, limit=200)
                
                if len(ohlcv) < 50:
                    continue
                
                df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                df.set_index('timestamp', inplace=True)
                
                # Keep UTC timestamps for freshness checking
                df['utc_timestamp'] = df.index
                
                # Convert to IST for display
                df.index = df.index + pd.Timedelta(hours=5, minutes=30)
                
                if len(df) > 25 and df['close'].iloc[-1] > 0:
                    return df, exchange_name
                    
            except Exception as e:
                print(f"⚠️ {exchange_name} failed for {symbol}: {str(e)[:50]}")
                continue
        
        return None, None
    
    def analyze_coin(self, coin_data):
        """Analyze single coin for 12h CipherB signals with 12-hour freshness"""
        symbol = coin_data['symbol']
        
        try:
            # Fetch 12h OHLCV data
            df, exchange_used = self.fetch_ohlcv_data(symbol, '12h')
            
            if df is None or len(df) < 25:
                return None
            
            # Get signal timestamp for freshness check
            signal_timestamp_utc = df['utc_timestamp'].iloc[-1]
            
            # ✅ FRESHNESS CHECK: Signal must be within last 12 hours
            if not is_signal_fresh(signal_timestamp_utc, '12h'):
                signal_age_hours = (datetime.now(timezone.utc) - signal_timestamp_utc.replace(tzinfo=timezone.utc)).total_seconds() / 3600
                print(f"⏰ {symbol}: Signal too old ({signal_age_hours:.1f}h ago) - skipping")
                return None
            
            # Rest of the existing analyze_coin logic...
            # Calculate CipherB signals
            cipherb_signals = detect_exact_cipherb_signals(df, self.config['cipherb'])
            
            if cipherb_signals.empty:
                return None
            
            # Get latest signal
            latest_idx = -1
            latest_signal = cipherb_signals.iloc[latest_idx]
            
            signal_timestamp_utc = df['utc_timestamp'].iloc[latest_idx]
            signal_timestamp_ist = cipherb_signals.index[latest_idx]
            
            current_time = datetime.utcnow()
            time_since_signal = current_time - signal_timestamp_utc.to_pydatetime()
            
            # Check freshness
            freshness_limit = timedelta(minutes=self.config['alerts']['freshness_minutes'])
            if time_since_signal > freshness_limit:
                return None
            
            # Check for BUY signal
            if latest_signal['buySignal']:
                if self.suppressor.should_alert(symbol, 'BUY', self.timeframe, signal_timestamp_utc):
                    return {
                        'symbol': symbol,
                        'signal_type': 'BUY',
                        'timeframe': self.timeframe,
                        'wt1': latest_signal['wt1'],
                        'wt2': latest_signal['wt2'],
                        'price': coin_data['current_price'],
                        'change_24h': coin_data['price_change_percentage_24h'],
                        'market_cap': coin_data['market_cap'],
                        'volume_24h': coin_data['volume_24h'],
                        'exchange': exchange_used,
                        'timestamp': signal_timestamp_ist,
                        'signal_age_seconds': time_since_signal.total_seconds(),
                        'coin_data': coin_data
                    }
            
            # Check for SELL signal
            if latest_signal['sellSignal']:
                if self.suppressor.should_alert(symbol, 'SELL', self.timeframe, signal_timestamp_utc):
                    return {
                        'symbol': symbol,
                        'signal_type': 'SELL',
                        'timeframe': self.timeframe,
                        'wt1': latest_signal['wt1'],
                        'wt2': latest_signal['wt2'],
                        'price': coin_data['current_price'],
                        'change_24h': coin_data['price_change_percentage_24h'],
                        'market_cap': coin_data['market_cap'],
                        'volume_24h': coin_data['volume_24h'],
                        'exchange': exchange_used,
                        'timestamp': signal_timestamp_ist,
                        'signal_age_seconds': time_since_signal.total_seconds(),
                        'coin_data': coin_data
                    }
            
            return None
            
        except Exception as e:
            print(f"❌ {symbol} analysis failed: {str(e)[:100]}")
            return None
    
    def run_analysis(self):
        """Run 12h CipherB analysis"""
        ist_current = get_ist_time()
        
        print("="*80)
        print("🎯 12-HOUR MULTI-TIMEFRAME ANALYSIS")
        print("="*80)
        print(f"🕐 Analysis Time: {ist_current.strftime('%Y-%m-%d %H:%M:%S IST')}")
        print(f"⏰ Timeframe: 12-hour candles")
        print(f"🔄 Advanced suppression logic")
        print(f"🔍 Analyzing {len(self.market_data)} filtered coins")
        
        if not self.market_data:
            print("❌ No market data available")
            return
        
        # Clean up old records
        self.suppressor.cleanup_old_states()
        
        # Analyze coins in batches
        valid_signals = []
        batch_size = self.config['alerts']['batch_size']
        total_analyzed = 0
        
        for i in range(0, len(self.market_data), batch_size):
            batch = self.market_data[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (len(self.market_data) - 1) // batch_size + 1
            
            print(f"\n🔄 Processing batch {batch_num}/{total_batches}")
            
            for coin in batch:
                signal_result = self.analyze_coin(coin)
                if signal_result:
                    valid_signals.append(signal_result)
                    age_s = signal_result['signal_age_seconds']
                    print(f"🚨 12H {signal_result['signal_type']}: {signal_result['symbol']} "
                          f"({age_s:.0f}s ago)")
                
                total_analyzed += 1
                time.sleep(self.config['exchanges']['rate_limit'])
        
        # Send alerts
        if valid_signals:
            success = send_multi_alert(valid_signals, self.timeframe)
            if success:
                avg_age = sum(s['signal_age_seconds'] for s in valid_signals) / len(valid_signals)
                print(f"\n✅ SENT 12H MULTI-TIMEFRAME ALERT")
                print(f"   Valid signals: {len(valid_signals)}")
                print(f"   Average age: {avg_age:.0f} seconds")
            else:
                print(f"\n❌ Failed to send multi-timeframe alert")
        else:
            print(f"\n📊 No valid 12h signals detected")
        
        print(f"\n" + "="*80)
        print("🎯 12H MULTI-TIMEFRAME ANALYSIS COMPLETE")
        print("="*80)
        print(f"📊 Total analyzed: {total_analyzed}")
        print(f"🚨 Valid signals: {len(valid_signals)}")
        print(f"📱 Alert sent: {'Yes' if valid_signals else 'No'}")
        print(f"⏰ Next analysis: 12 hours")
        print("="*80)

if __name__ == '__main__':
    analyzer = Analyzer12h()
    analyzer.run_analysis()
