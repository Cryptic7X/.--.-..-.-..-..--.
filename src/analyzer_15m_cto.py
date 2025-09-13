#!/usr/bin/env python3
"""
15-Minute CipherB + CTO Confirmation Analyzer
- CipherB primary signals
- CTO confirmation (±70 thresholds)
- 4-hour cooldown deduplication
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
from indicators.composite_trend_oscillator import CompositeTrendOscillator
from alerts.telegram_15m import send_15m_alert
from alerts.deduplication_15m import Deduplicator15m

def get_ist_time():
    """Convert UTC to IST"""
    utc_now = datetime.utcnow()
    return utc_now + timedelta(hours=5, minutes=30)

class Analyzer15mCTO:
    def __init__(self):
        self.config = self.load_config()
        self.cto = CompositeTrendOscillator(**self.config['cto'])
        self.deduplicator = Deduplicator15m()
        self.exchanges = self.init_exchanges()
        self.market_data = self.load_market_data()
    
    def load_config(self):
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config_15m.yaml')
        with open(config_path) as f:
            return yaml.safe_load(f)
    
    def load_market_data(self):
        """Load top 100 coins from cache"""
        cache_file = os.path.join(os.path.dirname(__file__), '..', 'cache', 'market_data_15m.json')
        
        if not os.path.exists(cache_file):
            print("❌ Market data cache not found - run data fetcher first")
            return []
        
        with open(cache_file) as f:
            data = json.load(f)
        
        coins = data.get('coins', [])
        print(f"📊 Loaded {len(coins)} coins for 15m analysis")
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
    
    def fetch_ohlcv_data(self, symbol, timeframe='15m'):
        """Fetch OHLCV data with exchange fallback"""
        for exchange_name, exchange in self.exchanges:
            try:
                ohlcv = exchange.fetch_ohlcv(f"{symbol}/USDT", timeframe, limit=200)
                
                if len(ohlcv) < 100:
                    continue
                
                df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                df.set_index('timestamp', inplace=True)
                
                # Keep UTC timestamps for freshness checking
                df['utc_timestamp'] = df.index
                
                # Convert to IST for display
                df.index = df.index + pd.Timedelta(hours=5, minutes=30)
                
                if len(df) > 50 and df['close'].iloc[-1] > 0:
                    return df, exchange_name
                    
            except Exception as e:
                print(f"⚠️ {exchange_name} failed for {symbol}: {str(e)[:50]}")
                continue
        
        return None, None
    
    def analyze_coin(self, coin_data):
        """Analyze single coin for CipherB + CTO confirmation"""
        symbol = coin_data['symbol']
        
        try:
            # Fetch 15m OHLCV data
            df, exchange_used = self.fetch_ohlcv_data(symbol, '15m')
            
            if df is None or len(df) < 50:
                return None
            
            # Calculate CipherB signals
            cipherb_signals = detect_exact_cipherb_signals(df, self.config['cipherb'])
            
            if cipherb_signals.empty:
                return None
            
            # Calculate CTO indicators
            cto_df = self.cto.calculate_signals(df)
            cto_final = self.cto.detect_cto_conditions(
                cto_df, 
                self.config['cto']['overbought_threshold'],
                self.config['cto']['oversold_threshold']
            )
            
            # Get latest signals
            latest_idx = -1
            latest_cipherb = cipherb_signals.iloc[latest_idx]
            latest_cto = cto_final.iloc[latest_idx]
            
            signal_timestamp_utc = df['utc_timestamp'].iloc[latest_idx]
            signal_timestamp_ist = cipherb_signals.index[latest_idx]
            
            current_time = datetime.utcnow()
            time_since_signal = current_time - signal_timestamp_utc.to_pydatetime()
            
            # Check for confirmed BUY signal
            if latest_cipherb['buySignal'] and latest_cto['cto_oversold']:
                if self.deduplicator.is_signal_allowed(symbol, 'BUY', signal_timestamp_utc):
                    return {
                        'symbol': symbol,
                        'signal_type': 'BUY',
                        'cipherb_wt1': latest_cipherb['wt1'],
                        'cipherb_wt2': latest_cipherb['wt2'],
                        'cto_score': latest_cto['cto_score'],
                        'price': coin_data['current_price'],
                        'change_24h': coin_data['price_change_percentage_24h'],
                        'market_cap': coin_data['market_cap'],
                        'volume_24h': coin_data['volume_24h'],
                        'exchange': exchange_used,
                        'timestamp': signal_timestamp_ist,
                        'signal_age_seconds': time_since_signal.total_seconds(),
                        'coin_data': coin_data
                    }
            
            # Check for confirmed SELL signal
            if latest_cipherb['sellSignal'] and latest_cto['cto_overbought']:
                if self.deduplicator.is_signal_allowed(symbol, 'SELL', signal_timestamp_utc):
                    return {
                        'symbol': symbol,
                        'signal_type': 'SELL',
                        'cipherb_wt1': latest_cipherb['wt1'],
                        'cipherb_wt2': latest_cipherb['wt2'],
                        'cto_score': latest_cto['cto_score'],
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
        """Run 15m CipherB + CTO analysis"""
        ist_current = get_ist_time()
        
        print("="*80)
        print("🎯 15-MINUTE CIPHERB + CTO ANALYSIS")
        print("="*80)
        print(f"🕐 Analysis Time: {ist_current.strftime('%Y-%m-%d %H:%M:%S IST')}")
        print(f"⏰ Timeframe: 15-minute candles")
        print(f"✅ CipherB + CTO confirmation (±70)")
        print(f"🔄 4-hour cooldown deduplication")
        print(f"🔍 Analyzing {len(self.market_data)} top coins")
        
        if not self.market_data:
            print("❌ No market data available")
            return
        
        # Clean up old records
        self.deduplicator.cleanup_old_signals()
        
        # Analyze coins in batches
        confirmed_signals = []
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
                    confirmed_signals.append(signal_result)
                    age_s = signal_result['signal_age_seconds']
                    cto_score = signal_result['cto_score']
                    print(f"🚨 {signal_result['signal_type']}: {signal_result['symbol']} "
                          f"(CTO: {cto_score:.1f}, {age_s:.0f}s ago)")
                
                total_analyzed += 1
                time.sleep(self.config['exchanges']['rate_limit'])
        
        # Send alerts
        if confirmed_signals:
            success = send_15m_alert(confirmed_signals)
            if success:
                avg_age = sum(s['signal_age_seconds'] for s in confirmed_signals) / len(confirmed_signals)
                print(f"\n✅ SENT 15M CONFIRMED SIGNAL ALERT")
                print(f"   Confirmed signals: {len(confirmed_signals)}")
                print(f"   Average age: {avg_age:.0f} seconds")
            else:
                print(f"\n❌ Failed to send confirmed signal alert")
        else:
            print(f"\n📊 No confirmed signals detected")
        
        print(f"\n" + "="*80)
        print("🎯 15M CIPHERB + CTO ANALYSIS COMPLETE")
        print("="*80)
        print(f"📊 Total analyzed: {total_analyzed}")
        print(f"🚨 Confirmed signals: {len(confirmed_signals)}")
        print(f"📱 Alert sent: {'Yes' if confirmed_signals else 'No'}")
        print(f"⏰ Next analysis: 15 minutes")
        print("="*80)

if __name__ == '__main__':
    analyzer = Analyzer15mCTO()
    analyzer.run_analysis()
