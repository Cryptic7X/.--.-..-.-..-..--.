#!/usr/bin/env python3
"""
15-Minute CipherB + CTO Confirmation Analyzer
OPTIMIZED for speed - prevents GitHub Actions timeout
"""

import os
import sys
import json
import ccxt
import pandas as pd
import yaml
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(__file__))
from indicators.cipherb_exact import detect_exact_cipherb_signals
from indicators.composite_trend_oscillator import CompositeTrendOscillator
from alerts.telegram_15m import send_15m_alert
from alerts.deduplication_15m import Deduplicator15m
from utils.freshness import is_signal_fresh, get_signal_age_display

def get_ist_time():
    """Convert UTC to IST"""
    utc_now = datetime.utcnow()
    return utc_now + timedelta(hours=5, minutes=30)

class Analyzer15mCTO:
    def __init__(self):
        self.config = self.load_config()
        self.timeframe = '15m'
        
        # Initialize CTO
        cto_config = self.config['cto']
        self.cto = CompositeTrendOscillator(
            spacing=cto_config.get('spacing', 3),
            signal_length=cto_config.get('signal_length', 20),
            filter_type=cto_config.get('filter_type', 'PhiSmoother'),
            post_smooth_length=cto_config.get('post_smooth_length', 1),
            upper_trim=cto_config.get('upper_trim', 0),
            lower_trim=cto_config.get('lower_trim', 0),
            phase=cto_config.get('phase', 3.7)
        )
        
        self.cto_overbought_threshold = cto_config.get('overbought_threshold', 70)
        self.cto_oversold_threshold = cto_config.get('oversold_threshold', -70)
        self.deduplicator = Deduplicator15m()
        self.exchanges = self.init_exchanges()
        self.market_data = self.load_market_data()
    
    def load_config(self):
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config_15m.yaml')
        with open(config_path) as f:
            return yaml.safe_load(f)
    
    def load_market_data(self):
        """Load and filter top coins by volume for speed"""
        cache_file = os.path.join(os.path.dirname(__file__), '..', 'cache', 'market_data_15m.json')
        
        if not os.path.exists(cache_file):
            print("❌ Market data cache not found")
            return []
        
        with open(cache_file) as f:
            data = json.load(f)
        
        coins = data.get('coins', [])
        
        # ✅ OPTIMIZATION: Filter to high-volume coins only (faster processing)
        high_volume_coins = [
            c for c in coins 
            if c.get('volume_24h', 0) > 5_000_000  # Min $5M 24h volume
        ]
        
        # ✅ OPTIMIZATION: Limit to top 50 coins (prevents timeout)
        top_coins = high_volume_coins[:50]
        
        print(f"📊 Loaded {len(top_coins)} high-volume coins for 15m analysis")
        return top_coins
    
    def init_exchanges(self):
        exchanges = []
        
        # Primary: BingX
        try:
            bingx = ccxt.bingx({
                'apiKey': os.getenv('BINGX_API_KEY', ''),
                'secret': os.getenv('BINGX_SECRET_KEY', ''),
                'rateLimit': 100,  # ✅ OPTIMIZATION: Faster rate limit
                'enableRateLimit': True,
                'timeout': 15000,  # ✅ OPTIMIZATION: Shorter timeout
            })
            bingx.load_markets()
            exchanges.append(('BingX', bingx))
        except Exception as e:
            print(f"⚠️ BingX init failed: {e}")
        
        # Fallback: KuCoin  
        try:
            kucoin = ccxt.kucoin({
                'rateLimit': 200,  # ✅ OPTIMIZATION: Faster rate limit
                'enableRateLimit': True,
                'timeout': 15000,  # ✅ OPTIMIZATION: Shorter timeout
            })
            kucoin.load_markets()
            exchanges.append(('KuCoin', kucoin))
        except Exception as e:
            print(f"⚠️ KuCoin init failed: {e}")
        
        # ✅ OPTIMIZATION: Simplified symbol mapping
        self.symbol_map = {}
        for exchange_name, exchange in exchanges:
            for market_symbol in exchange.symbols:
                if '/USDT' in market_symbol:
                    base = market_symbol.replace('/USDT', '')
                    self.symbol_map[base.upper()] = (exchange_name, market_symbol)
                elif market_symbol.endswith('USDT') and len(market_symbol) > 4:
                    base = market_symbol[:-4]
                    self.symbol_map[base.upper()] = (exchange_name, market_symbol)
        
        print(f"✅ Symbol map built: {len(self.symbol_map)} pairs")
        return exchanges
    
    def fetch_ohlcv_data(self, symbol, timeframe='15m'):
        """Fast OHLCV fetch with minimal error handling"""
        if symbol.upper() not in self.symbol_map:
            return None, None
        
        exchange_name, market_symbol = self.symbol_map[symbol.upper()]
        
        # Get exchange
        exchange = None
        for ex_name, ex in self.exchanges:
            if ex_name == exchange_name:
                exchange = ex
                break
        
        if not exchange:
            return None, None
        
        try:
            # ✅ OPTIMIZATION: Smaller candle limit (faster fetch)
            ohlcv = exchange.fetch_ohlcv(market_symbol, timeframe, limit=100)
            
            if len(ohlcv) < 30:  # ✅ OPTIMIZATION: Lower minimum requirement
                return None, None
            
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df.set_index('timestamp', inplace=True)
            df['utc_timestamp'] = df.index
            df.index = df.index + pd.Timedelta(hours=5, minutes=30)
            
            if len(df) > 30 and df['close'].iloc[-1] > 0:
                return df, exchange_name
            
        except Exception:
            # ✅ OPTIMIZATION: Silent fail (no logging delays)
            return None, None
        
        return None, None
    
    def analyze_coin(self, coin_data):
        """Fast coin analysis with minimal checks"""
        symbol = coin_data['symbol']
        
        try:
            # Fetch data
            df, exchange_used = self.fetch_ohlcv_data(symbol, '15m')
            if df is None or len(df) < 30:
                return None
            
            # Freshness check
            signal_timestamp_utc = df['utc_timestamp'].iloc[-1]
            if not is_signal_fresh(signal_timestamp_utc, self.timeframe):
                return None
            
            # CipherB signals
            cipherb_signals = detect_exact_cipherb_signals(df, self.config['cipherb'])
            if cipherb_signals.empty:
                return None
            
            # CTO analysis
            cto_df = self.cto.calculate_signals(df)
            cto_final = self.cto.detect_cto_conditions(
                cto_df, 
                self.cto_overbought_threshold,
                self.cto_oversold_threshold
            )
            
            # Get latest signals
            latest_cipherb = cipherb_signals.iloc[-1]
            latest_cto = cto_final.iloc[-1]
            signal_timestamp_ist = cipherb_signals.index[-1]
            
            current_time = datetime.utcnow()
            time_since_signal = current_time - signal_timestamp_utc.to_pydatetime()
            
            # Check BUY signal
            if (latest_cipherb['buySignal'] and 
                latest_cto['cto_oversold'] and 
                self.deduplicator.is_signal_allowed(symbol, 'BUY', signal_timestamp_utc)):
                
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
            
            # Check SELL signal
            if (latest_cipherb['sellSignal'] and 
                latest_cto['cto_overbought'] and 
                self.deduplicator.is_signal_allowed(symbol, 'SELL', signal_timestamp_utc)):
                
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
            
        except Exception:
            # ✅ OPTIMIZATION: Silent fail (no delays)
            return None
    
    def run_analysis(self):
        """Optimized 15m analysis - completes under 5 minutes"""
        ist_current = get_ist_time()
        
        print("="*60)
        print("🎯 OPTIMIZED 15M CIPHERB + CTO ANALYSIS")
        print("="*60)
        print(f"🕐 Start: {ist_current.strftime('%H:%M:%S IST')}")
        print(f"📊 Analyzing {len(self.market_data)} high-volume coins")
        print(f"⚡ Speed optimized for GitHub Actions")
        
        if not self.market_data:
            print("❌ No market data available")
            return
        
        # Clean deduplication cache
        self.deduplicator.cleanup_old_signals()
        
        # ✅ OPTIMIZATION: Process all coins without batching delays
        confirmed_signals = []
        processed = 0
        
        for coin in self.market_data:
            signal_result = self.analyze_coin(coin)
            if signal_result:
                confirmed_signals.append(signal_result)
                cto_score = signal_result['cto_score']
                age_s = signal_result['signal_age_seconds']
                print(f"🚨 {signal_result['signal_type']}: {signal_result['symbol']} "
                      f"(CTO: {cto_score:.1f}, {age_s:.0f}s ago)")
            
            processed += 1
            # ✅ OPTIMIZATION: No sleep delays between coins
        
        # Send alerts
        if confirmed_signals:
            success = send_15m_alert(confirmed_signals)
            if success:
                avg_age = sum(s['signal_age_seconds'] for s in confirmed_signals) / len(confirmed_signals)
                print(f"\n✅ ALERT SENT: {len(confirmed_signals)} confirmed signals")
                print(f"   Average signal age: {avg_age:.0f} seconds")
            else:
                print(f"\n❌ Failed to send alert")
        else:
            print(f"\n📊 No confirmed signals detected")
        
        end_time = get_ist_time()
        duration = (end_time - ist_current).total_seconds()
        
        print(f"\n" + "="*60)
        print("✅ 15M ANALYSIS COMPLETE")
        print("="*60)
        print(f"📊 Processed: {processed} coins")
        print(f"🚨 Signals: {len(confirmed_signals)}")
        print(f"⏱️ Duration: {duration:.1f} seconds")
        print(f"📱 Alert: {'Sent' if confirmed_signals else 'None'}")
        print("="*60)

if __name__ == '__main__':
    analyzer = Analyzer15mCTO()
    analyzer.run_analysis()
