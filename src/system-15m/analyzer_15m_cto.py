#!/usr/bin/env python3
"""
Enhanced 15-Minute CipherB + CTO Confirmation System
- Your validated CipherB indicator (100% accuracy)
- Full Composite Trend Oscillator implementation  
- 4-hour cooldown with immediate opposite signals
- Top 100 coins from CoinMarketCap
- Enhanced GitHub Actions timing with multiple backup schedules
"""

import os
import sys
import time
import json
import yaml
from datetime import datetime, timedelta
import traceback

# Add shared modules to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'shared'))

from indicators.cipherb_exact import detect_exact_cipherb_signals
from indicators.composite_trend_oscillator import CompositeTrendOscillator
from alerts.telegram_15m import send_15m_consolidated_alert, send_admin_alert
from alerts.deduplication import SignalDeduplicator
from utils.exchange_handler import MultiExchangeHandler
from utils.timezone_utils import get_ist_time, is_analysis_time_valid
from data.coinmarketcap_fetcher import load_market_data

class CipherBCTOAnalyzer:
    def __init__(self):
        self.config = self.load_config()
        self.exchange_handler = MultiExchangeHandler(self.config['exchanges'])
        self.deduplicator = SignalDeduplicator(
            cache_file=self.config['cache']['deduplication_file'],
            cooldown_hours=self.config['signals']['cooldown_hours']
        )
        self.cto = CompositeTrendOscillator(
            spacing=self.config['cto']['spacing'],
            signal_length=self.config['cto']['signal_length'],
            filter_type=self.config['cto']['filter_type'],
            phase=self.config['cto']['phase'],
            oversold_threshold=self.config['cto']['oversold_threshold'],
            overbought_threshold=self.config['cto']['overbought_threshold']
        )
        self.blocked_coins = self.load_blocked_coins()
        
    def load_config(self):
        config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'config-15m.yaml')
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def load_blocked_coins(self):
        blocked_file = os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'blocked_coins.txt')
        blocked_coins = set()
        
        try:
            with open(blocked_file, 'r') as f:
                for line in f:
                    coin = line.strip().upper()
                    if coin and not coin.startswith('#'):
                        blocked_coins.add(coin)
            print(f"🚫 Loaded {len(blocked_coins)} blocked coins for 15m system")
        except FileNotFoundError:
            print("📝 No blocked_coins.txt found - analyzing all coins")
        except Exception as e:
            print(f"⚠️ Error loading blocked coins: {e}")
        
        return blocked_coins
    
    def analyze_coin_for_signals(self, coin_data):
        """
        Analyze single coin for CipherB + CTO confirmed signals
        Returns signal data if all conditions met
        """
        symbol = coin_data.get('symbol', '').upper()
        
        # Skip blocked coins
        if symbol in self.blocked_coins:
            return None
            
        try:
            # Fetch 15-minute OHLCV data
            df, exchange_used = self.exchange_handler.fetch_ohlcv(
                symbol=symbol, 
                timeframe='15m', 
                limit=200
            )
            
            if df is None or len(df) < self.config['market_data']['min_candles_required']:
                return None
            
            # Calculate CipherB signals using your validated implementation
            cipherb_signals = detect_exact_cipherb_signals(df, self.config['cipherb'])
            
            if cipherb_signals.empty:
                return None
            
            # Get the most recent signal
            latest_signal = cipherb_signals.iloc[-1]
            signal_timestamp = cipherb_signals.index[-1]
            
            # Skip if no CipherB signal
            if not (latest_signal['buySignal'] or latest_signal['sellSignal']):
                return None
            
            # Calculate CTO for confirmation
            cto_df = self.cto.calculate_cto_signals(df)
            latest_cto = cto_df.iloc[-1]
            cto_score = latest_cto['cto_score']
            
            # Apply confirmation logic
            confirmed_signal = self.apply_confirmation_logic(
                latest_signal, cto_score, symbol, signal_timestamp
            )
            
            if confirmed_signal:
                return {
                    'symbol': symbol,
                    'signal_type': confirmed_signal['type'],
                    'cipherb_wt1': latest_signal['wt1'],
                    'cipherb_wt2': latest_signal['wt2'], 
                    'cto_score': cto_score,
                    'price': coin_data.get('current_price', 0),
                    'change_24h': coin_data.get('price_change_percentage_24h', 0),
                    'market_cap': coin_data.get('market_cap', 0),
                    'volume_24h': coin_data.get('total_volume', 0),
                    'exchange': exchange_used,
                    'timestamp': signal_timestamp,
                    'confirmation': 'CTO_CONFIRMED',
                    'coin_data': coin_data
                }
                
        except Exception as e:
            print(f"❌ Error analyzing {symbol}: {str(e)[:100]}")
            
        return None
    
    def apply_confirmation_logic(self, cipherb_signal, cto_score, symbol, timestamp):
        """
        Apply CipherB + CTO confirmation logic with deduplication
        """
        # Check CipherB BUY + CTO confirmation
        if cipherb_signal['buySignal']:
            # CTO must be in oversold zone for BUY confirmation
            if cto_score <= self.config['cto']['oversold_threshold']:
                # Check deduplication (4-hour cooldown for same direction)
                if self.deduplicator.is_signal_allowed(symbol, 'BUY', timestamp):
                    return {'type': 'BUY', 'confirmed': True}
                else:
                    print(f"🔄 {symbol} BUY signal blocked by 4h cooldown")
                    
        # Check CipherB SELL + CTO confirmation  
        if cipherb_signal['sellSignal']:
            # CTO must be in overbought zone for SELL confirmation
            if cto_score >= self.config['cto']['overbought_threshold']:
                # Check deduplication (4-hour cooldown for same direction)
                if self.deduplicator.is_signal_allowed(symbol, 'SELL', timestamp):
                    return {'type': 'SELL', 'confirmed': True}
                else:
                    print(f"🔄 {symbol} SELL signal blocked by 4h cooldown")
        
        return None
    
    def run_analysis(self):
        """
        Run complete 15-minute CipherB + CTO analysis
        Enhanced with timing validation and error handling
        """
        start_time = time.time()
        ist_time = get_ist_time()
        
        print("="*80)
        print("🎯 ENHANCED 15-MINUTE CIPHERB + CTO ANALYSIS")
        print("="*80)
        print(f"🕐 Analysis Time: {ist_time.strftime('%Y-%m-%d %H:%M:%S IST')}")
        print(f"⚰️ Timeframe: 15-minute candles")
        print(f"🔧 Confirmation: CTO (±70 thresholds)")
        print(f"⏰ Cooldown: 4-hour same direction")
        print(f"🎯 GitHub Actions Run: #{os.getenv('GITHUB_RUN_NUMBER', 'local')}")
        
        # Validate analysis timing
        timing_valid, timing_reason = is_analysis_time_valid('15m', ist_time)
        if not timing_valid:
            print(f"⏸️ TIMING CHECK: {timing_reason}")
            # Continue anyway for backup schedules, but log the timing issue
            send_admin_alert(f"15m Analysis Timing Issue", f"Reason: {timing_reason}\nTime: {ist_time}")
        
        try:
            # Load market data for top 100 coins
            market_data = load_market_data('15m')
            
            if not market_data:
                error_msg = "❌ No market data available for 15m analysis"
                print(error_msg)
                send_admin_alert("15m Analysis Failed", error_msg)
                return
            
            # Filter out blocked coins
            filtered_coins = [
                coin for coin in market_data 
                if coin.get('symbol', '').upper() not in self.blocked_coins
            ]
            
            print(f"📊 Market Data: {len(market_data)} total coins")
            print(f"🚫 Blocked: {len(market_data) - len(filtered_coins)} coins")
            print(f"🔍 Analyzing: {len(filtered_coins)} coins")
            
            # Clean up old deduplication records
            self.deduplicator.cleanup_old_records()
            
            # Analyze coins in batches
            confirmed_signals = []
            batch_size = self.config['processing']['batch_size']
            total_analyzed = 0
            
            for i in range(0, len(filtered_coins), batch_size):
                batch = filtered_coins[i:i + batch_size]
                batch_num = i // batch_size + 1
                total_batches = (len(filtered_coins) - 1) // batch_size + 1
                
                print(f"\n🔄 Processing batch {batch_num}/{total_batches}")
                
                for coin in batch:
                    signal_result = self.analyze_coin_for_signals(coin)
                    if signal_result:
                        confirmed_signals.append(signal_result)
                        print(f"✅ {signal_result['signal_type']}: {signal_result['symbol']} "
                              f"(CTO: {signal_result['cto_score']:.1f})")
                    
                    total_analyzed += 1
                    time.sleep(self.config['processing']['rate_limit_delay'])
            
            # Send consolidated alert
            if confirmed_signals:
                success = send_15m_consolidated_alert(confirmed_signals)
                if success:
                    print(f"\n📱 ALERT SENT: {len(confirmed_signals)} confirmed signals")
                else:
                    print(f"\n❌ Alert delivery failed")
                    send_admin_alert("15m Alert Failed", f"Failed to send {len(confirmed_signals)} signals")
            else:
                print(f"\n📊 No confirmed CipherB + CTO signals detected")
            
            # Save analysis cache
            self.save_analysis_cache(confirmed_signals, total_analyzed, start_time)
            
            # Final summary
            elapsed = time.time() - start_time
            print(f"\n" + "="*80)
            print("🎯 15-MINUTE ANALYSIS COMPLETE")
            print("="*80)
            print(f"📊 Total Analyzed: {total_analyzed}")
            print(f"✅ Confirmed Signals: {len(confirmed_signals)}")
            print(f"⏱️ Execution Time: {elapsed:.1f} seconds")
            print(f"📱 Alert Sent: {'Yes' if confirmed_signals else 'No'}")
            print(f"⏰ Next Analysis: 15 minutes")
            print("="*80)
            
        except Exception as e:
            error_msg = f"Critical error in 15m analysis: {str(e)}"
            print(f"💥 {error_msg}")
            print(f"📋 Traceback: {traceback.format_exc()}")
            send_admin_alert("15m Analysis Critical Error", f"{error_msg}\n\n{traceback.format_exc()}")
    
    def save_analysis_cache(self, signals, total_analyzed, start_time):
        """Save analysis results to cache"""
        try:
            cache_data = {
                'timestamp': datetime.utcnow().isoformat(),
                'analysis_duration': time.time() - start_time,
                'total_analyzed': total_analyzed,
                'signals_found': len(signals),
                'signals': [
                    {
                        'symbol': s['symbol'],
                        'type': s['signal_type'],
                        'cto_score': s['cto_score'],
                        'price': s['price']
                    } for s in signals
                ],
                'system': 'cipherb-cto-15m',
                'version': self.config['system']['version']
            }
            
            cache_file = self.config['cache']['analysis_cache']
            os.makedirs(os.path.dirname(cache_file), exist_ok=True)
            
            with open(cache_file, 'w') as f:
                json.dump(cache_data, f, indent=2)
                
        except Exception as e:
            print(f"⚠️ Failed to save analysis cache: {e}")

if __name__ == '__main__':
    analyzer = CipherBCTOAnalyzer()
    analyzer.run_analysis()
