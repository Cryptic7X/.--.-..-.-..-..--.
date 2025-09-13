#!/usr/bin/env python3
"""
Multi-Timeframe 6H CipherB Analysis System
- Pure CipherB signals (no confirmation needed)
- Advanced suppression logic with timeframe cascading
- Market cap ≥ $50M, Volume ≥ $20M filtering
- Intelligent deduplication across 6h/8h/12h timeframes
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
from alerts.telegram_multi import send_multi_consolidated_alert, send_admin_alert
from alerts.deduplication import MultiTimeframeSuppressionManager
from utils.exchange_handler import MultiExchangeHandler
from utils.timezone_utils import get_ist_time, is_analysis_time_valid
from data.coinmarketcap_fetcher import load_market_data

class Multi6HAnalyzer:
    def __init__(self):
        self.config = self.load_config()
        self.timeframe = '6h'
        self.exchange_handler = MultiExchangeHandler(self.config['exchanges'])
        self.suppression_manager = MultiTimeframeSuppressionManager(
            state_file=self.config['suppression']['state_file'],
            cascade_order=self.config['suppression']['cascade_order']
        )
        self.blocked_coins = self.load_blocked_coins()
        
    def load_config(self):
        config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'config-multi.yaml')
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
            print(f"🚫 Loaded {len(blocked_coins)} blocked coins for 6h analysis")
        except FileNotFoundError:
            print("📝 No blocked_coins.txt found - analyzing all coins")
        except Exception as e:
            print(f"⚠️ Error loading blocked coins: {e}")
        
        return blocked_coins
    
    def analyze_coin_for_signals(self, coin_data):
        """
        Analyze single coin for pure CipherB signals on 6h timeframe
        Returns signal data if conditions met and not suppressed
        """
        symbol = coin_data.get('symbol', '').upper()
        
        # Skip blocked coins
        if symbol in self.blocked_coins:
            return None
            
        try:
            # Fetch 6-hour OHLCV data
            df, exchange_used = self.exchange_handler.fetch_ohlcv(
                symbol=symbol,
                timeframe='6h', 
                limit=200
            )
            
            if df is None or len(df) < self.config['market_data']['min_candles_required']:
                return None
            
            # Calculate pure CipherB signals
            cipherb_signals = detect_exact_cipherb_signals(df, self.config['cipherb'])
            
            if cipherb_signals.empty:
                return None
            
            # Get the most recent signal
            latest_signal = cipherb_signals.iloc[-1]
            signal_timestamp = cipherb_signals.index[-1]
            
            # Check for valid signals
            if latest_signal['buySignal']:
                signal_type = 'BUY'
            elif latest_signal['sellSignal']:
                signal_type = 'SELL'
            else:
                return None
            
            # Apply advanced suppression logic
            suppression_result = self.suppression_manager.should_allow_signal(
                symbol=symbol,
                signal_type=signal_type,
                timeframe='6h',
                timestamp=signal_timestamp
            )
            
            if not suppression_result['allowed']:
                print(f"🔕 {symbol} {signal_type} suppressed: {suppression_result['reason']}")
                return None
            
            # Signal is allowed - create signal data
            return {
                'symbol': symbol,
                'signal_type': signal_type,
                'timeframe': '6h',
                'wt1': latest_signal['wt1'],
                'wt2': latest_signal['wt2'],
                'price': coin_data.get('current_price', 0),
                'change_24h': coin_data.get('price_change_percentage_24h', 0),
                'market_cap': coin_data.get('market_cap', 0),
                'volume_24h': coin_data.get('total_volume', 0),
                'exchange': exchange_used,
                'timestamp': signal_timestamp,
                'suppression_action': suppression_result['action'],
                'coin_data': coin_data
            }
                
        except Exception as e:
            print(f"❌ Error analyzing {symbol}: {str(e)[:100]}")
            
        return None
    
    def run_analysis(self):
        """
        Run complete 6-hour multi-timeframe analysis
        """
        start_time = time.time()
        ist_time = get_ist_time()
        
        print("="*80)
        print("📈 MULTI-TIMEFRAME 6H CIPHERB ANALYSIS")
        print("="*80)
        print(f"🕐 Analysis Time: {ist_time.strftime('%Y-%m-%d %H:%M:%S IST')}")
        print(f"⏰ Timeframe: 6-hour candles")
        print(f"🎯 Signal Type: Pure CipherB (no confirmation)")
        print(f"🔄 Suppression: Advanced multi-timeframe logic")
        print(f"🎯 GitHub Actions Run: #{os.getenv('GITHUB_RUN_NUMBER', 'local')}")
        
        # Validate analysis timing
        timing_valid, timing_reason = is_analysis_time_valid('6h', ist_time)
        if not timing_valid:
            print(f"⏸️ TIMING CHECK: {timing_reason}")
            send_admin_alert("6h Analysis Timing Issue", f"Reason: {timing_reason}\nTime: {ist_time}")
        
        try:
            # Load market data for multi-timeframe system
            market_data = load_market_data('multi')
            
            if not market_data:
                error_msg = "❌ No market data available for 6h analysis"
                print(error_msg)
                send_admin_alert("6h Analysis Failed", error_msg)
                return
            
            # Filter out blocked coins
            filtered_coins = [
                coin for coin in market_data 
                if coin.get('symbol', '').upper() not in self.blocked_coins
            ]
            
            print(f"📊 Market Data: {len(market_data)} total coins")
            print(f"🚫 Blocked: {len(market_data) - len(filtered_coins)} coins")
            print(f"🔍 Analyzing: {len(filtered_coins)} coins")
            
            # Clean up old suppression states
            self.suppression_manager.cleanup_old_states()
            
            # Analyze coins in batches
            valid_signals = []
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
                        valid_signals.append(signal_result)
                        print(f"✅ 6H {signal_result['signal_type']}: {signal_result['symbol']} "
                              f"({signal_result['suppression_action']})")
                        
                        # Update suppression state for this signal
                        self.suppression_manager.record_signal(
                            symbol=signal_result['symbol'],
                            signal_type=signal_result['signal_type'],
                            timeframe='6h',
                            timestamp=signal_result['timestamp']
                        )
                    
                    total_analyzed += 1
                    time.sleep(self.config['processing']['rate_limit_delay'])
            
            # Send consolidated alert
            if valid_signals:
                success = send_multi_consolidated_alert(valid_signals, timeframe='6h')
                if success:
                    print(f"\n📱 6H ALERT SENT: {len(valid_signals)} signals")
                else:
                    print(f"\n❌ 6H Alert delivery failed")
                    send_admin_alert("6h Alert Failed", f"Failed to send {len(valid_signals)} signals")
            else:
                print(f"\n📊 No valid 6h CipherB signals detected")
            
            # Save analysis cache
            self.save_analysis_cache(valid_signals, total_analyzed, start_time)
            
            # Final summary
            elapsed = time.time() - start_time
            print(f"\n" + "="*80)
            print("📈 6-HOUR ANALYSIS COMPLETE")
            print("="*80)
            print(f"📊 Total Analyzed: {total_analyzed}")
            print(f"✅ Valid Signals: {len(valid_signals)}")
            print(f"⏱️ Execution Time: {elapsed:.1f} seconds")
            print(f"📱 Alert Sent: {'Yes' if valid_signals else 'No'}")
            print(f"⏰ Next Analysis: 6 hours")
            print("="*80)
            
        except Exception as e:
            error_msg = f"Critical error in 6h analysis: {str(e)}"
            print(f"💥 {error_msg}")
            print(f"📋 Traceback: {traceback.format_exc()}")
            send_admin_alert("6h Analysis Critical Error", f"{error_msg}\n\n{traceback.format_exc()}")
    
    def save_analysis_cache(self, signals, total_analyzed, start_time):
        """Save analysis results to cache"""
        try:
            cache_data = {
                'timestamp': datetime.utcnow().isoformat(),
                'timeframe': '6h',
                'analysis_duration': time.time() - start_time,
                'total_analyzed': total_analyzed,
                'signals_found': len(signals),
                'signals': [
                    {
                        'symbol': s['symbol'],
                        'type': s['signal_type'],
                        'timeframe': s['timeframe'],
                        'wt1': s['wt1'],
                        'wt2': s['wt2'],
                        'price': s['price'],
                        'suppression_action': s['suppression_action']
                    } for s in signals
                ],
                'system': 'multi-timeframe-6h',
                'version': self.config['system']['version']
            }
            
            cache_file = self.config['cache']['analysis_cache_6h']
            os.makedirs(os.path.dirname(cache_file), exist_ok=True)
            
            with open(cache_file, 'w') as f:
                json.dump(cache_data, f, indent=2)
                
        except Exception as e:
            print(f"⚠️ Failed to save 6h analysis cache: {e}")

if __name__ == '__main__':
    analyzer = Multi6HAnalyzer()
    analyzer.run_analysis()
