#!/usr/bin/env python3
"""
CipherB 15M Analyzer - 100% Pine Script Match
Direction-based alert tracking (one per direction until opposite occurs)
"""

import os
import sys
import concurrent.futures
import yaml
from datetime import datetime

# Add src directory to path
sys.path.append(os.path.join(os.path.dirname(__file__)))

from exchange_manager import SimpleExchangeManager
from cipher_indicator import CipherB15MIndicator
from cipher_telegram import CipherBTelegram

class CipherB15MAnalyzer:
    def __init__(self):
        self.config = self.load_config()
        self.exchange_manager = SimpleExchangeManager()
        self.cipher_indicator = CipherB15MIndicator()
        self.telegram_sender = CipherBTelegram()
        
    def load_config(self) -> dict:
        """Load system configuration"""
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'cipher_config.yaml')
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            print(f"❌ Config load error: {e}")
            return {
                'cipher_b': {
                    'timeframe': '15m',
                    'freshness_minutes': 15,
                    'data_limit': 200
                },
                'system': {'max_workers': 10}
            }
    
    def load_coins(self):
        """Load coins from coins.txt file"""
        coin_file = os.path.join(os.path.dirname(__file__), '..', 'config', 'coins.txt')
        try:
            with open(coin_file, 'r') as f:
                coins = [line.strip().upper() for line in f if line.strip() and not line.startswith('#')]
            
            # Add USDT suffix if not present
            formatted_coins = []
            for coin in coins:
                if not coin.endswith('USDT'):
                    formatted_coins.append(coin + 'USDT')
                else:
                    formatted_coins.append(coin)
            
            print(f"📊 Loaded {len(formatted_coins)} coins for CipherB 15M analysis")
            return formatted_coins
        except FileNotFoundError:
            print("❌ config/coins.txt not found!")
            return []
        except Exception as e:
            print(f"❌ Error loading coins: {e}")
            return []
    
    def analyze_coin(self, symbol):
        """Analyze single coin for CipherB signals"""
        try:
            timeframe = self.config['cipher_b']['timeframe']
            data_limit = self.config['cipher_b']['data_limit']
            
            # Fetch 15M OHLCV data
            ohlcv_data, exchange_used = self.exchange_manager.fetch_ohlcv_with_fallback(
                symbol, timeframe, limit=data_limit
            )
            
            if not ohlcv_data:
                print(f"❌ No data fetched for {symbol}")
                return None
            
            # Analyze for CipherB signals (Pine Script match + direction tracking)
            result = self.cipher_indicator.analyze(ohlcv_data, symbol)
            
            if not result.get('signal_alert', False):
                return None
            
            return {
                'symbol': symbol,
                'signal_alert': result.get('signal_alert', False),
                'signal_type': result.get('signal_type'),
                'current_price': result.get('current_price'),
                'wt1_value': result.get('wt1_value'),
                'wt2_value': result.get('wt2_value'),
                'exchange_used': exchange_used
            }
            
        except Exception as e:
            print(f"❌ Error analyzing {symbol}: {e}")
            return None
    
    def run_analysis(self):
        """Run complete CipherB 15M analysis"""
        print("🟡 CIPHER B 15M ANALYSIS SYSTEM")
        print(f"⏰ Time: {datetime.now().strftime('%H:%M:%S IST')}")
        print("🎯 Pine Script Signals: Buy/Sell plot shapes only")
        print("🔄 Direction Logic: One alert per direction until opposite")
        
        coins = self.load_coins()
        if not coins:
            print("❌ No coins to analyze")
            return
        
        signals = []
        max_workers = self.config['system']['max_workers']
        
        # Analyze coins concurrently
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(self.analyze_coin, coin): coin for coin in coins}
            
            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result(timeout=30)
                    if result:
                        signals.append(result)
                        print(f"✅ CIPHER B: {result['symbol']} - {result['signal_type'].upper()} SIGNAL")
                        print(f"   💰 ${result['current_price']:.4f} via {result['exchange_used']}")
                except Exception as e:
                    coin = futures[future]
                    print(f"❌ Analysis timeout/error for {coin}: {e}")
                    continue
        
        # Send alerts if any signals found
        if signals:
            success = self.telegram_sender.send_alerts(signals, timeframe_minutes=15)
            
            signal_count = len(signals)
            buy_count = len([s for s in signals if s.get('signal_type') == 'buy'])
            sell_count = len([s for s in signals if s.get('signal_type') == 'sell'])
            
            print(f"📱 Results: {signal_count} signals ({buy_count} buy, {sell_count} sell)")
            print(f"📤 Telegram: {'✅ Sent' if success else '❌ Failed'}")
        else:
            print("📭 No CipherB signals found")
        
        # Display cache status
        cache = self.cipher_indicator.load_cache()
        print(f"📁 Direction Cache: {len(cache)} tracked symbols")
        
        # Show cache status for debugging
        if cache:
            print("🔍 Cache Status:")
            for symbol, data in list(cache.items())[:5]:  # Show first 5
                last_signal = data.get('last_signal', 'none')
                print(f"   {symbol}: last_{last_signal}")

if __name__ == "__main__":
    analyzer = CipherB15MAnalyzer()
    analyzer.run_analysis()
