#!/usr/bin/env python3
"""
Dynamic EMA Analyzer - Config-Aware Analysis System
Automatically switches between 12/21 and 21/50 EMA based on YAML config
"""

import os
import sys
import concurrent.futures
import yaml
from datetime import datetime

# Add src directory to path
sys.path.append(os.path.join(os.path.dirname(__file__)))

from exchange_manager import SimpleExchangeManager
from ema_indicator import DynamicEMAIndicator
from ema_telegram import DynamicEMATelegram

class DynamicEMAAnalyzer:
    def __init__(self):
        self.config = self.load_config()
        self.exchange_manager = SimpleExchangeManager()
        self.ema_indicator = DynamicEMAIndicator()
        self.telegram_sender = DynamicEMATelegram()

    def load_config(self) -> dict:
        """Load system configuration"""
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'ema_config.yaml')
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            print(f"❌ Config load error: {e}")
            # Return default config
            return {
                'system': {
                    'timeframe': '15m',
                    'freshness_minutes': 30,
                    'data_limit': 200,
                    'max_workers': 10
                }
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

            print(f"📊 Loaded {len(formatted_coins)} coins for dynamic EMA analysis")
            return formatted_coins
        except FileNotFoundError:
            print("❌ config/coins.txt not found!")
            return []
        except Exception as e:
            print(f"❌ Error loading coins: {e}")
            return []

    def analyze_coin(self, symbol):
        """Analyze single coin for EMA crossover"""
        try:
            timeframe = self.config.get('system', {}).get('timeframe', '15m')
            data_limit = self.config.get('system', {}).get('data_limit', 200)

            # Fetch OHLCV data
            ohlcv_data, exchange_used = self.exchange_manager.fetch_ohlcv_with_fallback(
                symbol, timeframe, limit=data_limit
            )

            if not ohlcv_data:
                print(f"❌ No data fetched for {symbol}")
                return None

            # Analyze for crossover (indicator auto-detects active analysis)
            result = self.ema_indicator.analyze(ohlcv_data, symbol)

            if not result.get('crossover_alert', False):
                return None

            return {
                'symbol': symbol,
                'crossover_alert': result.get('crossover_alert', False),
                'crossover_type': result.get('crossover_type'),
                'current_price': result.get('current_price'),
                'ema_short_period': result.get('ema_short_period'),
                'ema_long_period': result.get('ema_long_period'),
                'ema_short_value': result.get('ema_short_value'),
                'ema_long_value': result.get('ema_long_value'),
                'exchange_used': exchange_used
            }

        except Exception as e:
            print(f"❌ Error analyzing {symbol}: {e}")
            return None

    def run_analysis(self):
        """Run complete dynamic EMA analysis"""
        active_analysis = self.ema_indicator.get_active_analysis()

        print("🟡 DYNAMIC EMA ANALYSIS SYSTEM")
        print(f"⏰ Time: {datetime.now().strftime('%H:%M:%S IST')}")
        print(f"🎯 Active: {active_analysis['ema_short']}/{active_analysis['ema_long']} EMA")
        print(f"🔄 Cooldown: {active_analysis['cooldown_hours']} hours")

        coins = self.load_coins()
        if not coins:
            print("❌ No coins to analyze")
            return

        signals = []
        max_workers = self.config.get('system', {}).get('max_workers', 10)

        # Analyze coins concurrently
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(self.analyze_coin, coin): coin for coin in coins}

            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result(timeout=30)
                    if result:
                        signals.append(result)
                        print(f"✅ CROSSOVER: {result['symbol']} - {result['crossover_type'].upper()}")
                        print(f"   💰 ${result['current_price']:.4f} via {result['exchange_used']}")
                except Exception as e:
                    coin = futures[future]
                    print(f"❌ Analysis timeout/error for {coin}: {e}")
                    continue

        # Send alerts if any signals found
        if signals:
            success = self.telegram_sender.send_alerts(signals, 
                ema_short=active_analysis['ema_short'], 
                ema_long=active_analysis['ema_long'],
                timeframe_minutes=15
            )

            crossover_count = len(signals)
            golden_count = len([s for s in signals if s.get('crossover_type') == 'golden_cross'])
            death_count = len([s for s in signals if s.get('crossover_type') == 'death_cross'])

            print(f"📱 Results: {crossover_count} signals ({golden_count} golden, {death_count} death)")
            print(f"📤 Telegram: {'✅ Sent' if success else '❌ Failed'}")
        else:
            print("📭 No EMA crossover signals found")

        # Display cache status
        cache = self.ema_indicator.load_cache()
        cache_name = f"{active_analysis['ema_short']}/{active_analysis['ema_long']}"
        print(f"📁 Cache ({cache_name}): {len(cache)} tracked symbols")

if __name__ == "__main__":
    analyzer = DynamicEMAAnalyzer()
    analyzer.run_analysis()
