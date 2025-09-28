#!/usr/bin/env python3
"""
EMA 15M Analyzer - STANDALONE SYSTEM
21/50 EMA crossover analysis on 15-minute timeframe
"""

import os
import sys
import concurrent.futures
from datetime import datetime

# Add src directory to path
sys.path.append(os.path.join(os.path.dirname(__file__)))

from exchange_manager import SimpleExchangeManager
from ema15m_indicator import EMA15MIndicator
from ema15m_telegram import EMA15MTelegramSender

class EMA15MAnalyzer:
    def __init__(self):
        self.exchange_manager = SimpleExchangeManager()
        self.telegram_sender = EMA15MTelegramSender()
        self.ema_indicator = EMA15MIndicator()

    def load_coins(self):
        """Load coins from coin_list.txt"""
        coin_file = os.path.join(os.path.dirname(__file__), '..', 'coin_list.txt')
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

            print(f"📊 Loaded {len(formatted_coins)} coins for EMA 15M analysis")
            return formatted_coins
        except FileNotFoundError:
            print("❌ coin_list.txt not found!")
            return []
        except Exception as e:
            print(f"❌ Error loading coins: {e}")
            return []

    def analyze_coin(self, symbol):
        """Analyze single coin for EMA crossover"""
        try:
            # Fetch 15M OHLCV data
            ohlcv_data, exchange_used = self.exchange_manager.fetch_ohlcv_with_fallback(
                symbol, '15m', limit=200
            )

            if not ohlcv_data:
                print(f"❌ No data fetched for {symbol}")
                return None

            # Analyze for crossover
            result = self.ema_indicator.analyze(ohlcv_data, symbol)

            if not result.get('crossover_alert', False):
                return None

            return {
                'symbol': symbol,
                'crossover_alert': result.get('crossover_alert', False),
                'crossover_type': result.get('crossover_type'),
                'current_price': result.get('current_price'),
                'exchange_used': exchange_used
            }

        except Exception as e:
            print(f"❌ Error analyzing {symbol}: {e}")
            return None

    def run_analysis(self):
        """Run complete EMA 15M analysis"""
        print("🟡 EMA 15M ANALYSIS - STANDALONE SYSTEM")
        print(f"⏰ Time: {datetime.now().strftime('%H:%M:%S IST')}")

        coins = self.load_coins()
        if not coins:
            print("❌ No coins to analyze")
            return

        signals = []

        # Analyze coins concurrently
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(self.analyze_coin, coin): coin for coin in coins}

            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result(timeout=30)
                    if result:
                        signals.append(result)
                        print(f"✅ CROSSOVER: {result['symbol']} - {result['crossover_type'].upper()}")
                except Exception as e:
                    coin = futures[future]
                    print(f"❌ Analysis timeout/error for {coin}: {e}")
                    continue

        # Send alerts if any signals found
        if signals:
            success = self.telegram_sender.send_alerts(signals, timeframe_minutes=15)

            crossover_count = len(signals)
            print(f"📱 Results: {crossover_count} crossover signals")
            print(f"📤 Telegram: {'✅ Sent' if success else '❌ Failed'}")
        else:
            print("📭 No EMA crossover signals found")

        # Display cache status
        cache = self.ema_indicator.load_cache()
        print(f"📁 Cache: {len(cache)} tracked symbols")

if __name__ == "__main__":
    analyzer = EMA15MAnalyzer()
    analyzer.run_analysis()
