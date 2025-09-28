"""
Standalone EMA 12/21 Crossover Analyzer
30-minute timeframe with 6-hour cooldown
"""

import os
import sys
import concurrent.futures
from datetime import datetime

from exchange_30m import SimpleExchange30M
from ema_12_21_indicator import EMA12_21Indicator  
from ema_30m_telegram import EMA30MTelegram

class EMA30MAnalyzer:
    def __init__(self):
        self.exchange = SimpleExchange30M()
        self.indicator = EMA12_21Indicator()
        self.telegram = EMA30MTelegram()
    
    def load_coins(self):
        """Load coins from coins.txt file"""
        try:
            with open('coins.txt', 'r') as f:
                coins = [line.strip() for line in f if line.strip()]
            print(f"📊 Loaded {len(coins)} coins for EMA 30M analysis")
            return coins
        except Exception as e:
            print(f"❌ Error loading coins: {e}")
            return []
    
    def analyze_coin(self, symbol):
        """Analyze single coin for 12/21 EMA crossover"""
        try:
            ohlcv_data, exchange_used = self.exchange.fetch_ohlcv_with_fallback(symbol, limit=200)
            
            if not ohlcv_data:
                print(f"❌ No data for {symbol}")
                return None
            
            result = self.indicator.analyze(ohlcv_data, symbol)
            
            if not result.get('crossover_alert', False):
                return None
            
            return {
                'symbol': symbol,
                'crossover_alert': True,
                'crossover_type': result['crossover_type'],
                'exchange_used': exchange_used
            }
            
        except Exception as e:
            print(f"❌ Error analyzing {symbol}: {e}")
            return None
    
    def run_analysis(self):
        """Run complete 30M EMA analysis"""
        print("🟡 EMA 30M CROSSOVER ANALYSIS")
        print(f"⏰ Time: {datetime.now().strftime('%H:%M:%S IST')}")
        
        coins = self.load_coins()
        if not coins:
            return
        
        signals = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(self.analyze_coin, coin): coin for coin in coins}
            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result(timeout=30)
                    if result:
                        signals.append(result)
                        print(f"✅ CROSSOVER: {result['symbol']} ({result['crossover_type'].upper()})")
                except Exception as e:
                    print(f"❌ Analysis error: {e}")
        
        if signals:
            success = self.telegram.send_alerts(signals)
            print(f"📱 Telegram: {'✅ Sent' if success else '❌ Failed'}")
            print(f"📊 Total crossovers found: {len(signals)}")
        else:
            print("📭 No EMA crossover signals detected")

if __name__ == "__main__":
    analyzer = EMA30MAnalyzer()
    analyzer.run_analysis()
