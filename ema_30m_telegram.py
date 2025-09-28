"""
EMA 30M Telegram Alert System - FIXED VERSION  
NO external APIs - uses only OHLCV data like your existing system
"""

import os
import requests
from datetime import datetime
from typing import List, Dict

class EMA30MTelegram:
    def __init__(self):
        self.bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.chat_id = os.getenv('TELEGRAM_30M_CHAT_ID')
    
    def format_price(self, price: float) -> str:
        """Format price display"""
        if price < 0.001:
            return f"${price:.8f}"
        elif price < 1:
            return f"${price:.4f}"
        else:
            return f"${price:.2f}"
    
    def create_chart_links(self, symbol: str) -> tuple:
        """Create TradingView and CoinGlass links for 30M timeframe"""
        clean_symbol = symbol.replace('USDT', '').replace('USD', '')
        tv_link = f"https://www.tradingview.com/chart/?symbol={clean_symbol}USDT&interval=30"
        cg_link = f"https://www.coinglass.com/pro/futures/LiquidationHeatMapNew?coin={clean_symbol}"
        return tv_link, cg_link
    
    def send_alerts(self, signals: List[Dict]) -> bool:
        """Send EMA 30M alerts - NO EXTERNAL API CALLS"""
        if not self.bot_token or not self.chat_id or not signals:
            return False
        
        try:
            current_time = datetime.now().strftime('%H:%M:%S IST')
            
            message = f"""📊 EMA 30M Signal Detected
🕐 {current_time}
⏰ Timeframe: 30M Candles

🔄 CROSSOVER SIGNALS:"""
            
            # Group signals by type
            golden_signals = [s for s in signals if s.get('crossover_type') == 'golden_cross']
            death_signals = [s for s in signals if s.get('crossover_type') == 'death_cross']
            
            # Golden Cross signals
            if golden_signals:
                message += "\n🟡GOLDEN CROSS: "
                for i, signal in enumerate(golden_signals, 1):
                    symbol = signal['symbol']
                    price = self.format_price(signal['current_price'])  # Use price from OHLCV data
                    
                    tv_link, cg_link = self.create_chart_links(symbol)
                    
                    message += f"""
{i}. {symbol} | 💰 {price}
  📈[Chart →]({tv_link}) |🔥 [Liq Heat →]({cg_link})"""
            
            # Death Cross signals
            if death_signals:
                message += "\n🔴DEATH CROSS: "
                for i, signal in enumerate(death_signals, 1):
                    symbol = signal['symbol']
                    price = self.format_price(signal['current_price'])  # Use price from OHLCV data
                    
                    tv_link, cg_link = self.create_chart_links(symbol)
                    
                    message += f"""
{i}. {symbol} | 💰 {price}
  📈[Chart →]({tv_link}) |🔥 [Liq Heat →]({cg_link})"""
            
            # Summary
            total_crossovers = len(golden_signals) + len(death_signals)
            golden_count = len(golden_signals)
            death_count = len(death_signals)
            
            message += f"""


📊 EMA SUMMARY
• Total Crossovers: {total_crossovers} (🟡 {golden_count} Golden, 🔴 {death_count} Death)
🎯 EMA 12/21 crossover strategy
⚡ 30-minute timeframe for precise signals"""
            
            # Send message
            url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
            payload = {
                'chat_id': self.chat_id,
                'text': message,
                'parse_mode': 'Markdown',
                'disable_web_page_preview': False
            }
            
            response = requests.post(url, json=payload, timeout=30)
            response.raise_for_status()
            return True
            
        except Exception as e:
            print(f"❌ Telegram send error: {e}")
            return False
