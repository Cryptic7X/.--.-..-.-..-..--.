"""
CipherB Telegram Sender - Direction-Based Alert System
Shows CipherB buy/sell signals with your exact format
"""

import os
import requests
from datetime import datetime
from typing import List, Dict

class CipherBTelegram:
    def __init__(self):
        self.bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.chat_id = os.getenv('TELEGRAM_15M_CHAT_ID')
    
    def format_price(self, price: float) -> str:
        """Format price display"""
        if price < 0.001:
            return f"${price:.8f}"
        elif price < 1:
            return f"${price:.4f}"
        else:
            return f"${price:.2f}"
    
    def create_chart_links(self, symbol: str, timeframe_minutes: int = 15) -> tuple:
        """Create TradingView and CoinGlass links"""
        clean_symbol = symbol.replace('USDT', '').replace('USD', '')
        tv_link = f"https://www.tradingview.com/chart/?symbol={clean_symbol}USDT&interval={timeframe_minutes}"
        cg_link = f"https://www.coinglass.com/pro/futures/LiquidationHeatMapNew?coin={clean_symbol}"
        return tv_link, cg_link
    
    def send_alerts(self, signals: List[Dict], timeframe_minutes: int = 15) -> bool:
        """Send CipherB direction-based alerts"""
        if not self.bot_token or not self.chat_id or not signals:
            return False
        
        try:
            current_time = datetime.now().strftime('%H:%M:%S IST')
            
            message = f"""📊 CipherB 15M Signal Detected
🕐 {current_time}
⏰ Timeframe: 15M Candles

🔄 CIPHER B SIGNALS:"""
            
            # Group signals by type
            buy_signals = [s for s in signals if s.get('signal_type') == 'buy']
            sell_signals = [s for s in signals if s.get('signal_type') == 'sell']
            
            # Buy signals
            if buy_signals:
                message += "\n🟡BUY SIGNAL:"
                for i, signal in enumerate(buy_signals, 1):
                    symbol = signal['symbol']
                    price = self.format_price(signal['current_price'])
                    
                    tv_link, cg_link = self.create_chart_links(symbol, timeframe_minutes)
                    
                    message += f"""
{i}. {symbol} | 💰 {price}
  📈[Chart →]({tv_link}) |🔥 [Liq Heat →]({cg_link})"""
            
            # Sell signals
            if sell_signals:
                message += "\n🔴SELL SIGNAL:"
                for i, signal in enumerate(sell_signals, 1):
                    symbol = signal['symbol']
                    price = self.format_price(signal['current_price'])
                    
                    tv_link, cg_link = self.create_chart_links(symbol, timeframe_minutes)
                    
                    message += f"""
{i}. {symbol} | 💰 {price}
  📈[Chart →]({tv_link}) |🔥 [Liq Heat →]({cg_link})"""
            
            # Summary
            total_signals = len(buy_signals) + len(sell_signals)
            buy_count = len(buy_signals)
            sell_count = len(sell_signals)
            
            message += f"""

📊 CIPHER B SUMMARY
• Total Signals: {total_signals} (🟡 {buy_count} Buy, 🔴 {sell_count} Sell)
⚡ CipherB - 15M timeframe for precise signals"""
            
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
