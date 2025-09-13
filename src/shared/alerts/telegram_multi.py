#!/usr/bin/env python3
"""
Multi-Timeframe Telegram Alerts - RESTORED to working version
"""

import os
import requests
from datetime import datetime, timedelta

def get_ist_time():
    return datetime.utcnow() + timedelta(hours=5, minutes=30)

def send_telegram_message(bot_token, chat_id, message):
    """Send message with proper error handling and fallback"""
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    
    # Try with Markdown first
    payload = {
        'chat_id': chat_id,
        'text': message,
        'parse_mode': 'Markdown',
        'disable_web_page_preview': False
    }
    
    try:
        response = requests.post(url, json=payload, timeout=30)
        response.raise_for_status()
        return True
    except Exception as e:
        print(f"❌ Markdown failed: {e}")
        
        # Fallback to plain text
        payload['parse_mode'] = None
        try:
            response = requests.post(url, json=payload, timeout=30)
            response.raise_for_status()
            print("✅ Sent as plain text")
            return True
        except Exception as e2:
            print(f"❌ Plain text also failed: {e2}")
            return False

def send_multi_consolidated_alert(signals, timeframe):
    if not signals:
        return False
    
    bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
    chat_id = os.getenv('TELEGRAM_MULTI_CHAT_ID')
    
    if not bot_token or not chat_id:
        print("❌ Missing Telegram credentials")
        return False
    
    ist_time = get_ist_time()
    buy_signals = [s for s in signals if s['signal_type'] == 'BUY']
    sell_signals = [s for s in signals if s['signal_type'] == 'SELL']
    
    # Timeframe emojis
    tf_emoji = {'6h': '🕕', '8h': '🕗', '12h': '🕛'}.get(timeframe, '⏰')
    
    # Build clean message with TradingView links
    message = f"""*📈 MULTI-TIMEFRAME CIPHERB ALERTS*

{tf_emoji} *{timeframe.upper()} Analysis | {len(signals)} Signals*
⏰ *{ist_time.strftime('%H:%M:%S IST')}*

"""
    
    # Add BUY signals with TradingView links
    if buy_signals:
        message += f"*🟢 {timeframe.upper()} BUY SIGNALS:*\n"
        for i, signal in enumerate(buy_signals, 1):
            symbol = signal['symbol']
            price = signal['price']
            change_24h = signal['change_24h']
            market_cap_m = signal.get('market_cap', 0) / 1_000_000
            wt1 = signal.get('wt1', 0)
            wt2 = signal.get('wt2', 0)
            exchange = signal.get('exchange', 'Unknown')
            
            # Clean TradingView link with correct timeframe
            clean_symbol = symbol.replace('USDT', '').replace('USD', '')
            tv_link = f"https://www.tradingview.com/chart/?symbol={clean_symbol}USDT&interval={timeframe}"
            
            message += f"""
{i}. *{symbol}* ({timeframe.upper()}) | ${price:.4f} | {change_24h:+.1f}%
   Cap: ${market_cap_m:.0f}M | WT: {wt1:.1f}/{wt2:.1f} | {exchange}
   [📈 Chart]({tv_link})"""
    
    # Add SELL signals with TradingView links
    if sell_signals:
        message += f"\n\n*🔴 {timeframe.upper()} SELL SIGNALS:*\n"
        for i, signal in enumerate(sell_signals, 1):
            symbol = signal['symbol']
            price = signal['price']
            change_24h = signal['change_24h']
            market_cap_m = signal.get('market_cap', 0) / 1_000_000
            wt1 = signal.get('wt1', 0)
            wt2 = signal.get('wt2', 0)
            exchange = signal.get('exchange', 'Unknown')
            
            # Clean TradingView link with correct timeframe
            clean_symbol = symbol.replace('USDT', '').replace('USD', '')
            tv_link = f"https://www.tradingview.com/chart/?symbol={clean_symbol}USDT&interval={timeframe}"
            
            message += f"""
{i}. *{symbol}* ({timeframe.upper()}) | ${price:.4f} | {change_24h:+.1f}%
   Cap: ${market_cap_m:.0f}M | WT: {wt1:.1f}/{wt2:.1f} | {exchange}
   [📈 Chart]({tv_link})"""
    
    # Footer
    message += f"""

📊 *{timeframe.upper()} Summary:* {len(signals)} pure CipherB signals
• Buy: {len(buy_signals)} | Sell: {len(sell_signals)}
• System: Multi-Timeframe CipherB v3.0

#Trading #Crypto"""
    
    return send_telegram_message(bot_token, chat_id, message)

def send_admin_alert(subject, error_message):
    bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
    admin_chat_id = os.getenv('TELEGRAM_ADMIN_CHAT_ID')
    
    if not bot_token or not admin_chat_id:
        return False
    
    message = f"""*🚨 SYSTEM ALERT: {subject}*

{error_message}

Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}"""
    
    return send_telegram_message(bot_token, admin_chat_id, message)
