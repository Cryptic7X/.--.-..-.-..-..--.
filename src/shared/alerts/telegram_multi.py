#!/usr/bin/env python3
"""
Multi-Timeframe Alerts - RESTORED to your original working format
"""

import os
import requests
from datetime import datetime, timedelta

def get_ist_time():
    return datetime.utcnow() + timedelta(hours=5, minutes=30)

def send_telegram_message(bot_token, chat_id, message):
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
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
        print(f"❌ Alert failed: {e}")
        # Try without Markdown as fallback
        payload['parse_mode'] = None
        try:
            response = requests.post(url, json=payload, timeout=30)
            response.raise_for_status()
            return True
        except:
            return False

def send_multi_consolidated_alert(signals, timeframe):
    if not signals:
        return False
    
    bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
    chat_id = os.getenv('TELEGRAM_MULTI_CHAT_ID')
    
    if not bot_token or not chat_id:
        return False
    
    ist_time = get_ist_time()
    buy_signals = [s for s in signals if s['signal_type'] == 'BUY']
    sell_signals = [s for s in signals if s['signal_type'] == 'SELL']
    
    # Simple, working message format (based on your original)
    message = f"""*📈 {timeframe.upper()} CIPHERB SIGNALS*

⏰ *{ist_time.strftime('%H:%M:%S IST')} | {len(signals)} Signals*

"""
    
    # BUY signals (NO exchange names, simple TradingView links)
    if buy_signals:
        message += f"*🟢 BUY SIGNALS:*\n"
        for i, signal in enumerate(buy_signals, 1):
            symbol = signal['symbol']
            price = signal['price']
            change_24h = signal['change_24h']
            
            # SIMPLE TradingView link (your original working format)
            tv_link = f"https://tradingview.com/chart/?symbol={symbol}&interval={timeframe}"
            
            message += f"\n{i}. *{symbol}* | ${price:.4f} | {change_24h:+.1f}%"
            message += f"\n   [📊 Chart]({tv_link})"
    
    # SELL signals
    if sell_signals:
        message += f"\n\n*🔴 SELL SIGNALS:*\n"
        for i, signal in enumerate(sell_signals, 1):
            symbol = signal['symbol']
            price = signal['price']
            change_24h = signal['change_24h']
            
            # SIMPLE TradingView link (your original working format)
            tv_link = f"https://tradingview.com/chart/?symbol={symbol}&interval={timeframe}"
            
            message += f"\n{i}. *{symbol}* | ${price:.4f} | {change_24h:+.1f}%"
            message += f"\n   [📊 Chart]({tv_link})"
    
    message += f"\n\n📊 *{timeframe.upper()} Summary:* {len(signals)} signals\n• Buy: {len(buy_signals)} | Sell: {len(sell_signals)}"
    
    return send_telegram_message(bot_token, chat_id, message)

def send_admin_alert(subject, error_message):
    bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
    admin_chat_id = os.getenv('TELEGRAM_ADMIN_CHAT_ID')
    
    if not bot_token or not admin_chat_id:
        return False
    
    message = f"*🚨 {subject}*\n\n{error_message}\n\nTime: {datetime.utcnow().strftime('%H:%M UTC')}"
    return send_telegram_message(bot_token, admin_chat_id, message)
