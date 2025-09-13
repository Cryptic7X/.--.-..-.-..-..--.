#!/usr/bin/env python3
"""
Fixed Telegram Alert System for Multi-Timeframe Analysis
- Proper MarkdownV2 escaping to prevent 400 errors
- Timeframe-specific formatting
- Enhanced error handling
"""

import os
import requests
from datetime import datetime, timedelta

def escape_markdown_v2(text: str) -> str:
    """Escape special characters for Telegram MarkdownV2"""
    escape_chars = '_*[]()~`>#+-=|{}.!'
    for char in escape_chars:
        text = text.replace(char, '\\' + char)
    return text

def get_ist_time():
    """Convert UTC to IST"""
    return datetime.utcnow() + timedelta(hours=5, minutes=30)

def send_telegram_message(bot_token, chat_id, message):
    """Send message via Telegram Bot API with proper error handling"""
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    
    # Validate message length (Telegram limit: 4096 characters)
    if len(message) > 4096:
        message = message[:4090] + "..."
        print(f"⚠️ Message truncated to fit Telegram limit")
    
    payload = {
        'chat_id': chat_id,
        'text': message,
        'parse_mode': 'MarkdownV2',
        'disable_web_page_preview': True  # Prevent link previews
    }
    
    try:
        response = requests.post(url, json=payload, timeout=30)
        response.raise_for_status()
        return True
    except Exception as e:
        print(f"❌ Telegram message failed: {e}")
        
        # Fallback: try sending as plain text
        payload['parse_mode'] = None
        payload['text'] = message.replace('\\', '').replace('*', '').replace('_', '')
        
        try:
            response = requests.post(url, json=payload, timeout=30)
            response.raise_for_status()
            print("✅ Sent as plain text fallback")
            return True
        except Exception as e2:
            print(f"❌ Plain text fallback also failed: {e2}")
            return False

def get_timeframe_emoji(timeframe):
    """Get emoji for timeframe"""
    emoji_map = {'6h': '🕕', '8h': '🕗', '12h': '🕛'}
    return emoji_map.get(timeframe, '⏰')

def send_multi_consolidated_alert(signals, timeframe):
    """Send consolidated multi-timeframe CipherB alert"""
    if not signals:
        return False
    
    bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
    chat_id = os.getenv('TELEGRAM_MULTI_CHAT_ID')
    
    if not bot_token or not chat_id:
        print("❌ Missing Telegram credentials for multi-timeframe system")
        return False
    
    ist_time = get_ist_time()
    tf_emoji = get_timeframe_emoji(timeframe)
    
    # Build message with proper escaping
    buy_signals = [s for s in signals if s['signal_type'] == 'BUY']
    sell_signals = [s for s in signals if s['signal_type'] == 'SELL']
    
    # Header (escape all dynamic content)
    time_str = escape_markdown_v2(ist_time.strftime('%H:%M:%S IST'))
    tf_upper = escape_markdown_v2(timeframe.upper())
    signal_count = len(signals)
    
    message = f"""📈 *MULTI\\-TIMEFRAME CIPHERB ALERTS*

{tf_emoji} *{signal_count} {tf_upper} SIGNALS*
🕐 *{time_str}*
⏰ *Timeframe: {tf_upper} candles*

"""
    
    # Add BUY signals
    if buy_signals:
        message += f"🟢 *{tf_upper} BUY SIGNALS:*\n"
        for i, signal in enumerate(buy_signals, 1):
            symbol = escape_markdown_v2(signal['symbol'])
            tf_disp = escape_markdown_v2(signal['timeframe'].upper())
            price = f"{signal['price']:.4f}"
            price_esc = escape_markdown_v2(price)
            change_24h = f"{signal['change_24h']:+.1f}"
            change_esc = escape_markdown_v2(change_24h)
            exchange = escape_markdown_v2(signal['exchange'])
            
            message += f"\n{i}\\. *{symbol}* \\({tf_disp}\\) \\| ${price_esc} \\| {change_esc}%\n"
            message += f"   {exchange}\n"
    
    # Add SELL signals
    if sell_signals:
        message += f"\n🔴 *{tf_upper} SELL SIGNALS:*\n"
        for i, signal in enumerate(sell_signals, 1):
            symbol = escape_markdown_v2(signal['symbol'])
            tf_disp = escape_markdown_v2(signal['timeframe'].upper())
            price = f"{signal['price']:.4f}"
            price_esc = escape_markdown_v2(price)
            change_24h = f"{signal['change_24h']:+.1f}"
            change_esc = escape_markdown_v2(change_24h)
            exchange = escape_markdown_v2(signal['exchange'])
            
            message += f"\n{i}\\. *{symbol}* \\({tf_disp}\\) \\| ${price_esc} \\| {change_esc}%\n"
            message += f"   {exchange}\n"
    
    # Footer
    buy_count = len(buy_signals)
    sell_count = len(sell_signals)
    
    message += f"""
📊 *MULTI\\-TIMEFRAME SUMMARY:*
• {tf_upper} Signals: {signal_count} \\(Buy: {buy_count}, Sell: {sell_count}\\)
• Suppression: Advanced cascading active
• System: Multi\\-Timeframe v3\\.0

\\#Trading \\#Crypto"""
    
    return send_telegram_message(bot_token, chat_id, message)

def send_admin_alert(subject, error_message):
    """Send admin alert for system errors"""
    bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
    admin_chat_id = os.getenv('TELEGRAM_ADMIN_CHAT_ID')
    
    if not bot_token or not admin_chat_id:
        print("❌ Missing admin Telegram credentials")
        return False
    
    utc_time = datetime.utcnow()
    time_str = escape_markdown_v2(utc_time.strftime('%Y-%m-%d %H:%M:%S UTC'))
    subject_esc = escape_markdown_v2(subject)
    message_esc = escape_markdown_v2(error_message[:500])  # Limit error message length
    
    message = f"""🚨 *SYSTEM ALERT: {subject_esc}*

⚠️ *Error Details:*
{message_esc}

🕐 *Time:* {time_str}
🎯 *System:* Enhanced GitHub Actions

🔧 *Action Required:* Check system logs"""
    
    return send_telegram_message(bot_token, chat_id, message)
