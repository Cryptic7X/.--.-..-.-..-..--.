#!/usr/bin/env python3
"""
Telegram Alert System for Multi-Timeframe CipherB Analysis
- Advanced suppression-aware alerts
- Timeframe source identification
- Professional multi-timeframe formatting
"""

import os
import requests
from datetime import datetime, timedelta

def get_ist_time():
    """Convert UTC to IST"""
    return datetime.utcnow() + timedelta(hours=5, minutes=30)

def send_telegram_message(bot_token, chat_id, message):
    """Send message via Telegram Bot API"""
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
        print(f"❌ Telegram message failed: {e}")
        return False

def format_price(price):
    """Format price based on value"""
    if price < 0.001:
        return f"${price:.8f}"
    elif price < 1:
        return f"${price:.4f}"
    else:
        return f"${price:.3f}"

def get_timeframe_emoji(timeframe):
    """Get emoji for timeframe"""
    emoji_map = {
        '6h': '🕕',
        '8h': '🕗', 
        '12h': '🕛'
    }
    return emoji_map.get(timeframe, '⏰')

def send_multi_consolidated_alert(signals, timeframe):
    """
    Send consolidated multi-timeframe CipherB alert
    """
    if not signals:
        return False
    
    bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
    chat_id = os.getenv('TELEGRAM_MULTI_CHAT_ID')
    
    if not bot_token or not chat_id:
        print("❌ Missing Telegram credentials for multi-timeframe system")
        return False
    
    ist_time = get_ist_time()
    tf_emoji = get_timeframe_emoji(timeframe)
    
    # Group signals by type
    buy_signals = [s for s in signals if s['signal_type'] == 'BUY']
    sell_signals = [s for s in signals if s['signal_type'] == 'SELL']
    
    # Build message
    message = f"""📈 *MULTI-TIMEFRAME CIPHERB ALERTS*

{tf_emoji} *{len(signals)} {timeframe.upper()} SIGNALS*
🕐 *{ist_time.strftime('%H:%M:%S IST')}*
⏰ *Primary Timeframe: {timeframe.upper()} candles*
🔄 *Suppression: Advanced multi-timeframe logic*

"""
    
    # Add BUY signals
    if buy_signals:
        message += f"🟢 *{timeframe.upper()} BUY SIGNALS:*\n"
        for i, signal in enumerate(buy_signals, 1):
            symbol = signal['symbol']
            price_fmt = format_price(signal['price'])
            change_24h = signal['change_24h']
            market_cap_m = signal['market_cap'] / 1_000_000
            wt1 = signal['wt1']
            wt2 = signal['wt2']
            exchange = signal['exchange']
            suppression_action = signal.get('suppression_action', 'allow')
            
            # TradingView link with correct timeframe
            clean_symbol = symbol.replace('USDT', '').replace('USD', '')
            tv_link = f"https://www.tradingview.com/chart/?symbol={clean_symbol}USDT&interval={timeframe}"
            
            message += f"""
{i}. *{symbol}* ({timeframe.upper()}) | {price_fmt} | {change_24h:+.1f}%
   Cap: ${market_cap_m:.0f}M | WT: {wt1:.1f}/{wt2:.1f}
   {exchange} | Action: {suppression_action} | [Chart →]({tv_link})"""
    
    # Add SELL signals
    if sell_signals:
        message += f"\n\n🔴 *{timeframe.upper()} SELL SIGNALS:*\n"
        for i, signal in enumerate(sell_signals, 1):
            symbol = signal['symbol']
            price_fmt = format_price(signal['price'])
            change_24h = signal['change_24h']
            market_cap_m = signal['market_cap'] / 1_000_000
            wt1 = signal['wt1']
            wt2 = signal['wt2']
            exchange = signal['exchange']
            suppression_action = signal.get('suppression_action', 'allow')
            
            # TradingView link with correct timeframe
            clean_symbol = symbol.replace('USDT', '').replace('USD', '')
            tv_link = f"https://www.tradingview.com/chart/?symbol={clean_symbol}USDT&interval={timeframe}"
            
            message += f"""
{i}. *{symbol}* ({timeframe.upper()}) | {price_fmt} | {change_24h:+.1f}%
   Cap: ${market_cap_m:.0f}M | WT: {wt1:.1f}/{wt2:.1f}
   {exchange} | Action: {suppression_action} | [Chart →]({tv_link})"""
    
    # Footer
    message += f"""

📊 *MULTI-TIMEFRAME SUMMARY:*
• {timeframe.upper()} Signals: {len(signals)} (Buy: {len(buy_signals)}, Sell: {len(sell_signals)})
• Suppression Logic: ✅ Advanced cascading active
• Next Timeframes: 6h → 8h → 12h escalation
• Pure CipherB: No confirmation filters applied

🎯 *Multi-Timeframe CipherB System v3.0*"""
    
    return send_telegram_message(bot_token, chat_id, message)

def send_admin_alert(subject, error_message):
    """Send admin alert for system errors"""
    bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
    admin_chat_id = os.getenv('TELEGRAM_ADMIN_CHAT_ID')
    
    if not bot_token or not admin_chat_id:
        print("❌ Missing admin Telegram credentials")
        return False
    
    utc_time = datetime.utcnow()
    message = f"""🚨 *SYSTEM ALERT: {subject}*

⚠️ *Error Details:*
{error_message}

🕐 *Time:* {utc_time.strftime('%Y-%m-%d %H:%M:%S UTC')}
🎯 *System:* Enhanced GitHub Actions
📍 *Component:* Multi-Timeframe Analysis

🔧 *Action Required:* Check system logs and resolve issue"""
    
    return send_telegram_message(bot_token, admin_chat_id, message)
