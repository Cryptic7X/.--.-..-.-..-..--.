#!/usr/bin/env python3
"""
Telegram Alert System for 15-Minute CipherB + CTO Analysis
- Consolidated alerts to prevent spam
- Professional formatting with chart links
- Enhanced error handling
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

def send_15m_consolidated_alert(signals):
    """
    Send consolidated 15-minute CipherB + CTO alert
    """
    if not signals:
        return False
    
    bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
    chat_id = os.getenv('TELEGRAM_15M_CHAT_ID')
    
    if not bot_token or not chat_id:
        print("❌ Missing Telegram credentials for 15m system")
        return False
    
    ist_time = get_ist_time()
    
    # Group signals by type
    buy_signals = [s for s in signals if s['signal_type'] == 'BUY']
    sell_signals = [s for s in signals if s['signal_type'] == 'SELL']
    
    # Build message
    message = f"""🎯 *15-MINUTE CIPHERB + CTO ALERTS*

🎯 *{len(signals)} CONFIRMED SIGNALS*
🕐 *{ist_time.strftime('%H:%M:%S IST')}*
⏰ *Timeframe: 15-minute candles + CTO confirmation*
🔧 *Thresholds: CTO ±70 (enhanced precision)*

"""
    
    # Add BUY signals
    if buy_signals:
        message += "🟢 *BUY SIGNALS (CTO OVERSOLD):*\n"
        for i, signal in enumerate(buy_signals, 1):
            symbol = signal['symbol']
            price_fmt = format_price(signal['price'])
            change_24h = signal['change_24h']
            market_cap_m = signal['market_cap'] / 1_000_000
            cto_score = signal['cto_score']
            wt1 = signal['cipherb_wt1']
            wt2 = signal['cipherb_wt2']
            exchange = signal['exchange']
            
            # TradingView link for 15m
            clean_symbol = symbol.replace('USDT', '').replace('USD', '')
            tv_link = f"https://www.tradingview.com/chart/?symbol={clean_symbol}USDT&interval=15"
            
            message += f"""
{i}. *{symbol}* | {price_fmt} | {change_24h:+.1f}%
   Cap: ${market_cap_m:.0f}M | CTO: {cto_score:.1f} (✓ Oversold)
   WT: {wt1:.1f}/{wt2:.1f} | {exchange} | [Chart →]({tv_link})"""
    
    # Add SELL signals
    if sell_signals:
        message += f"\n\n🔴 *SELL SIGNALS (CTO OVERBOUGHT):*\n"
        for i, signal in enumerate(sell_signals, 1):
            symbol = signal['symbol']
            price_fmt = format_price(signal['price'])
            change_24h = signal['change_24h']
            market_cap_m = signal['market_cap'] / 1_000_000
            cto_score = signal['cto_score']
            wt1 = signal['cipherb_wt1']
            wt2 = signal['cipherb_wt2']
            exchange = signal['exchange']
            
            # TradingView link for 15m
            clean_symbol = symbol.replace('USDT', '').replace('USD', '')
            tv_link = f"https://www.tradingview.com/chart/?symbol={clean_symbol}USDT&interval=15"
            
            message += f"""
{i}. *{symbol}* | {price_fmt} | {change_24h:+.1f}%
   Cap: ${market_cap_m:.0f}M | CTO: {cto_score:.1f} (✓ Overbought)
   WT: {wt1:.1f}/{wt2:.1f} | {exchange} | [Chart →]({tv_link})"""
    
    # Footer
    message += f"""

📊 *15M CTO CONFIRMATION SUMMARY:*
• Total Signals: {len(signals)} (Buy: {len(buy_signals)}, Sell: {len(sell_signals)})
• Confirmation Rate: 100% (CTO + CipherB aligned)
• Cooldown Active: 4-hour same direction
• Alert Quality: ✅ Enhanced precision with CTO filter

🎯 *Enhanced 15-Minute CipherB + CTO System v3.0*"""
    
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
📍 *Component:* 15-Minute Analysis

🔧 *Action Required:* Check system logs and resolve issue"""
    
    return send_telegram_message(bot_token, admin_chat_id, message)
