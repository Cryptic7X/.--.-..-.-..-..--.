"""
Telegram Alert System for 30-Minute CipherB + CTO System
"""

import os
import requests
from datetime import datetime, timedelta

def get_ist_time():
    """Convert UTC to IST"""
    utc_now = datetime.utcnow()
    return utc_now + timedelta(hours=5, minutes=30)

def send_30m_alert(all_signals):
    """Send consolidated 30m alert to dedicated channel"""
    bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
    chat_id = os.getenv('TELEGRAM_30M_CHAT_ID')
    
    if not bot_token or not chat_id or not all_signals:
        return False
    
    ist_time = get_ist_time()
    current_time_str = ist_time.strftime('%H:%M:%S IST')
    
    # Group signals by type
    buy_signals = [s for s in all_signals if s['signal_type'] == 'BUY']
    sell_signals = [s for s in all_signals if s['signal_type'] == 'SELL']
    
    # Build message header
    message = f"🔧 CIPHERB + CTO 30M ALERT\n\n"
    message += f"🎯 {len(all_signals)} CONFIRMED SIGNALS\n"
    message += f"🕐 {current_time_str}\n"
    message += f"⏰ Timeframe: 30M + CTO Confirmation\n\n"

    # Add BUY signals with 3-line format
    if buy_signals:
        message += "🟢 BUY SIGNALS:\n"
        for i, signal in enumerate(buy_signals, 1):
            symbol = signal['symbol']
            price = signal['price']
            change_24h = signal['change_24h']
            market_cap_m = signal['market_cap'] / 1_000_000
            wt1 = signal['cipherb_wt1']
            wt2 = signal['cipherb_wt2']
            cto_score = signal['cto_score']
            
            # Format price
            if price < 0.001:
                price_fmt = f"${price:.8f}"
            elif price < 1:
                price_fmt = f"${price:.4f}"
            else:
                price_fmt = f"${price:.3f}"
            
            # Determine CTO status
            cto_status = "Oversold ✓" if cto_score <= -70 else "Overbought ✓"
            
            # 3-line format for 30m
            message += f"{i}. {symbol} | {price_fmt} | {change_24h:+.1f}%\n"
            message += f"   Cap: ${market_cap_m:.0f}M | WT: {wt1:.1f}/{wt2:.1f}\n"
            message += f"   CTO: {cto_score:.1f} ({cto_status}) | [Chart →](https://www.tradingview.com/chart/?symbol={symbol}USDT&interval=30)\n"

    # Add SELL signals with 3-line format
    if sell_signals:
        message += f"\n🔴 SELL SIGNALS:\n"
        for i, signal in enumerate(sell_signals, 1):
            symbol = signal['symbol']
            price = signal['price']
            change_24h = signal['change_24h']
            market_cap_m = signal['market_cap'] / 1_000_000
            wt1 = signal['cipherb_wt1']
            wt2 = signal['cipherb_wt2']
            cto_score = signal['cto_score']
            
            # Format price
            if price < 0.001:
                price_fmt = f"${price:.8f}"
            elif price < 1:
                price_fmt = f"${price:.4f}"
            else:
                price_fmt = f"${price:.3f}"
            
            # Determine CTO status
            cto_status = "Oversold ✓" if cto_score <= -70 else "Overbought ✓"
            
            # 3-line format for 30m
            message += f"{i}. {symbol} | {price_fmt} | {change_24h:+.1f}%\n"
            message += f"   Cap: ${market_cap_m:.0f}M | WT: {wt1:.1f}/{wt2:.1f}\n"
            message += f"   CTO: {cto_score:.1f} ({cto_status}) | [Chart →](https://www.tradingview.com/chart/?symbol={symbol}USDT&interval=30)\n"

    # Footer
    avg_age = sum(s.get('signal_age_seconds', 0) for s in all_signals) / len(all_signals)
    message += f"\n📊 30M CONFIRMED SIGNAL SUMMARY:\n"
    message += f"• Total Signals: {len(all_signals)} (avg age: {avg_age:.0f}s)\n"
    message += f"• Buy Signals: {len(buy_signals)}\n"
    message += f"• Sell Signals: {len(sell_signals)}\n"
    message += f"• Confirmation: CTO ±70 thresholds ✅\n"
    message += f"• Cooldown: 4-hour deduplication ✅\n\n"
    message += f"🎯 CipherB + CTO 30M System v3.0"

    # Send message
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
        print(f"📱 30m alert sent: {len(all_signals)} confirmed signals")
        return True
    except Exception as e:
        print(f"❌ 30m alert failed: {e}")
        return False

def send_admin_alert(error_type, error_message):
    """Send system error to admin channel"""
    bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
    admin_chat_id = os.getenv('TELEGRAM_ADMIN_CHAT_ID')
    
    if not bot_token or not admin_chat_id:
        return False
    
    ist_time = get_ist_time()
    
    # Simple string concatenation to avoid triple-quote issues
    message = "🚨 30M SYSTEM ERROR\n\n"
    message += f"⚠️ Error Type: {error_type}\n"
    message += f"🕐 Time: {ist_time.strftime('%Y-%m-%d %H:%M:%S IST')}\n"
    message += f"🔧 System: 30-Minute CipherB + CTO\n\n"
    message += f"Error Details:\n{error_message[:1000]}\n\n"
    message += f"🔧 Action Required: Check system logs"
    
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        'chat_id': admin_chat_id,
        'text': message,
        'parse_mode': 'Markdown'
    }
    
    try:
        response = requests.post(url, json=payload, timeout=30)
        response.raise_for_status()
        return True
    except Exception as e:
        print(f"❌ Admin alert failed: {e}")
        return False
