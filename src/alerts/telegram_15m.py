"""
Telegram Alert System for 15-Minute CipherB + CTO System
"""

import os
import requests
from datetime import datetime, timedelta

def get_ist_time():
    """Convert UTC to IST"""
    utc_now = datetime.utcnow()
    return utc_now + timedelta(hours=5, minutes=30)

def send_15m_alert(all_signals):
    """Send consolidated 15m alert to dedicated channel"""
    bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
    chat_id = os.getenv('TELEGRAM_15M_CHAT_ID')
    
    if not bot_token or not chat_id or not all_signals:
        return False
    
    ist_time = get_ist_time()
    current_time_str = ist_time.strftime('%H:%M:%S IST')
    
    # Group signals by type
    buy_signals = [s for s in all_signals if s['signal_type'] == 'BUY']
    sell_signals = [s for s in all_signals if s['signal_type'] == 'SELL']
    
    # Build message
    message = f"""🔧 *CIPHERB + CTO 15M ALERT*

🎯 *{len(all_signals)} CONFIRMED SIGNALS*
🕐 *{current_time_str}*
⏰ *Timeframe: 15M + CTO Confirmation*

"""

    # Add BUY signals
    if buy_signals:
        message += "🟢 *BUY SIGNALS (CTO Oversold):*\n"
        for i, signal in enumerate(buy_signals, 1):
            symbol = signal['symbol']
            price = signal['price']
            change_24h = signal['change_24h']
            market_cap_m = signal['market_cap'] / 1_000_000
            wt1 = signal['cipherb_wt1']
            wt2 = signal['cipherb_wt2']
            cto_score = signal['cto_score']
            exchange = signal['exchange']
            age_s = signal.get('signal_age_seconds', 0)
            
            # Format price
            if price < 0.001:
                price_fmt = f"${price:.8f}"
            elif price < 1:
                price_fmt = f"${price:.4f}"
            else:
                price_fmt = f"${price:.3f}"
            
            # TradingView link
            clean_symbol = symbol.replace('USDT', '').replace('USD', '')
            tv_link = f"https://www.tradingview.com/chart/?symbol={clean_symbol}USDT&interval=15"
            
            message += f"""
{i}. *{symbol}* | {price_fmt} | {change_24h:+.1f}%
   Cap: ${market_cap_m:.0f}M | WT: {wt1:.1f}/{wt2:.1f}
   CTO: {cto_score:.1f} (Oversold ✓) | {exchange}
   ⚡{age_s:.0f}s ago | [Chart →]({tv_link})"""

    # Add SELL signals
    if sell_signals:
        message += f"\n\n🔴 *SELL SIGNALS (CTO Overbought):*\n"
        for i, signal in enumerate(sell_signals, 1):
            symbol = signal['symbol']
            price = signal['price']
            change_24h = signal['change_24h']
            market_cap_m = signal['market_cap'] / 1_000_000
            wt1 = signal['cipherb_wt1']
            wt2 = signal['cipherb_wt2']
            cto_score = signal['cto_score']
            exchange = signal['exchange']
            age_s = signal.get('signal_age_seconds', 0)
            
            # Format price
            if price < 0.001:
                price_fmt = f"${price:.8f}"
            elif price < 1:
                price_fmt = f"${price:.4f}"
            else:
                price_fmt = f"${price:.3f}"
            
            # TradingView link
            clean_symbol = symbol.replace('USDT', '').replace('USD', '')
            tv_link = f"https://www.tradingview.com/chart/?symbol={clean_symbol}USDT&interval=15"
            
            message += f"""
{i}. *{symbol}* | {price_fmt} | {change_24h:+.1f}%
   Cap: ${market_cap_m:.0f}M | WT: {wt1:.1f}/{wt2:.1f}
   CTO: {cto_score:.1f} (Overbought ✓) | {exchange}
   ⚡{age_s:.0f}s ago | [Chart →]({tv_link})"""

    # Footer
    avg_age = sum(s.get('signal_age_seconds', 0) for s in all_signals) / len(all_signals)
    message += f"""

📊 *15M CONFIRMED SIGNAL SUMMARY:*
• Total Signals: {len(all_signals)} (avg age: {avg_age:.0f}s)
• Buy Signals: {len(buy_signals)}
• Sell Signals: {len(sell_signals)}
• Confirmation: CTO ±70 thresholds ✅
• Cooldown: 4-hour deduplication ✅

🎯 *CipherB + CTO 15M System v3.0*"""

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
        print(f"📱 15m alert sent: {len(all_signals)} confirmed signals")
        return True
    except Exception as e:
        print(f"❌ 15m alert failed: {e}")
        return False

def send_admin_alert(error_type, error_message):
    """Send system error to admin channel"""
    bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
    admin_chat_id = os.getenv('TELEGRAM_ADMIN_CHAT_ID')
    
    if not bot_token or not admin_chat_id:
        return False
    
    ist_time = get_ist_time()
    
    # Fixed: Use .format() instead of f-string to avoid syntax issues
    message = """🚨 *15M SYSTEM ERROR*

⚠️ *Error Type:* {error_type}
🕐 *Time:* {ist_time}
🔧 *System:* 15-Minute CipherB + CTO

*Error Details:*
