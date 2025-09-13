#!/usr/bin/env python3
"""
Multi-Timeframe Telegram Alerts - CORRECTED to match your working 30m system
"""

import os
import requests
from datetime import datetime, timedelta

def get_ist_time():
    """Convert UTC to IST"""
    utc_now = datetime.utcnow()
    return utc_now + timedelta(hours=5, minutes=30)

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
        print(f"❌ Alert failed: {e}")
        # Fallback to plain text
        payload['parse_mode'] = None
        try:
            response = requests.post(url, json=payload, timeout=30)
            response.raise_for_status()
            return True
        except:
            return False

def send_multi_consolidated_alert(signals, timeframe):
    """Send consolidated multi-timeframe alert - EXACT format from your 30m system"""
    if not signals:
        return False
    
    bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
    chat_id = os.getenv('TELEGRAM_MULTI_CHAT_ID')
    
    if not bot_token or not chat_id:
        return False
    
    # Current IST time
    ist_time = get_ist_time()
    current_time_str = ist_time.strftime('%H:%M:%S IST')
    
    # Group signals by type
    buy_signals = [s for s in signals if s['signal_type'] == 'BUY']
    sell_signals = [s for s in signals if s['signal_type'] == 'SELL']
    
    # Build message - EXACT format from your working 30m system
    message = f"""🔧 *EXACT CIPHERB {timeframe.upper()} ALERT*

🎯 *{len(signals)} PRECISE SIGNALS*
🕐 *{current_time_str}*
⏰ *Timeframe: {timeframe.upper()} Candles*

"""
    
    # Add BUY signals - EXACT format from your working system
    if buy_signals:
        message += "🟢 *BUY SIGNALS:*\n"
        for i, signal in enumerate(buy_signals, 1):
            symbol = signal['symbol']
            price = signal['price']
            change_24h = signal['change_24h']
            market_cap_m = signal.get('market_cap', 0) / 1_000_000
            wt1 = signal.get('wt1', 0)
            wt2 = signal.get('wt2', 0)
            age_s = signal.get('signal_age_seconds', 0)
            
            # Format price - EXACT from your system
            if price < 0.001:
                price_fmt = f"${price:.8f}"
            elif price < 1:
                price_fmt = f"${price:.4f}"
            else:
                price_fmt = f"${price:.3f}"
            
            # CORRECT TradingView link - EXACT from your working system
            # Inside send_multi_consolidated_alert:
            clean_symbol = symbol.replace('USDT', '').replace('USD', '')
            tv_link = f"https://www.tradingview.com/chart/?symbol={clean_symbol}USDT&interval=12h"

            
            message += f"""
{i}. *{symbol}* | {price_fmt} | {change_24h:+.1f}%
   Cap: ${market_cap_m:.0f}M | WT: {wt1:.1f}/{wt2:.1f}
   ⚡{age_s:.0f}s ago | [Chart →]({tv_link})"""
    
    # Add SELL signals - EXACT format from your working system
    if sell_signals:
        message += f"\n\n🔴 *SELL SIGNALS:*\n"
        for i, signal in enumerate(sell_signals, 1):
            symbol = signal['symbol']
            price = signal['price']
            change_24h = signal['change_24h']
            market_cap_m = signal.get('market_cap', 0) / 1_000_000
            wt1 = signal.get('wt1', 0)
            wt2 = signal.get('wt2', 0)
            age_s = signal.get('signal_age_seconds', 0)
            
            # Format price - EXACT from your system
            if price < 0.001:
                price_fmt = f"${price:.8f}"
            elif price < 1:
                price_fmt = f"${price:.4f}"
            else:
                price_fmt = f"${price:.3f}"
            
            # CORRECT TradingView link - EXACT from your working system
            clean_symbol = symbol.replace('USDT', '').replace('USD', '')
            tv_link = f"https://www.tradingview.com/chart/?symbol={clean_symbol}USDT&interval={timeframe}"
            
            message += f"""
{i}. *{symbol}* | {price_fmt} | {change_24h:+.1f}%
   Cap: ${market_cap_m:.0f}M | WT: {wt1:.1f}/{wt2:.1f}
   ⚡{age_s:.0f}s ago | [Chart →]({tv_link})"""
    
    # Footer - EXACT from your working system
    avg_age = sum(s.get('signal_age_seconds', 0) for s in signals) / len(signals)
    message += f"""

📊 *FRESH {timeframe.upper()} SIGNAL SUMMARY:*
• Total Signals: {len(signals)} (avg age: {avg_age:.0f}s)
• Buy Signals: {len(buy_signals)}
• Sell Signals: {len(sell_signals)}
• Fresh Detection: ✅ No duplicates or stale alerts
• Timeframe: {timeframe.upper()} candles only

🎯 *Fresh {timeframe.upper()} CipherB System v2.0*"""
    
    return send_telegram_message(bot_token, chat_id, message)

def send_admin_alert(subject, error_message):
    """Send admin alert"""
    bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
    admin_chat_id = os.getenv('TELEGRAM_ADMIN_CHAT_ID')
    
    if not bot_token or not admin_chat_id:
        return False
    
    message = f"*🚨 {subject}*\n\n{error_message}\n\nTime: {datetime.utcnow().strftime('%H:%M UTC')}"
    return send_telegram_message(bot_token, admin_chat_id, message)
