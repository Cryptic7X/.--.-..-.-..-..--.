"""
Telegram Alert System for Multi-Timeframe System
Clean format with TradingView links and no exchange names
"""

import os
import requests
from datetime import datetime, timedelta

def get_ist_time():
    """Convert UTC to IST"""
    utc_now = datetime.utcnow()
    return utc_now + timedelta(hours=5, minutes=30)

def send_multi_alert(all_signals, timeframe):
    """Send consolidated multi-timeframe alert with TradingView links and no exchange names"""
    bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
    chat_id = os.getenv('TELEGRAM_MULTI_CHAT_ID')
    
    if not bot_token or not chat_id or not all_signals:
        return False
    
    ist_time = get_ist_time()
    current_time_str = ist_time.strftime('%H:%M:%S IST')
    
    # Group signals by type
    buy_signals = [s for s in all_signals if s['signal_type'] == 'BUY']
    sell_signals = [s for s in all_signals if s['signal_type'] == 'SELL']
    
    # Build message header
    message = f"📈 *MULTI-TIMEFRAME CIPHERB ALERT*\n\n"
    message += f"🎯 *{len(all_signals)} {timeframe.upper()} SIGNALS*\n"
    message += f"🕐 *{current_time_str}*\n"
    message += f"⏰ *Timeframe: {timeframe.upper()} Candles*\n\n"
    
    # TradingView interval mapping
    interval_map = {'15m': '15', '6h': '360', '8h': '480', '12h': '720'}
    interval = interval_map.get(timeframe, '360')

    # Add BUY signals with TradingView links
    if buy_signals:
        message += f"🟢 *{timeframe.upper()} BUY SIGNALS:*\n"
        for i, signal in enumerate(buy_signals, 1):
            symbol = signal['symbol']
            price = signal['price']
            change_24h = signal['change_24h']
            market_cap_m = signal['market_cap'] / 1_000_000
            volume_m = signal['volume_24h'] / 1_000_000
            age_s = signal.get('signal_age_seconds', 0)

            # Format price safely
            if price < 0.001:
                price_fmt = f"${price:.8f}"
            elif price < 1:
                price_fmt = f"${price:.4f}"
            else:
                price_fmt = f"${price:.3f}"

            # Build TradingView link
            clean_symbol = symbol.replace('USDT', '').replace('USD', '')
            tv_link = f"https://www.tradingview.com/chart/?symbol={clean_symbol}USDT&interval={interval}"

            message += f"\n{i}\\. *{symbol}* | {price_fmt} | {change_24h:+.1f}%\n"
            message += f"   Cap: ${market_cap_m:.0f}M | Vol: ${volume_m:.0f}M\n"
            message += f"   ⚡{age_s:.0f}s ago | [Chart →]({tv_link})"

    # Add SELL signals with TradingView links  
    if sell_signals:
        message += f"\n\n🔴 *{timeframe.upper()} SELL SIGNALS:*\n"
        for i, signal in enumerate(sell_signals, 1):
            symbol = signal['symbol']
            price = signal['price']
            change_24h = signal['change_24h']
            market_cap_m = signal['market_cap'] / 1_000_000
            volume_m = signal['volume_24h'] / 1_000_000
            age_s = signal.get('signal_age_seconds', 0)

            # Format price safely
            if price < 0.001:
                price_fmt = f"${price:.8f}"
            elif price < 1:
                price_fmt = f"${price:.4f}"
            else:
                price_fmt = f"${price:.3f}"

            # Build TradingView link
            clean_symbol = symbol.replace('USDT', '').replace('USD', '')
            tv_link = f"https://www.tradingview.com/chart/?symbol={clean_symbol}USDT&interval={interval}"

            message += f"\n{i}\\. *{symbol}* | {price_fmt} | {change_24h:+.1f}%\n"
            message += f"   Cap: ${market_cap_m:.0f}M | Vol: ${volume_m:.0f}M\n"
            message += f"   ⚡{age_s:.0f}s ago | [Chart →]({tv_link})"

    # Footer summary
    avg_age = sum(s.get('signal_age_seconds', 0) for s in all_signals) / len(all_signals)
    message += f"\n\n📊 *{timeframe.upper()} SUMMARY:*\n"
    message += f"• Total: {len(all_signals)} (avg age: {avg_age:.0f}s)\n"
    message += f"• Buy: {len(buy_signals)} | Sell: {len(sell_signals)}\n"
    message += f"• Multi\\-Timeframe CipherB System v3\\.0"

    # Send message with Markdown formatting
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        'chat_id': chat_id,
        'text': message,
        'parse_mode': 'MarkdownV2',
        'disable_web_page_preview': True
    }
    
    try:
        response = requests.post(url, json=payload, timeout=30)
        response.raise_for_status()
        print(f"📱 {timeframe} multi-timeframe alert sent: {len(all_signals)} signals")
        return True
    except Exception as e:
        print(f"❌ {timeframe} multi-timeframe alert failed: {e}")
        # Fallback: try without Markdown if formatting fails
        try:
            payload['parse_mode'] = None
            response = requests.post(url, json=payload, timeout=30)
            response.raise_for_status()
            print(f"📱 {timeframe} alert sent (plain text fallback)")
            return True
        except:
            return False

def send_admin_alert(error_type, error_message, timeframe=None):
    """Send system error to admin channel"""
    bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
    admin_chat_id = os.getenv('TELEGRAM_ADMIN_CHAT_ID')
    
    if not bot_token or not admin_chat_id:
        return False
    
    ist_time = get_ist_time()
    tf_info = f" ({timeframe})" if timeframe else ""
    
    # Simple string concatenation for admin alerts
    message = f"🚨 MULTI-TIMEFRAME SYSTEM ERROR{tf_info}\n\n"
    message += f"⚠️ Error Type: {error_type}\n"
    message += f"🕐 Time: {ist_time.strftime('%Y-%m-%d %H:%M:%S IST')}\n"
    message += f"🔧 System: Multi-Timeframe CipherB{tf_info}\n\n"
    message += f"Error Details:\n{error_message[:1000]}\n\n"
    message += f"🔧 Action Required: Check system logs"
    
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        'chat_id': admin_chat_id,
        'text': message
    }
    
    try:
        response = requests.post(url, json=payload, timeout=30)
        response.raise_for_status()
        return True
    except Exception as e:
        print(f"❌ Admin alert failed: {e}")
        return False
