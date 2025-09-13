#!/usr/bin/env python3
"""
Timezone Utilities for Enhanced Timing Precision
- IST conversion functions
- Analysis timing validation
- Candle close time calculations
"""

from datetime import datetime, timedelta
import pytz

def get_ist_time():
    """Get current time in IST"""
    return datetime.utcnow() + timedelta(hours=5, minutes=30)

def get_utc_time():
    """Get current UTC time"""
    return datetime.utcnow()

def convert_utc_to_ist(utc_datetime):
    """Convert UTC datetime to IST"""
    if isinstance(utc_datetime, str):
        utc_datetime = datetime.fromisoformat(utc_datetime)
    
    return utc_datetime + timedelta(hours=5, minutes=30)

def is_analysis_time_valid(timeframe, current_time=None):
    """
    Validate if current time is appropriate for analysis
    Enhanced timing validation for different timeframes
    """
    if current_time is None:
        current_time = get_ist_time()
    
    current_minute = current_time.minute
    current_hour = current_time.hour
    
    if timeframe == '15m':
        # 15-minute candles close at :00, :15, :30, :45
        # Analysis should run at :01, :16, :31, :46 (1 minute after)
        expected_minutes = [1, 16, 31, 46]
        if current_minute in expected_minutes:
            return True, f"Perfect timing for 15m analysis (:{current_minute:02d})"
        elif current_minute in [3, 6, 18, 21, 33, 36, 48, 51]:
            return True, f"Backup timing for 15m analysis (:{current_minute:02d})"
        else:
            return True, f"Analysis allowed but non-optimal timing (:{current_minute:02d})"
    
    elif timeframe == '6h':
        # 6-hour candles close at 00:00, 06:00, 12:00, 18:00 UTC
        # In IST: 05:30, 11:30, 17:30, 23:30
        # Analysis should run around :01 and :04 after these times
        if current_hour in [5, 11, 17, 23] and current_minute in [31, 34]:
            return True, f"Perfect timing for 6h analysis ({current_hour:02d}:{current_minute:02d})"
        else:
            return True, f"6h analysis allowed (current: {current_hour:02d}:{current_minute:02d})"
    
    elif timeframe == '8h':
        # 8-hour candles close at 00:00, 08:00, 16:00 UTC  
        # In IST: 05:30, 13:30, 21:30
        # Analysis should run around :01 and :04 after these times
        if current_hour in [5, 13, 21] and current_minute in [31, 34]:
            return True, f"Perfect timing for 8h analysis ({current_hour:02d}:{current_minute:02d})"
        else:
            return True, f"8h analysis allowed (current: {current_hour:02d}:{current_minute:02d})"
    
    elif timeframe == '12h':
        # 12-hour candles close at 00:00, 12:00 UTC
        # In IST: 05:30, 17:30
        # Analysis should run around :01 and :04 after these times
        if current_hour in [5, 17] and current_minute in [31, 34]:
            return True, f"Perfect timing for 12h analysis ({current_hour:02d}:{current_minute:02d})"
        else:
            return True, f"12h analysis allowed (current: {current_hour:02d}:{current_minute:02d})"
    
    else:
        return True, f"Unknown timeframe {timeframe}, analysis allowed"

def calculate_next_candle_close(timeframe):
    """Calculate when the next candle will close"""
    current_utc = get_utc_time()
    
    if timeframe == '15m':
        # Find next 15-minute boundary
        next_close = current_utc.replace(second=0, microsecond=0)
        minutes_to_add = 15 - (next_close.minute % 15)
        if minutes_to_add == 15:  # Already at boundary
            minutes_to_add = 0
        next_close += timedelta(minutes=minutes_to_add)
    
    elif timeframe == '6h':
        # Find next 6-hour boundary (00:00, 06:00, 12:00, 18:00)
        next_close = current_utc.replace(minute=0, second=0, microsecond=0)
        hours_to_add = 6 - (next_close.hour % 6)
        if hours_to_add == 6:
            hours_to_add = 0
        next_close += timedelta(hours=hours_to_add)
    
    elif timeframe == '8h':
        # Find next 8-hour boundary (00:00, 08:00, 16:00)
        next_close = current_utc.replace(minute=0, second=0, microsecond=0)
        hours_to_add = 8 - (next_close.hour % 8)
        if hours_to_add == 8:
            hours_to_add = 0
        next_close += timedelta(hours=hours_to_add)
    
    elif timeframe == '12h':
        # Find next 12-hour boundary (00:00, 12:00)
        next_close = current_utc.replace(minute=0, second=0, microsecond=0)
        hours_to_add = 12 - (next_close.hour % 12)
        if hours_to_add == 12:
            hours_to_add = 0
        next_close += timedelta(hours=hours_to_add)
    
    else:
        return None
    
    return convert_utc_to_ist(next_close)

def format_time_until_next_analysis(timeframe):
    """Format time remaining until next expected analysis"""
    next_close = calculate_next_candle_close(timeframe)
    if not next_close:
        return "Unknown timeframe"
    
    # Analysis runs 1 minute after candle close
    next_analysis = next_close + timedelta(minutes=1)
    current_ist = get_ist_time()
    
    time_diff = next_analysis - current_ist
    
    if time_diff.total_seconds() < 0:
        return "Analysis time has passed"
    
    hours = int(time_diff.total_seconds() // 3600)
    minutes = int((time_diff.total_seconds() % 3600) // 60)
    
    if hours > 0:
        return f"{hours}h {minutes}m"
    else:
        return f"{minutes}m"
