"""
Signal Freshness Utility for Multi-Timeframe Analysis
Checks if signals are fresh enough for each timeframe
"""

from datetime import datetime, timezone

def is_signal_fresh(signal_time_utc, timeframe):
    """
    Check if signal is fresh based on timeframe requirements
    
    Args:
        signal_time_utc: Signal timestamp (datetime object)
        timeframe: Timeframe string ('15m', '30m', '6h', '8h', '12h')
    
    Returns:
        bool: True if signal is fresh enough for the timeframe
    """
    now_utc = datetime.now(timezone.utc)
    
    # Timeframe-specific freshness limits
    freshness_limits = {
        # '15m': 15,    # 15 minutes for 15m timeframe
        '30m': 30,    # 30 minutes for 30m timeframe
        '6h': 360,    # 6 hours (360 minutes) for 6h timeframe
        '8h': 480,    # 8 hours (480 minutes) for 8h timeframe
        '12h': 720,   # 12 hours (720 minutes) for 12h timeframe
    }
    
    max_age_minutes = freshness_limits.get(timeframe, 30)
    
    # Ensure signal_time_utc has timezone info
    if signal_time_utc.tzinfo is None:
        signal_time_utc = signal_time_utc.replace(tzinfo=timezone.utc)
    
    signal_age_minutes = (now_utc - signal_time_utc).total_seconds() / 60
    
    return signal_age_minutes <= max_age_minutes

def get_signal_age_display(signal_time_utc, timeframe):
    """
    Get human-readable signal age for logging
    
    Args:
        signal_time_utc: Signal timestamp (datetime object)
        timeframe: Timeframe string
    
    Returns:
        str: Formatted age string (e.g., "5.2m ago" or "2.1h ago")
    """
    now_utc = datetime.now(timezone.utc)
    
    if signal_time_utc.tzinfo is None:
        signal_time_utc = signal_time_utc.replace(tzinfo=timezone.utc)
    
    age_seconds = (now_utc - signal_time_utc).total_seconds()
    age_minutes = age_seconds / 60
    
    if timeframe in ['15m', '30m']:
        return f"{age_minutes:.1f}m ago"
    else:
        age_hours = age_minutes / 60
        return f"{age_hours:.1f}h ago"
