"""
Signal Freshness Utility for Multi-Timeframe Analysis
Checks if signals are fresh enough for each timeframe with GitHub Actions delay compensation
"""

from datetime import datetime, timezone

def is_signal_fresh(signal_time_utc, timeframe):
    """
    Check if signal is fresh based on timeframe requirements
    
    Args:
        signal_time_utc: Signal timestamp (datetime object)
        timeframe: Timeframe string ('30m', '6h', '8h', '12h')
    
    Returns:
        bool: True if signal is fresh enough for the timeframe
    """
    now_utc = datetime.now(timezone.utc)
    
    # Expanded freshness limits to compensate for GitHub Actions delays
    freshness_limits = {
        '30m': 45,    # 45 minutes (was 30m) - covers execution delays
        '6h': 420,    # 7 hours (was 6h) - handles scheduling variance  
        '8h': 540,    # 9 hours (was 8h) - accommodates peak delays
        '12h': 780,   # 13 hours (was 12h) - ensures coverage near market open
    }
    
    max_age_minutes = freshness_limits.get(timeframe, 45)
    
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
    
    if timeframe == '30m':
        return f"{age_minutes:.1f}m ago"
    else:
        age_hours = age_minutes / 60
        return f"{age_hours:.1f}h ago"

def get_freshness_status():
    """Get current freshness window settings for debugging"""
    return {
        '30m': '45 minutes',
        '6h': '7 hours',
        '8h': '9 hours', 
        '12h': '13 hours'
    }

