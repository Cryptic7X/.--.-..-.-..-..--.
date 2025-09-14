"""
Deduplication System for 30-Minute CipherB + CTO Alerts
4-hour cooldown per symbol per signal type
"""

import json
import os
from datetime import datetime, timedelta

class Deduplicator30m:
    def __init__(self):
        self.cache_file = os.path.join(os.path.dirname(__file__), '..', '..', 'cache', 'deduplication_30m.json')
        self.signal_history = self.load_history()
        self.cooldown_hours = 4  # 4-hour cooldown
    
    def load_history(self):
        """Load signal history from cache"""
        try:
            with open(self.cache_file, 'r') as f:
                history = json.load(f)
            print(f"📁 Loaded 30m signal history: {len(history)} entries")
            return history
        except (FileNotFoundError, json.JSONDecodeError):
            print("📁 Starting fresh 30m signal history")
            return {}
    
    def save_history(self):
        """Save signal history to cache"""
        os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
        with open(self.cache_file, 'w') as f:
            json.dump(self.signal_history, f, indent=2, default=str)
    
    def is_signal_allowed(self, symbol, signal_type, signal_timestamp):
        """Check if signal should be allowed based on 4-hour cooldown"""
        current_time = datetime.utcnow()
        
        # Convert signal timestamp to datetime if needed
        if isinstance(signal_timestamp, str):
            signal_timestamp = datetime.fromisoformat(signal_timestamp.replace('Z', '+00:00'))
        
        # Create unique key for symbol + signal type
        signal_key = f"{symbol}_{signal_type}"
        
        # Check if we have previous signal
        if signal_key in self.signal_history:
            last_signal = self.signal_history[signal_key]
            last_time = datetime.fromisoformat(last_signal['timestamp'])
            
            # Calculate time since last signal
            time_since_last = current_time - last_time
            
            # If within cooldown period, suppress
            if time_since_last < timedelta(hours=self.cooldown_hours):
                remaining = timedelta(hours=self.cooldown_hours) - time_since_last
                hours_remaining = remaining.total_seconds() / 3600
                print(f"🔕 {symbol} {signal_type}: Suppressed ({hours_remaining:.1f}h cooldown remaining)")
                return False
        
        # Signal is allowed - record it
        self.signal_history[signal_key] = {
            'symbol': symbol,
            'signal_type': signal_type,
            'timestamp': signal_timestamp.isoformat(),
            'recorded_at': current_time.isoformat()
        }
        
        self.save_history()
        print(f"✅ {symbol} {signal_type}: Allowed (4h cooldown started)")
        return True
    
    def cleanup_old_signals(self):
        """Remove signals older than 24 hours"""
        cutoff_time = datetime.utcnow() - timedelta(hours=24)
        old_keys = []
        
        for key, signal_data in self.signal_history.items():
            try:
                signal_time = datetime.fromisoformat(signal_data['timestamp'])
                if signal_time < cutoff_time:
                    old_keys.append(key)
            except (ValueError, TypeError, KeyError):
                old_keys.append(key)  # Remove invalid entries
        
        # Remove old signals
        for key in old_keys:
            del self.signal_history[key]
        
        if old_keys:
            self.save_history()
            print(f"🧹 Cleaned {len(old_keys)} old 30m signal records")
    
    def get_status(self):
        """Get deduplication status"""
        active_signals = len(self.signal_history)
        return {
            'active_cooldowns': active_signals,
            'cooldown_hours': self.cooldown_hours
        }
