"""
Deduplication System for 15-Minute CipherB + CTO System
4-hour cooldown for same direction, immediate for opposite
"""

import json
import os
from datetime import datetime, timedelta

class Deduplicator15m:
    def __init__(self):
        self.cache_file = os.path.join(os.path.dirname(__file__), '..', '..', 'cache', 'deduplication_15m.json')
        self.cooldown_hours = 4
        self.signal_cache = self.load_cache()
    
    def load_cache(self):
        try:
            with open(self.cache_file, 'r') as f:
                cache = json.load(f)
            print(f"📁 Loaded 15m deduplication cache: {len(cache)} entries")
            return cache
        except (FileNotFoundError, json.JSONDecodeError):
            print("📁 Starting fresh 15m deduplication cache")
            return {}
    
    def save_cache(self):
        os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
        with open(self.cache_file, 'w') as f:
            json.dump(self.signal_cache, f, indent=2, default=str)
    
    def is_signal_allowed(self, symbol, signal_type, signal_timestamp):
        """
        Check if signal is allowed based on 4-hour cooldown rules
        - Same direction: 4-hour cooldown
        - Opposite direction: immediate (resets cooldown)
        """
        current_time = datetime.utcnow()
        
        if isinstance(signal_timestamp, str):
            signal_timestamp = datetime.fromisoformat(signal_timestamp.replace('Z', '+00:00'))
        
        # Check for recent signals of same symbol
        symbol_key = f"{symbol}_recent"
        
        if symbol_key in self.signal_cache:
            last_signal_data = self.signal_cache[symbol_key]
            last_signal_type = last_signal_data['signal_type']
            last_signal_time = datetime.fromisoformat(last_signal_data['signal_time'])
            
            time_since_last = current_time - last_signal_time
            
            # If same direction and within cooldown period
            if signal_type == last_signal_type and time_since_last < timedelta(hours=self.cooldown_hours):
                remaining_cooldown = timedelta(hours=self.cooldown_hours) - time_since_last
                remaining_minutes = remaining_cooldown.total_seconds() / 60
                print(f"❌ {symbol} {signal_type}: Cooldown active ({remaining_minutes:.0f}m remaining)")
                return False
            
            # If opposite direction, allow immediately (resets cooldown)
            if signal_type != last_signal_type:
                print(f"✅ {symbol} {signal_type}: Opposite direction - cooldown reset")
        
        # Record this signal
        self.signal_cache[symbol_key] = {
            'signal_type': signal_type,
            'signal_time': signal_timestamp.isoformat(),
            'alerted_at': current_time.isoformat()
        }
        
        self.save_cache()
        print(f"✅ {symbol} {signal_type}: Signal allowed")
        return True
    
    def cleanup_old_signals(self):
        """Remove signals older than 24 hours"""
        cutoff_time = datetime.utcnow() - timedelta(hours=24)
        old_keys = []
        
        for key, data in self.signal_cache.items():
            try:
                alerted_at = datetime.fromisoformat(data['alerted_at'])
                if alerted_at < cutoff_time:
                    old_keys.append(key)
            except (ValueError, TypeError, KeyError):
                old_keys.append(key)  # Remove invalid entries
        
        for key in old_keys:
            del self.signal_cache[key]
        
        if old_keys:
            self.save_cache()
            print(f"🧹 Cleaned {len(old_keys)} old 15m deduplication records")
