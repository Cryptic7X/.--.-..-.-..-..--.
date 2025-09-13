#!/usr/bin/env python3
"""
Advanced Deduplication Systems for Both Trading Systems
- System 1: 4-hour cooldown with immediate opposite direction
- System 2: Multi-timeframe suppression with cascading logic
"""

import os
import json
from datetime import datetime, timedelta

class SignalDeduplicator:
    """
    Deduplication for 15-minute CipherB + CTO system
    4-hour cooldown for same direction, immediate for opposite
    """
    
    def __init__(self, cache_file, cooldown_hours=4):
        self.cache_file = cache_file
        self.cooldown_hours = cooldown_hours
        self.cache = self.load_cache()
    
    def load_cache(self):
        """Load deduplication cache from file"""
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'r') as f:
                    data = json.load(f)
                # Convert string timestamps back to datetime objects
                cache = {}
                for key, value in data.items():
                    cache[key] = datetime.fromisoformat(value)
                return cache
            return {}
        except Exception as e:
            print(f"⚠️ Error loading deduplication cache: {e}")
            return {}
    
    def save_cache(self):
        """Save deduplication cache to file"""
        try:
            os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
            # Convert datetime objects to strings for JSON serialization
            data = {}
            for key, value in self.cache.items():
                data[key] = value.isoformat()
            
            with open(self.cache_file, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            print(f"⚠️ Error saving deduplication cache: {e}")
    
    def is_signal_allowed(self, symbol, signal_type, timestamp):
        """
        Check if signal is allowed based on cooldown rules
        Returns True if signal should be sent
        """
        if not isinstance(timestamp, datetime):
            timestamp = datetime.fromisoformat(str(timestamp))
        
        current_time = datetime.utcnow()
        
        # Check if signal is fresh (within reasonable time)
        signal_age = current_time - timestamp
        if signal_age > timedelta(hours=1):  # Signal too old
            print(f"⏰ {symbol} {signal_type}: Signal too old ({signal_age})")
            return False
        
        # Check cooldown for same direction
        same_direction_key = f"{symbol}_{signal_type}"
        if same_direction_key in self.cache:
            last_signal_time = self.cache[same_direction_key]
            time_since_last = current_time - last_signal_time
            
            if time_since_last < timedelta(hours=self.cooldown_hours):
                remaining = timedelta(hours=self.cooldown_hours) - time_since_last
                print(f"🔄 {symbol} {signal_type}: Cooldown active ({remaining} remaining)")
                return False
        
        # Check for opposite direction (always allowed immediately)
        opposite_type = 'SELL' if signal_type == 'BUY' else 'BUY'
        opposite_key = f"{symbol}_{opposite_type}"
        if opposite_key in self.cache:
            # Clear opposite direction when new signal appears
            del self.cache[opposite_key]
            print(f"🔄 {symbol}: Opposite direction detected, clearing {opposite_type} cooldown")
        
        # Record this signal
        self.cache[same_direction_key] = current_time
        self.save_cache()
        
        return True
    
    def cleanup_old_records(self):
        """Remove old records to keep cache clean"""
        cutoff_time = datetime.utcnow() - timedelta(hours=self.cooldown_hours * 2)
        
        keys_to_remove = [
            key for key, timestamp in self.cache.items()
            if timestamp < cutoff_time
        ]
        
        for key in keys_to_remove:
            del self.cache[key]
        
        if keys_to_remove:
            self.save_cache()
            print(f"🧹 Cleaned up {len(keys_to_remove)} old deduplication records")

class MultiTimeframeSuppressionManager:
    """
    Advanced suppression for multi-timeframe system
    Implements cascading logic: 6h → 8h → 12h
    """
    
    def __init__(self, state_file, cascade_order=['6h', '8h', '12h']):
        self.state_file = state_file
        self.cascade_order = cascade_order
        self.suppression_states = self.load_state()
    
    def load_state(self):
        """Load suppression state from file"""
        try:
            if os.path.exists(self.state_file):
                with open(self.state_file, 'r') as f:
                    return json.load(f)
            return {}
        except Exception as e:
            print(f"⚠️ Error loading suppression state: {e}")
            return {}
    
    def save_state(self):
        """Save suppression state to file"""
        try:
            os.makedirs(os.path.dirname(self.state_file), exist_ok=True)
            with open(self.state_file, 'w') as f:
                json.dump(self.suppression_states, f, indent=2)
        except Exception as e:
            print(f"⚠️ Error saving suppression state: {e}")
    
    def get_state_key(self, symbol, signal_type):
        """Generate unique key for suppression state"""
        return f"{symbol}_{signal_type}"
    
    def should_allow_signal(self, symbol, signal_type, timeframe, timestamp):
        """
        Determine if signal should be allowed based on complex suppression logic
        """
        if not isinstance(timestamp, datetime):
            timestamp = datetime.fromisoformat(str(timestamp))
        
        state_key = self.get_state_key(symbol, signal_type)
        current_state = self.suppression_states.get(state_key, None)
        current_timestamp = timestamp.timestamp()
        
        # No previous state - allow signal
        if not current_state:
            return {
                'allowed': True,
                'reason': 'No previous suppression state',
                'action': 'allow_initial'
            }
        
        last_timeframe = current_state['timeframe']
        last_timestamp = current_state['timestamp']
        
        # Calculate time since last signal
        time_diff = current_timestamp - last_timestamp
        suppression_window = 72 * 3600  # 72 hours in seconds
        
        # If suppression window has expired, allow signal
        if time_diff > suppression_window:
            return {
                'allowed': True,
                'reason': f'Suppression window expired ({time_diff/3600:.1f}h > 72h)',
                'action': 'allow_expired'
            }
        
        # Get cascade positions
        try:
            current_tf_index = self.cascade_order.index(timeframe)
            last_tf_index = self.cascade_order.index(last_timeframe)
        except ValueError:
            # Unknown timeframe, allow by default
            return {
                'allowed': True,
                'reason': 'Unknown timeframe in cascade',
                'action': 'allow_unknown'
            }
        
        # Same timeframe - suppress if within window
        if timeframe == last_timeframe:
            return {
                'allowed': False,
                'reason': f'Same timeframe {timeframe} suppressed (within {time_diff/3600:.1f}h)',
                'action': 'suppress_same_tf'
            }
        
        # Lower timeframe trying to signal after higher timeframe
        if current_tf_index < last_tf_index:
            return {
                'allowed': False,
                'reason': f'Lower timeframe {timeframe} suppressed by active {last_timeframe}',
                'action': 'suppress_lower_tf'
            }
        
        # Higher timeframe - allow (this is escalation)
        if current_tf_index > last_tf_index:
            return {
                'allowed': True,
                'reason': f'Timeframe escalation: {last_timeframe} → {timeframe}',
                'action': 'allow_escalation'
            }
        
        # Default allow
        return {
            'allowed': True,
            'reason': 'Default allow',
            'action': 'allow_default'
        }
    
    def record_signal(self, symbol, signal_type, timeframe, timestamp):
        """Record signal in suppression state"""
        if not isinstance(timestamp, datetime):
            timestamp = datetime.fromisoformat(str(timestamp))
        
        state_key = self.get_state_key(symbol, signal_type)
        
        self.suppression_states[state_key] = {
            'timeframe': timeframe,
            'timestamp': timestamp.timestamp(),
            'recorded_at': datetime.utcnow().timestamp()
        }
        
        self.save_state()
        print(f"📝 Recorded {symbol} {signal_type} suppression state: {timeframe}")
        
        # Check for opposite direction reset
        opposite_type = 'SELL' if signal_type == 'BUY' else 'BUY'
        opposite_key = self.get_state_key(symbol, opposite_type)
        
        if opposite_key in self.suppression_states:
            del self.suppression_states[opposite_key]
            self.save_state()
            print(f"🔄 Reset {symbol} {opposite_type} suppression (opposite direction)")
    
    def cleanup_old_states(self):
        """Remove old suppression states"""
        current_time = datetime.utcnow().timestamp()
        cleanup_threshold = 7 * 24 * 3600  # 7 days in seconds
        
        keys_to_remove = []
        for key, state in self.suppression_states.items():
            if current_time - state['timestamp'] > cleanup_threshold:
                keys_to_remove.append(key)
        
        for key in keys_to_remove:
            del self.suppression_states[key]
        
        if keys_to_remove:
            self.save_state()
            print(f"🧹 Cleaned up {len(keys_to_remove)} old suppression states")
