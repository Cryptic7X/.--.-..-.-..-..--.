"""
Advanced Suppression System for Multi-Timeframe Analysis
Implements cascading suppression logic across 6h/8h/12h timeframes
"""

import json
import os
from datetime import datetime, timedelta

class MultiTimeframeSuppressor:
    def __init__(self):
        self.cache_file = os.path.join(os.path.dirname(__file__), '..', '..', 'cache', 'suppression_multi.json')
        self.suppression_states = self.load_states()
    
    def load_states(self):
        try:
            with open(self.cache_file, 'r') as f:
                states = json.load(f)
            print(f"📁 Loaded multi-timeframe suppression states: {len(states)} entries")
            return states
        except (FileNotFoundError, json.JSONDecodeError):
            print("📁 Starting fresh multi-timeframe suppression states")
            return {}
    
    def save_states(self):
        os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
        with open(self.cache_file, 'w') as f:
            json.dump(self.suppression_states, f, indent=2, default=str)
    
    def should_alert(self, symbol, signal_type, timeframe, signal_timestamp):
        """
        Advanced suppression logic:
        1. First 6h signal: Always alert
        2. Subsequent 6h same-direction: Suppress, wait for 8h
        3. First 8h same-direction: Alert, suppress subsequent 8h
        4. 12h same-direction: Always alert
        5. Opposite direction on 6h: Reset all suppression
        """
        current_time = datetime.utcnow()
        
        if isinstance(signal_timestamp, str):
            signal_timestamp = datetime.fromisoformat(signal_timestamp.replace('Z', '+00:00'))
        
        # Check for existing suppression state
        state_key = f"{symbol}_{signal_type}"
        
        if state_key not in self.suppression_states:
            # No previous state - allow signal and create state
            self.suppression_states[state_key] = {
                'current_direction': signal_type,
                'last_alerted_timeframe': timeframe,
                'last_alert_time': signal_timestamp.isoformat(),
                'created_at': current_time.isoformat(),
                'suppression_level': 'none'
            }
            self.save_states()
            print(f"✅ {symbol} {signal_type} {timeframe}: Initial signal - allowed")
            return True
        
        state = self.suppression_states[state_key]
        last_direction = state['current_direction']
        
        # Check for opposite direction (resets all suppression)
        opposite_key = f"{symbol}_{'SELL' if signal_type == 'BUY' else 'BUY'}"
        if opposite_key in self.suppression_states:
            # Check if there's a recent opposite signal on 6h
            if self._has_recent_6h_opposite_signal(symbol, signal_type):
                # Reset suppression for this direction
                self.suppression_states[state_key] = {
                    'current_direction': signal_type,
                    'last_alerted_timeframe': timeframe,
                    'last_alert_time': signal_timestamp.isoformat(),
                    'created_at': current_time.isoformat(),
                    'suppression_level': 'none'
                }
                self.save_states()
                print(f"✅ {symbol} {signal_type} {timeframe}: Opposite direction reset - allowed")
                return True
        
        # Apply cascading suppression logic
        last_timeframe = state['last_alerted_timeframe']
        suppression_level = state.get('suppression_level', 'none')
        
        if timeframe == '6h':
            if suppression_level == 'none':
                # First 6h signal already handled above
                pass
            elif suppression_level == '6h_suppressed' or last_timeframe == '6h':
                # Suppress subsequent 6h signals
                print(f"❌ {symbol} {signal_type} 6h: Suppressed - waiting for 8h confirmation")
                return False
            
        elif timeframe == '8h':
            if last_timeframe == '6h' and suppression_level != '8h_suppressed':
                # First 8h signal after 6h suppression - allow
                self.suppression_states[state_key].update({
                    'last_alerted_timeframe': '8h',
                    'last_alert_time': signal_timestamp.isoformat(),
                    'suppression_level': '6h_suppressed'
                })
                self.save_states()
                print(f"✅ {symbol} {signal_type} 8h: First 8h after 6h suppression - allowed")
                return True
            elif last_timeframe == '8h':
                # Suppress subsequent 8h signals
                self.suppression_states[state_key]['suppression_level'] = '8h_suppressed'
                self.save_states()
                print(f"❌ {symbol} {signal_type} 8h: Suppressed - waiting for 12h confirmation")
                return False
            
        elif timeframe == '12h':
            # 12h signals are always allowed (highest timeframe)
            self.suppression_states[state_key].update({
                'last_alerted_timeframe': '12h',
                'last_alert_time': signal_timestamp.isoformat(),
                'suppression_level': '8h_suppressed'
            })
            self.save_states()
            print(f"✅ {symbol} {signal_type} 12h: Highest timeframe - always allowed")
            return True
        
        # Default: allow signal and update state
        self.suppression_states[state_key].update({
            'last_alerted_timeframe': timeframe,
            'last_alert_time': signal_timestamp.isoformat()
        })
        self.save_states()
        print(f"✅ {symbol} {signal_type} {timeframe}: Default allow")
        return True
    
    def _has_recent_6h_opposite_signal(self, symbol, current_signal_type):
        """Check if there's a recent opposite direction signal on 6h timeframe"""
        opposite_type = 'SELL' if current_signal_type == 'BUY' else 'BUY'
        opposite_key = f"{symbol}_{opposite_type}"
        
        if opposite_key not in self.suppression_states:
            return False
        
        opposite_state = self.suppression_states[opposite_key]
        
        # Check if the opposite signal was recent and on 6h timeframe
        try:
            last_alert_time = datetime.fromisoformat(opposite_state['last_alert_time'])
            time_since_opposite = datetime.utcnow() - last_alert_time
            
            # Consider it a reset if opposite signal was within last 24 hours on 6h
            return (opposite_state.get('last_alerted_timeframe') == '6h' and 
                    time_since_opposite < timedelta(hours=24))
        except (ValueError, KeyError):
            return False
    
    def cleanup_old_states(self):
        """Remove suppression states older than 72 hours"""
        cutoff_time = datetime.utcnow() - timedelta(hours=72)
        old_keys = []
        
        for key, state in self.suppression_states.items():
            try:
                created_at = datetime.fromisoformat(state['created_at'])
                if created_at < cutoff_time:
                    old_keys.append(key)
            except (ValueError, TypeError, KeyError):
                old_keys.append(key)  # Remove invalid entries
        
        for key in old_keys:
            del self.suppression_states[key]
        
        if old_keys:
            self.save_states()
            # Fixed: Use string concatenation instead of f-string
            print("🧹 Cleaned " + str(len(old_keys)) + " old multi-timeframe suppression records")
