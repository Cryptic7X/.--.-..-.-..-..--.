"""
Dynamic EMA Indicator - Supports 12/21 and 21/50 EMA Analysis
Automatically switches between configurations based on YAML config
"""

import json
import os
import time
import yaml
from datetime import datetime
from typing import Dict, List, Tuple, Optional

class DynamicEMAIndicator:
    def __init__(self):
        self.config = self.load_config()
        self.active_analysis = self.get_active_analysis()
        self.cache_file = f"cache/ema_{self.active_analysis['ema_short']}_{self.active_analysis['ema_long']}_alerts.json"
        self.last_config_check = 0
        self.config_check_interval = 60  # Check config every 60 seconds

    def load_config(self) -> Dict:
        """Load YAML configuration file"""
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'ema_config.yaml')
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            return config
        except Exception as e:
            print(f"❌ Config load error: {e}")
            # Return default 21/50 configuration
            return {
                'analysis': {
                    'ema_21_50': {
                        'enabled': True,
                        'ema_short': 21,
                        'ema_long': 50,
                        'cooldown_hours': 12,
                        'description': 'Default fallback'
                    }
                },
                'system': {
                    'timeframe': '15m',
                    'freshness_minutes': 30,
                    'data_limit': 200
                }
            }

    def get_active_analysis(self) -> Optional[Dict]:
        """Get the currently active analysis configuration"""
        for name, analysis in self.config.get('analysis', {}).items():
            if analysis.get('enabled', False):
                analysis['name'] = name
                print(f"✅ Active Analysis: {analysis['ema_short']}/{analysis['ema_long']} EMA")
                print(f"📋 Description: {analysis.get('description', 'No description')}")
                return analysis

        # No enabled analysis found - use last valid or default to 21/50
        print("⚠️ No enabled analysis found, using default 21/50")
        return {
            'name': 'ema_21_50',
            'ema_short': 21,
            'ema_long': 50,
            'cooldown_hours': 12,
            'description': 'Default fallback'
        }

    def check_config_updates(self):
        """Check if config file has been updated"""
        current_time = time.time()
        if current_time - self.last_config_check > self.config_check_interval:
            old_config = self.active_analysis
            self.config = self.load_config()
            new_config = self.get_active_analysis()

            if (old_config['ema_short'] != new_config['ema_short'] or 
                old_config['ema_long'] != new_config['ema_long']):
                print(f"🔄 Config changed: {old_config['ema_short']}/{old_config['ema_long']} → {new_config['ema_short']}/{new_config['ema_long']}")
                self.active_analysis = new_config
                self.cache_file = f"cache/ema_{new_config['ema_short']}_{new_config['ema_long']}_alerts.json"

            self.last_config_check = current_time

    def calculate_ema(self, data: List[float], period: int) -> List[float]:
        """Calculate EMA with high accuracy"""
        if len(data) < period:
            return [0] * len(data)

        ema_values = []
        multiplier = 2 / (period + 1)

        # Start with SMA for first EMA value
        sma = sum(data[:period]) / period
        ema_values.extend([0] * (period - 1))
        ema_values.append(sma)

        # Calculate EMA for the rest
        for i in range(period, len(data)):
            ema = (data[i] * multiplier) + (ema_values[i-1] * (1 - multiplier))
            ema_values.append(ema)

        return ema_values

    def load_cache(self) -> Dict:
        """Load alert cache for current analysis"""
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'r') as f:
                    return json.load(f)
        except Exception as e:
            print(f"⚠️ Cache load error: {e}")
        return {}

    def save_cache(self, cache_data: Dict):
        """Save alert cache for current analysis"""
        try:
            os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
            with open(self.cache_file, 'w') as f:
                json.dump(cache_data, f, indent=2)
        except Exception as e:
            print(f"❌ Cache save error: {e}")

    def detect_crossover(self, ema_short: List[float], ema_long: List[float]) -> Optional[str]:
        """Detect EMA crossover (generic for any EMA pair)"""
        if len(ema_short) < 2 or len(ema_long) < 2:
            return None

        # Get previous and current values
        prev_short, curr_short = ema_short[-2], ema_short[-1]
        prev_long, curr_long = ema_long[-2], ema_long[-1]

        # Golden Cross: Short EMA crosses above Long EMA
        if prev_short <= prev_long and curr_short > curr_long:
            return 'golden_cross'

        # Death Cross: Short EMA crosses below Long EMA
        if prev_short >= prev_long and curr_short < curr_long:
            return 'death_cross'

        return None

    def is_fresh_signal(self, timestamps: List[int]) -> bool:
        """Check if signal is within freshness window"""
        if not timestamps:
            return False

        freshness_minutes = self.config.get('system', {}).get('freshness_minutes', 30)
        current_time = int(time.time())
        latest_candle_time = timestamps[-1] / 1000  # Convert to seconds

        # Check if latest candle is within freshness window
        time_diff_minutes = (current_time - latest_candle_time) / 60
        return time_diff_minutes <= freshness_minutes

    def analyze(self, ohlcv_data: Dict, symbol: str) -> Dict:
        """Analyze EMA crossover with dynamic configuration"""
        try:
            # Check for config updates
            self.check_config_updates()

            closes = ohlcv_data['close']
            timestamps = ohlcv_data['timestamp']

            # Require sufficient data
            data_limit = self.config.get('system', {}).get('data_limit', 200)
            if len(closes) < max(50, self.active_analysis['ema_long']):
                return {'crossover_alert': False, 'reason': 'insufficient_data'}

            # Check freshness
            if not self.is_fresh_signal(timestamps):
                return {'crossover_alert': False, 'reason': 'stale_data'}

            # Calculate EMAs with dynamic periods
            ema_short = self.calculate_ema(closes, self.active_analysis['ema_short'])
            ema_long = self.calculate_ema(closes, self.active_analysis['ema_long'])

            # Detect crossover
            crossover_type = self.detect_crossover(ema_short, ema_long)
            if not crossover_type:
                return {'crossover_alert': False, 'reason': 'no_crossover'}

            # Check cooldown
            current_time = time.time()
            cache = self.load_cache()
            cooldown_hours = self.active_analysis['cooldown_hours']

            if symbol in cache:
                last_alert_time = cache[symbol].get('last_alert_time', 0)
                hours_since = (current_time - last_alert_time) / 3600

                if hours_since < cooldown_hours:
                    return {'crossover_alert': False, 'reason': 'cooldown_active'}

            # Valid crossover - update cache
            cache[symbol] = {'last_alert_time': current_time}
            self.save_cache(cache)

            return {
                'crossover_alert': True,
                'crossover_type': crossover_type,
                'current_price': closes[-1],
                'ema_short_period': self.active_analysis['ema_short'],
                'ema_long_period': self.active_analysis['ema_long'],
                'ema_short_value': ema_short[-1],
                'ema_long_value': ema_long[-1],
                'reason': 'valid_crossover'
            }

        except Exception as e:
            print(f"❌ EMA analysis error for {symbol}: {e}")
            return {'crossover_alert': False, 'reason': 'analysis_error'}
