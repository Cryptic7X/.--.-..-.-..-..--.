"""
EMA 15M Indicator - 21/50 EMA CROSSOVER ANALYSIS
Optimized for 15-minute timeframe with 12-hour cooldown
"""

import json
import os
import time
from datetime import datetime
from typing import Dict, List

class EMA15MIndicator:
    def __init__(self):
        self.ema_short = 21  
        self.ema_long = 50   
        self.cache_file = "cache/ema15m_alerts.json"
        self.cooldown_hours = 12
        self.freshness_minutes = 30

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
        """Load alert cache"""
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'r') as f:
                    return json.load(f)
        except Exception as e:
            print(f"⚠️ Cache load error: {e}")
        return {}

    def save_cache(self, cache_data: Dict):
        """Save alert cache"""
        try:
            os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
            with open(self.cache_file, 'w') as f:
                json.dump(cache_data, f, indent=2)
        except Exception as e:
            print(f"❌ Cache save error: {e}")

    def detect_crossover(self, ema21: List[float], ema50: List[float]) -> str:
        """Detect 21/50 EMA crossover"""
        if len(ema21) < 2 or len(ema50) < 2:
            return None

        # Get previous and current values
        prev_21, curr_21 = ema21[-2], ema21[-1]
        prev_50, curr_50 = ema50[-2], ema50[-1]

        # Golden Cross: 21 EMA crosses above 50 EMA
        if prev_21 <= prev_50 and curr_21 > curr_50:
            return 'golden_cross'

        # Death Cross: 21 EMA crosses below 50 EMA
        if prev_21 >= prev_50 and curr_21 < curr_50:
            return 'death_cross'

        return None

    def is_fresh_signal(self, timestamps: List[int]) -> bool:
        """Check if signal is within 30-minute freshness window"""
        if not timestamps:
            return False

        current_time = int(time.time())
        latest_candle_time = timestamps[-1] / 1000  # Convert to seconds

        # Check if latest candle is within 30 minutes
        time_diff_minutes = (current_time - latest_candle_time) / 60
        return time_diff_minutes <= self.freshness_minutes

    def analyze(self, ohlcv_data: Dict, symbol: str) -> Dict:
        """Analyze 21/50 EMA crossover with freshness and cooldown"""
        try:
            closes = ohlcv_data['close']
            timestamps = ohlcv_data['timestamp']

            # Require sufficient data
            if len(closes) < 100:
                return {'crossover_alert': False, 'reason': 'insufficient_data'}

            # Check freshness
            if not self.is_fresh_signal(timestamps):
                return {'crossover_alert': False, 'reason': 'stale_data'}

            # Calculate EMAs
            ema21 = self.calculate_ema(closes, self.ema_short)
            ema50 = self.calculate_ema(closes, self.ema_long)

            # Detect crossover
            crossover_type = self.detect_crossover(ema21, ema50)
            if not crossover_type:
                return {'crossover_alert': False, 'reason': 'no_crossover'}

            # Check cooldown
            current_time = time.time()
            cache = self.load_cache()

            if symbol in cache:
                last_alert_time = cache[symbol].get('last_alert_time', 0)
                hours_since = (current_time - last_alert_time) / 3600

                if hours_since < self.cooldown_hours:
                    return {'crossover_alert': False, 'reason': 'cooldown_active'}

            # Valid crossover - update cache
            cache[symbol] = {'last_alert_time': current_time}
            self.save_cache(cache)

            return {
                'crossover_alert': True,
                'crossover_type': crossover_type,
                'current_price': closes[-1],
                'reason': 'valid_crossover'
            }

        except Exception as e:
            print(f"❌ EMA analysis error for {symbol}: {e}")
            return {'crossover_alert': False, 'reason': 'analysis_error'}
