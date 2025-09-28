"""
EMA 12/21 Crossover Indicator
6-hour cooldown per symbol
"""

import json
import os
import time
from typing import Dict, List

class EMA12_21Indicator:
    def __init__(self):
        self.ema_short = 12
        self.ema_long = 21
        self.cache_file = "cache/ema_12_21_alerts.json"
        self.crossover_cooldown_hours = 6  # 6-hour cooldown

    def calculate_ema(self, data: List[float], period: int) -> List[float]:
        """Calculate EMA with high accuracy"""
        if len(data) < period:
            return [0] * len(data)

        ema_values = []
        multiplier = 2 / (period + 1)

        # Start with SMA for the first EMA value
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
            print(f"❌ Cache load error: {e}")
        return {}

    def save_cache(self, cache_data: Dict):
        """Save alert cache"""
        try:
            os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
            with open(self.cache_file, 'w') as f:
                json.dump(cache_data, f, indent=2)
        except Exception as e:
            print(f"❌ Cache save error: {e}")

    def detect_crossover(self, ema12: List[float], ema21: List[float]) -> str:
        """Detect 12/21 EMA crossover"""
        if len(ema12) < 2 or len(ema21) < 2:
            return None

        # Get previous and current values
        prev_12, curr_12 = ema12[-2], ema12[-1]
        prev_21, curr_21 = ema21[-2], ema21[-1]

        # Golden Cross: 12 EMA crosses above 21 EMA
        if prev_12 <= prev_21 and curr_12 > curr_21:
            return 'golden_cross'

        # Death Cross: 12 EMA crosses below 21 EMA
        if prev_12 >= prev_21 and curr_12 < curr_21:
            return 'death_cross'

        return None

    def analyze(self, ohlcv_data: Dict, symbol: str) -> Dict:
        """Analyze 12/21 EMA crossover with 6-hour cooldown"""
        try:
            closes = ohlcv_data['close']

            # Need sufficient data for accurate EMA calculation
            if len(closes) < 50:
                return {'crossover_alert': False}

            ema12 = self.calculate_ema(closes, self.ema_short)
            ema21 = self.calculate_ema(closes, self.ema_long)

            current_time = time.time()
            cache = self.load_cache()
            cache_updated = False

            # Check for crossover
            crossover_type = self.detect_crossover(ema12, ema21)
            crossover_alert = False

            if crossover_type:
                cache_key = symbol

                if cache_key in cache:
                    last_time = cache[cache_key].get('last_alert_time', 0)
                    hours_since = (current_time - last_time) / 3600

                    # 6-hour cooldown check
                    if hours_since >= self.crossover_cooldown_hours:
                        crossover_alert = True
                        cache[cache_key] = {'last_alert_time': current_time}
                        cache_updated = True
                else:
                    # First crossover for this symbol - immediate alert allowed
                    crossover_alert = True
                    cache[cache_key] = {'last_alert_time': current_time}
                    cache_updated = True

            # Save cache if updated
            if cache_updated:
                self.save_cache(cache)

            # Return analysis result
            return {
                'crossover_alert': crossover_alert,
                'crossover_type': crossover_type if crossover_alert else None,
                'current_price': closes[-1]
            }

        except Exception as e:
            print(f"❌ EMA analysis error for {symbol}: {e}")
            return {'crossover_alert': False}
