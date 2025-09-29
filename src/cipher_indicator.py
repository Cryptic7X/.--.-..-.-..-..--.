"""
CipherB 15M Indicator - 100% TradingView Pine Script Match
Exact replication of plot shape buy/sell signals only
"""

import pandas as pd
import numpy as np
import json
import os
import time
import yaml
from datetime import datetime
from typing import Dict, List, Optional, Tuple

class CipherB15MIndicator:
    def __init__(self):
        self.config = self.load_config()
        self.cache_file = "cache/cipher_b_alerts.json"
        
    def load_config(self) -> Dict:
        """Load CipherB configuration"""
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'cipher_config.yaml')
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            print(f"❌ Config load error: {e}")
            # Return default config matching your Pine Script
            return {
                'cipher_b': {
                    'timeframe': '15m',
                    'freshness_minutes': 15,
                    'data_limit': 200,
                    'wt_channel_len': 9,
                    'wt_average_len': 12,
                    'wt_ma_len': 3,
                    'os_level': -60,
                    'ob_level': 60
                },
                'system': {'max_workers': 10}
            }
    
    def ema(self, series: pd.Series, length: int) -> pd.Series:
        """Exponential Moving Average - matches Pine Script ta.ema()"""
        return series.ewm(span=length, adjust=False).mean()
    
    def sma(self, series: pd.Series, length: int) -> pd.Series:  
        """Simple Moving Average - matches Pine Script ta.sma()"""
        return series.rolling(window=length).mean()
    
    def calculate_wavetrend(self, df: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
        """
        100% EXACT Pine Script f_wavetrend function replication
        Returns (wt1, wt2) matching your TradingView exactly
        """
        config = self.config['cipher_b']
        
        # Your exact Pine Script parameters
        wt_channel_len = config['wt_channel_len']  # 9
        wt_average_len = config['wt_average_len']  # 12
        wt_ma_len = config['wt_ma_len']           # 3
        
        # Calculate HLC3 - your exact: wtMASource = hlc3
        hlc3 = (df['high'] + df['low'] + df['close']) / 3
        
        # YOUR EXACT f_wavetrend function implementation:
        # tfsrc = hlc3
        # esa = ta.ema(tfsrc, wtChannelLen)
        esa = self.ema(hlc3, wt_channel_len)
        
        # d = ta.ema(math.abs(tfsrc - esa), wtChannelLen)
        d = self.ema((hlc3 - esa).abs(), wt_channel_len)
        
        # ci = (tfsrc - esa) / (0.015 * d)
        ci = (hlc3 - esa) / (0.015 * d)
        
        # tci = ta.ema(ci, wtAverageLen)
        tci = self.ema(ci, wt_average_len)
        
        # wt1 = tci
        wt1 = tci
        
        # wt2 = ta.sma(wt1, wtMALen)
        wt2 = self.sma(wt1, wt_ma_len)
        
        return wt1, wt2
    
    def detect_cipher_b_signals(self, df: pd.DataFrame) -> Dict:
        """
        100% EXACT Pine Script buy/sell signal detection
        Only plot shape signals (buySignal and sellSignal)
        """
        if len(df) < 50:
            return {'buy_signal': False, 'sell_signal': False}
        
        config = self.config['cipher_b']
        os_level = config['os_level']  # -60
        ob_level = config['ob_level']  # 60
        
        # Calculate WaveTrend (100% Pine Script match)
        wt1, wt2 = self.calculate_wavetrend(df)
        
        # YOUR EXACT Pine Script buy/sell conditions:
        # buySignal = wt2 <= osLevel2 and wt1 <= wt2 and wt1[1] > wt2[1] and wt1 < osLevel2
        buy_conditions = (
            (wt2 <= os_level) &
            (wt1 <= wt2) & 
            (wt1.shift(1) > wt2.shift(1)) &
            (wt1 < os_level)
        )
        
        # sellSignal = wt2 >= obLevel2 and wt1 >= wt2 and wt1[1] < wt2[1] and wt1 > obLevel2  
        sell_conditions = (
            (wt2 >= ob_level) &
            (wt1 >= wt2) &
            (wt1.shift(1) < wt2.shift(1)) &
            (wt1 > ob_level)
        )
        
        # Get the last signals (most recent candle)
        buy_signal = buy_conditions.iloc[-1] if len(buy_conditions) > 0 else False
        sell_signal = sell_conditions.iloc[-1] if len(sell_conditions) > 0 else False
        
        return {
            'buy_signal': bool(buy_signal),
            'sell_signal': bool(sell_signal),
            'wt1_current': float(wt1.iloc[-1]) if len(wt1) > 0 else 0,
            'wt2_current': float(wt2.iloc[-1]) if len(wt2) > 0 else 0
        }
    
    def load_cache(self) -> Dict:
        """Load direction-based alert cache"""
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'r') as f:
                    return json.load(f)
        except Exception as e:
            print(f"⚠️ Cache load error: {e}")
        return {}
    
    def save_cache(self, cache_data: Dict):
        """Save direction-based alert cache"""
        try:
            os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
            with open(self.cache_file, 'w') as f:
                json.dump(cache_data, f, indent=2)
        except Exception as e:
            print(f"❌ Cache save error: {e}")
    
    def is_fresh_signal(self, timestamps: List[int]) -> bool:
        """Check if signal occurred within 15-minute freshness window"""
        if not timestamps:
            return False
        
        freshness_minutes = self.config['cipher_b']['freshness_minutes']
        current_time = int(time.time())
        latest_candle_time = timestamps[-1] / 1000  # Convert to seconds
        
        # Check if latest candle is within freshness window
        time_diff_minutes = (current_time - latest_candle_time) / 60
        return time_diff_minutes <= freshness_minutes
    
    def analyze(self, ohlcv_data: Dict, symbol: str) -> Dict:
        """
        Analyze CipherB signals with direction-based tracking
        One alert per direction until opposite signal occurs
        """
        try:
            # Require sufficient data
            if len(ohlcv_data['close']) < self.config['cipher_b']['data_limit']:
                return {'signal_alert': False, 'reason': 'insufficient_data'}
            
            # Check freshness
            if not self.is_fresh_signal(ohlcv_data['timestamp']):
                return {'signal_alert': False, 'reason': 'stale_data'}
            
            # Create DataFrame for analysis
            df = pd.DataFrame({
                'high': ohlcv_data['high'],
                'low': ohlcv_data['low'], 
                'close': ohlcv_data['close'],
                'timestamp': ohlcv_data['timestamp']
            })
            
            # Detect CipherB signals (100% Pine Script match)
            signals = self.detect_cipher_b_signals(df)
            
            if not signals['buy_signal'] and not signals['sell_signal']:
                return {'signal_alert': False, 'reason': 'no_signal'}
            
            # Load cache to check last signal direction
            cache = self.load_cache()
            current_time = time.time()
            
            # Determine signal type and check direction tracking
            signal_type = None
            should_alert = False
            
            if signals['buy_signal']:
                # Check if last signal was buy (skip if same direction)
                last_signal = cache.get(symbol, {}).get('last_signal')
                if last_signal != 'buy':
                    signal_type = 'buy'
                    should_alert = True
                    # Update cache with new buy signal
                    cache[symbol] = {
                        'last_signal': 'buy',
                        'last_alert_time': current_time
                    }
            
            elif signals['sell_signal']:
                # Check if last signal was sell (skip if same direction)
                last_signal = cache.get(symbol, {}).get('last_signal')
                if last_signal != 'sell':
                    signal_type = 'sell'
                    should_alert = True
                    # Update cache with new sell signal
                    cache[symbol] = {
                        'last_signal': 'sell',
                        'last_alert_time': current_time
                    }
            
            if should_alert:
                # Save updated cache
                self.save_cache(cache)
                
                return {
                    'signal_alert': True,
                    'signal_type': signal_type,
                    'current_price': ohlcv_data['close'][-1],
                    'wt1_value': signals['wt1_current'],
                    'wt2_value': signals['wt2_current'],
                    'reason': 'valid_signal'
                }
            else:
                return {'signal_alert': False, 'reason': 'same_direction_blocked'}
                
        except Exception as e:
            print(f"❌ CipherB analysis error for {symbol}: {e}")
            return {'signal_alert': False, 'reason': 'analysis_error'}
