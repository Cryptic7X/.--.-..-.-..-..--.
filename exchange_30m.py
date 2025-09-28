"""
Simplified Exchange Manager - 30M Timeframe Only
For Standalone EMA 12/21 Crossover System
"""

import os
import json
import time
import requests
from datetime import datetime, timedelta
from typing import Optional, Tuple, Dict, Any

class SimpleExchange30M:
    def __init__(self):
        self.session = self.create_session()

    def create_session(self):
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'EMA-30M-Tracker/1.0',
            'Accept': 'application/json'
        })
        return session

    def fetch_bingx_perpetuals_data(self, symbol: str, limit: int = 200) -> Optional[Dict]:
        """Fetch 30M data from BingX Perpetuals"""
        try:
            url = "https://open-api.bingx.com/openApi/swap/v2/quote/klines"
            params = {
                'symbol': symbol,
                'interval': '30m',  # Only 30m supported
                'limit': limit
            }

            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            if data.get('code') == 0 and data.get('data'):
                ohlcv_data = {
                    'open': [float(item[1]) for item in data['data']],
                    'high': [float(item[2]) for item in data['data']],
                    'low': [float(item[3]) for item in data['data']],
                    'close': [float(item[4]) for item in data['data']],
                    'volume': [float(item[5]) for item in data['data']]
                }
                return ohlcv_data

        except Exception as e:
            print(f"❌ BingX Perpetuals error for {symbol}: {e}")
            return None

    def fetch_bingx_spot_data(self, symbol: str, limit: int = 200) -> Optional[Dict]:
        """Fetch 30M data from BingX Spot"""
        try:
            # Convert BTCUSDT -> BTC-USDT for spot API
            if symbol.endswith('USDT'):
                spot_symbol = symbol[:-4] + '-USDT'
            else:
                spot_symbol = symbol

            url = "https://open-api.bingx.com/openApi/spot/v1/market/kline"
            params = {
                'symbol': spot_symbol,
                'interval': '30m',  # Only 30m supported
                'limit': limit
            }

            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            if data.get('code') == 0 and data.get('data'):
                ohlcv_data = {
                    'open': [float(item[1]) for item in data['data']],
                    'high': [float(item[2]) for item in data['data']],
                    'low': [float(item[3]) for item in data['data']],
                    'close': [float(item[4]) for item in data['data']],
                    'volume': [float(item[5]) for item in data['data']]
                }
                return ohlcv_data

        except Exception as e:
            print(f"❌ BingX Spot error for {symbol}: {e}")
            return None

    def fetch_kucoin_data(self, symbol: str, limit: int = 200) -> Optional[Dict]:
        """Fetch 30M data from KuCoin"""
        try:
            url = "https://api.kucoin.com/api/v1/market/candles"
            end_time = int(time.time())
            start_time = end_time - (limit * 30 * 60)  # 30 minutes * limit

            params = {
                'symbol': symbol,
                'type': '30min',  # KuCoin format for 30m
                'startAt': start_time,
                'endAt': end_time
            }

            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            if data.get('code') == '200000' and data.get('data'):
                # KuCoin returns data in reverse order
                candles = data['data'][::-1]

                ohlcv_data = {
                    'open': [float(item[1]) for item in candles],
                    'high': [float(item[3]) for item in candles],
                    'low': [float(item[4]) for item in candles],
                    'close': [float(item[2]) for item in candles],
                    'volume': [float(item[5]) for item in candles]
                }
                return ohlcv_data

        except Exception as e:
            print(f"❌ KuCoin error for {symbol}: {e}")
            return None

    def fetch_okx_data(self, symbol: str, limit: int = 200) -> Optional[Dict]:
        """Fetch 30M data from OKX"""
        try:
            # Convert BTCUSDT -> BTC-USDT for OKX
            if symbol.endswith('USDT'):
                okx_symbol = symbol[:-4] + '-USDT'
            else:
                okx_symbol = symbol

            url = "https://www.okx.com/api/v5/market/candles"
            params = {
                'instId': okx_symbol,
                'bar': '30m',  # OKX format for 30m
                'limit': limit
            }

            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            if data.get('code') == '0' and data.get('data'):
                # OKX returns newest first
                candles = data['data'][::-1]

                ohlcv_data = {
                    'open': [float(item[1]) for item in candles],
                    'high': [float(item[2]) for item in candles],
                    'low': [float(item[3]) for item in candles],
                    'close': [float(item[4]) for item in candles],
                    'volume': [float(item[5]) for item in candles]
                }
                return ohlcv_data

        except Exception as e:
            print(f"❌ OKX error for {symbol}: {e}")
            return None

    def fetch_ohlcv_with_fallback(self, symbol: str, limit: int = 200) -> Tuple[Optional[Dict], str]:
        """Try exchanges in order until one succeeds"""

        # Try BingX Perpetuals first
        ohlcv_data = self.fetch_bingx_perpetuals_data(symbol, limit)
        if ohlcv_data:
            return ohlcv_data, 'BingX-Perp'

        # Try BingX Spot
        ohlcv_data = self.fetch_bingx_spot_data(symbol, limit)
        if ohlcv_data:
            return ohlcv_data, 'BingX-Spot'

        # Try KuCoin
        ohlcv_data = self.fetch_kucoin_data(symbol, limit)
        if ohlcv_data:
            return ohlcv_data, 'KuCoin'

        # Try OKX
        ohlcv_data = self.fetch_okx_data(symbol, limit)
        if ohlcv_data:
            return ohlcv_data, 'OKX'

        print(f"❌ All exchanges failed for {symbol}")
        return None, 'Failed'
