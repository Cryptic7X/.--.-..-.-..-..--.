"""
Simple Exchange Manager for EMA 15M - OPTIMIZED VERSION
Based on user's simple_exchange.py - 15M timeframe only
"""

import os
import json
import time
import requests
from datetime import datetime, timedelta
from typing import Optional, Tuple, Dict, Any

class SimpleExchangeManager:
    def __init__(self):
        self.session = self.create_session()

    def create_session(self):
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'EMA-15M-Standalone/1.0',
            'Accept': 'application/json'
        })
        return session

    def get_supported_timeframes(self) -> list:
        """Only 15m timeframe supported"""
        return ['15m']

    def fetch_bingx_perpetuals_data(self, symbol: str, limit: int = 200) -> Optional[Dict]:
        """Fetch 15m data from BingX Perpetuals"""
        try:
            interval_map = {'15m': '15m'}
            bingx_interval = interval_map.get('15m')

            url = "https://open-api.bingx.com/openApi/swap/v2/quote/klines"
            params = {
                'symbol': symbol,
                'interval': bingx_interval,
                'limit': limit
            }

            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            if data.get('code') == 0 and data.get('data'):
                klines = data['data']

                return {
                    'open': [float(k['open']) for k in klines],
                    'high': [float(k['high']) for k in klines],
                    'low': [float(k['low']) for k in klines],
                    'close': [float(k['close']) for k in klines],
                    'volume': [float(k['volume']) for k in klines],
                    'timestamp': [int(k['time']) for k in klines]
                }
        except Exception as e:
            print(f"❌ BingX Perpetuals error for {symbol}: {e}")
            return None

    def fetch_bingx_spot_data(self, symbol: str, limit: int = 200) -> Optional[Dict]:
        """Fetch 15m data from BingX Spot"""
        try:
            # Convert BTCUSDT to BTC-USDT format for BingX Spot
            if symbol.endswith('USDT'):
                spot_symbol = symbol[:-4] + '-USDT'
            else:
                spot_symbol = symbol

            interval_map = {'15m': '15m'}
            bingx_interval = interval_map.get('15m')

            url = "https://open-api.bingx.com/openApi/spot/v1/market/kline"
            params = {
                'symbol': spot_symbol,
                'interval': bingx_interval,
                'limit': limit
            }

            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            if data.get('code') == 0 and data.get('data'):
                klines = data['data']

                return {
                    'open': [float(k[1]) for k in klines],
                    'high': [float(k[2]) for k in klines],
                    'low': [float(k[3]) for k in klines],
                    'close': [float(k[4]) for k in klines],
                    'volume': [float(k[5]) for k in klines],
                    'timestamp': [int(k[0]) for k in klines]
                }
        except Exception as e:
            print(f"❌ BingX Spot error for {symbol}: {e}")
            return None

    def fetch_kucoin_data(self, symbol: str, limit: int = 200) -> Optional[Dict]:
        """Fetch 15m data from KuCoin"""
        try:
            interval_map = {'15m': '15min'}
            kucoin_interval = interval_map.get('15m')

            # Calculate time range
            end_time = int(time.time())
            start_time = end_time - (limit * 15 * 60)  # 15 minutes per candle

            url = "https://api.kucoin.com/api/v1/market/candles"
            params = {
                'symbol': symbol,
                'type': kucoin_interval,
                'startAt': start_time,
                'endAt': end_time
            }

            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            if data.get('code') == '200000' and data.get('data'):
                klines = data['data']
                klines.reverse()  # KuCoin returns newest first

                return {
                    'open': [float(k[1]) for k in klines],
                    'high': [float(k[3]) for k in klines],
                    'low': [float(k[4]) for k in klines],
                    'close': [float(k[2]) for k in klines],
                    'volume': [float(k[5]) for k in klines],
                    'timestamp': [int(k[0]) for k in klines]
                }
        except Exception as e:
            print(f"❌ KuCoin error for {symbol}: {e}")
            return None

    def fetch_ohlcv_with_fallback(self, symbol: str, timeframe: str = '15m', limit: int = 200) -> Tuple[Optional[Dict], str]:
        """Fetch OHLCV data with exchange fallback - 15M ONLY"""
        if timeframe != '15m':
            print(f"❌ Unsupported timeframe: {timeframe}. Only 15m supported.")
            return None, None

        exchanges = [
            ('BingX Perpetuals', self.fetch_bingx_perpetuals_data),
            ('BingX Spot', self.fetch_bingx_spot_data),
            ('KuCoin', self.fetch_kucoin_data)
        ]

        for exchange_name, fetch_func in exchanges:
            try:
                data = fetch_func(symbol, limit)
                if data and len(data.get('close', [])) >= 50:  # Minimum data check
                    print(f"✅ {symbol} data from {exchange_name}")
                    return data, exchange_name
            except Exception as e:
                print(f"❌ {exchange_name} failed for {symbol}: {e}")
                continue

        print(f"❌ All exchanges failed for {symbol}")
        return None, None
