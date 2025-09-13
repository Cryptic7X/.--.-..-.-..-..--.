#!/usr/bin/env python3
"""
CoinMarketCap Data Fetcher - CORRECTED endpoint
Based on your working 30m system but using CoinMarketCap instead of CoinGecko
"""

import os
import json
import time
import requests
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class CoinMarketCapFetcher:
    def __init__(self):
        self.api_key = os.getenv('COINMARKETCAP_API_KEY')
        if not self.api_key:
            raise ValueError("COINMARKETCAP_API_KEY environment variable not set")
        
        self.session = self.create_session()
        self.base_url = "https://pro-api.coinmarketcap.com/v1"
        
    def create_session(self):
        """Create requests session with retry strategy"""
        session = requests.Session()
        
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        
        session.headers.update({
            'Accepts': 'application/json',
            'X-CMC_PRO_API_KEY': self.api_key,
        })
        
        return session
    
    def fetch_top_coins(self, limit=500):
        """Fetch top coins by market cap"""
        print(f"🚀 Fetching top {limit} coins from CoinMarketCap...")
        
        # CORRECTED ENDPOINT
        url = f"{self.base_url}/cryptocurrency/listings/latest"
        params = {
            'start': 1,
            'limit': limit,
            'sort': 'market_cap',
            'convert': 'USD'
        }
        
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            coins = data.get('data', [])
            
            print(f"✅ Fetched {len(coins)} coins from CoinMarketCap")
            return self.format_coins_data(coins)
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Failed to fetch coins: {e}")
            return []
    
    def format_coins_data(self, coins):
        """Format coin data to match expected structure"""
        formatted_coins = []
        
        for coin in coins:
            try:
                quote_usd = coin.get('quote', {}).get('USD', {})
                
                formatted_coin = {
                    'id': coin.get('id'),
                    'symbol': coin.get('symbol', '').upper(),
                    'name': coin.get('name', ''),
                    'current_price': quote_usd.get('price', 0),
                    'market_cap': quote_usd.get('market_cap', 0),
                    'total_volume': quote_usd.get('volume_24h', 0),
                    'price_change_percentage_24h': quote_usd.get('percent_change_24h', 0),
                    'market_cap_rank': coin.get('cmc_rank', 0),
                    'last_updated': coin.get('last_updated', ''),
                }
                
                formatted_coins.append(formatted_coin)
                
            except (KeyError, TypeError) as e:
                print(f"⚠️ Error formatting coin data: {e}")
                continue
        
        return formatted_coins
    
    def filter_coins(self, coins, system_type):
        """Filter coins based on system type"""
        filtered_coins = []
        
        for coin in coins:
            try:
                market_cap = coin.get('market_cap', 0) or 0
                volume_24h = coin.get('total_volume', 0) or 0
                
                if system_type == '15m':
                    # Top 100 coins for 15m system
                    if coin.get('market_cap_rank', 999) <= 100:
                        filtered_coins.append(coin)
                elif system_type == 'multi':
                    # Filtered coins for multi-timeframe
                    if market_cap >= 50_000_000 and volume_24h >= 20_000_000:
                        filtered_coins.append(coin)
                        
            except (KeyError, TypeError):
                continue
        
        print(f"✅ Filtered to {len(filtered_coins)} coins for {system_type} system")
        return filtered_coins
    
    def save_market_data(self, coins, system_type):
        """Save market data to cache"""
        cache_dir = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'cache')
        os.makedirs(cache_dir, exist_ok=True)
        
        cache_file = os.path.join(cache_dir, f'market-data-{system_type}.json')
        
        data = {
            'updated_at': datetime.utcnow().isoformat(),
            'system_type': system_type,
            'total_coins': len(coins),
            'coins': coins
        }
        
        with open(cache_file, 'w') as f:
            json.dump(data, f, indent=2)
        
        print(f"💾 Saved {len(coins)} coins to {cache_file}")
        return cache_file
    
    def run_data_fetch(self):
        """Run complete market data fetch for both systems"""
        print("="*80)
        print("📊 COINMARKETCAP DATA FETCH")
        print("="*80)
        print(f"🕐 Fetch Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
        
        try:
            # Fetch top 1000 coins
            all_coins = self.fetch_top_coins(1000)
            
            if all_coins:
                # System 1: Top 100 for 15m
                coins_15m = self.filter_coins(all_coins, '15m')
                self.save_market_data(coins_15m, '15m')
                
                # System 2: Filtered for multi-timeframe  
                coins_multi = self.filter_coins(all_coins, 'multi')
                self.save_market_data(coins_multi, 'multi')
                
                print(f"\n✅ Market data fetch completed")
                print(f"📊 15m System: {len(coins_15m)} coins")
                print(f"📊 Multi System: {len(coins_multi)} coins")
            else:
                print("❌ No coins fetched")
                
        except Exception as e:
            print(f"💥 Critical error: {str(e)}")

def load_market_data(system_type):
    """Load cached market data for specified system"""
    cache_file = os.path.join(
        os.path.dirname(__file__), '..', '..', '..', 'cache', 
        f'market-data-{system_type}.json'
    )
    
    try:
        if not os.path.exists(cache_file):
            print(f"❌ Market data cache not found: {cache_file}")
            return []
        
        with open(cache_file, 'r') as f:
            data = json.load(f)
        
        coins = data.get('coins', [])
        print(f"📊 Loaded {len(coins)} coins for {system_type} system")
        
        return coins
        
    except Exception as e:
        print(f"❌ Error loading market data: {e}")
        return []

if __name__ == '__main__':
    fetcher = CoinMarketCapFetcher()
    fetcher.run_data_fetch()
