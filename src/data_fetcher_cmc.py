#!/usr/bin/env python3
"""
CoinMarketCap Market Data Fetcher for Dual Systems
- System 1: Top 100 coins by rank
- System 2: Filtered by market cap and volume (post-fetch filtering)
"""

import os
import json
import time
import requests
import yaml
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class CoinMarketCapFetcher:
    def __init__(self):
        self.api_key = os.getenv('COINMARKETCAP_API_KEY')
        self.base_url = "https://pro-api.coinmarketcap.com/v1"
        self.session = self.create_session()
        
    def create_session(self):
        session = requests.Session()
        
        retry_strategy = Retry(
            total=3,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        
        session.headers.update({
            'X-CMC_PRO_API_KEY': self.api_key,
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip, deflate',
        })
        
        return session
    
    def fetch_top_100_coins(self):
        """Fetch top 100 coins by market cap for 15m system"""
        print("🚀 Fetching top 100 coins for 15m system...")
        
        url = f"{self.base_url}/cryptocurrency/listings/latest"
        params = {
            'start': 1,
            'limit': 100,
            'sort': 'market_cap',
            'sort_dir': 'desc',
            'cryptocurrency_type': 'coins',
            'convert': 'USD'
        }
        
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            coins = []
            for coin in data['data']:
                coin_data = {
                    'symbol': coin['symbol'],
                    'name': coin['name'],
                    'market_cap': coin['quote']['USD']['market_cap'],
                    'volume_24h': coin['quote']['USD']['volume_24h'],
                    'current_price': coin['quote']['USD']['price'],
                    'price_change_percentage_24h': coin['quote']['USD']['percent_change_24h'],
                    'rank': coin['cmc_rank']
                }
                coins.append(coin_data)
            
            print(f"✅ Fetched {len(coins)} top coins")
            return coins
            
        except Exception as e:
            print(f"❌ Error fetching top 100 coins: {e}")
            return []
    
    def fetch_filtered_coins(self):
        """Fetch coins for multi-timeframe system with post-fetch filtering"""
        print("🚀 Fetching coins for multi-timeframe system...")
        
        # Fetch more coins to filter from (top 1000)
        url = f"{self.base_url}/cryptocurrency/listings/latest"
        params = {
            'start': 1,
            'limit': 3000,  # Increased to get more coins to filter from
            'sort': 'market_cap',
            'sort_dir': 'desc',
            'cryptocurrency_type': 'coins',
            'convert': 'USD'
        }
        
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            all_coins = []
            for coin in data['data']:
                coin_data = {
                    'symbol': coin['symbol'],
                    'name': coin['name'],
                    'market_cap': coin['quote']['USD']['market_cap'],
                    'volume_24h': coin['quote']['USD']['volume_24h'],
                    'current_price': coin['quote']['USD']['price'],
                    'price_change_percentage_24h': coin['quote']['USD']['percent_change_24h'],
                    'rank': coin['cmc_rank']
                }
                all_coins.append(coin_data)
            
            print(f"✅ Fetched {len(all_coins)} total coins for filtering")
            
            # Apply your filtering criteria
            filtered_coins = self.apply_multi_criteria(all_coins)
            return filtered_coins
            
        except Exception as e:
            print(f"❌ Error fetching coins for multi-timeframe: {e}")
            return []
    
    def apply_multi_criteria(self, coins):
        """Apply market cap and volume filtering for multi-timeframe system"""
        min_market_cap = 50_000_000   # $50M
        min_volume_24h = 20_000_000   # $20M
        
        filtered_coins = []
        
        for coin in coins:
            market_cap = coin.get('market_cap') or 0
            volume_24h = coin.get('volume_24h') or 0
            
            # Apply your criteria
            if market_cap >= min_market_cap and volume_24h >= min_volume_24h:
                filtered_coins.append(coin)
        
        print(f"🔍 Applied filtering criteria:")
        print(f"   Market cap >= ${min_market_cap:,}")
        print(f"   Volume 24h >= ${min_volume_24h:,}")
        print(f"   Result: {len(filtered_coins)} coins qualify")
        
        return filtered_coins
    
    def load_blocked_coins(self):
        """Load blocked coins list"""
        blocked_file = os.path.join(os.path.dirname(__file__), '..', 'config', 'blocked_coins.txt')
        blocked_coins = set()
        
        try:
            with open(blocked_file, 'r') as f:
                for line in f:
                    coin = line.strip().upper()
                    if coin and not coin.startswith('#'):
                        blocked_coins.add(coin)
            
            print(f"🚫 Loaded {len(blocked_coins)} blocked coins")
            return blocked_coins
            
        except FileNotFoundError:
            print("📝 No blocked_coins.txt found")
            return set()
    
    def filter_blocked_coins(self, coins, blocked_coins):
        """Remove blocked coins from list"""
        if not blocked_coins:
            return coins
        
        filtered = []
        blocked_count = 0
        
        for coin in coins:
            if coin['symbol'].upper() in blocked_coins:
                blocked_count += 1
                continue
            filtered.append(coin)
        
        print(f"🔍 Filtered out {blocked_count} blocked coins")
        return filtered
    
    def save_market_data(self, top_100_coins, filtered_coins):
        """Save market data to cache"""
        cache_dir = os.path.join(os.path.dirname(__file__), '..', 'cache')
        os.makedirs(cache_dir, exist_ok=True)
        
        # Save data for 15m system
        system_15m_data = {
            'updated_at': datetime.utcnow().isoformat(),
            'system': '15m',
            'total_coins': len(top_100_coins),
            'coins': top_100_coins
        }
        
        with open(os.path.join(cache_dir, 'market_data_15m.json'), 'w') as f:
            json.dump(system_15m_data, f, indent=2)
        
        # Save data for multi-timeframe system
        system_multi_data = {
            'updated_at': datetime.utcnow().isoformat(),
            'system': 'multi',
            'total_coins': len(filtered_coins),
            'filtering_criteria': {
                'min_market_cap': 50_000_000,
                'min_volume_24h': 20_000_000
            },
            'coins': filtered_coins
        }
        
        with open(os.path.join(cache_dir, 'market_data_multi.json'), 'w') as f:
            json.dump(system_multi_data, f, indent=2)
        
        print(f"💾 Saved market data:")
        print(f"   15m system: {len(top_100_coins)} coins")
        print(f"   Multi system: {len(filtered_coins)} coins")

def main():
    fetcher = CoinMarketCapFetcher()
    
    if not fetcher.api_key:
        print("❌ COINMARKETCAP_API_KEY not found in environment variables")
        return
    
    # Load blocked coins
    blocked_coins = fetcher.load_blocked_coins()
    
    # Fetch data for both systems
    print("📊 Starting market data fetch for both systems...")
    
    # System 1: Top 100 coins
    top_100_raw = fetcher.fetch_top_100_coins()
    top_100_filtered = fetcher.filter_blocked_coins(top_100_raw, blocked_coins)
    
    # System 2: Filtered coins (with proper criteria application)
    filtered_raw = fetcher.fetch_filtered_coins()
    filtered_final = fetcher.filter_blocked_coins(filtered_raw, blocked_coins)
    
    # Save to cache
    fetcher.save_market_data(top_100_filtered, filtered_final)
    
    print("✅ Market data fetch completed successfully!")
    print(f"\n📈 Expected results:")
    print(f"   15m system: ~98 coins (top 100 minus blocked)")
    print(f"   Multi system: ~200-400 coins (market cap ≥ $50M, volume ≥ $20M)")

if __name__ == '__main__':
    main()
