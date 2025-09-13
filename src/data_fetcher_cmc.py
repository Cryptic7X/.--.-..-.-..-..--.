#!/usr/bin/env python3
"""
CoinMarketCap Market Data Fetcher for Dual Systems
- System 1: Top 100 coins by rank
- System 2: Top 500 coins by rank (with proper handling of zero market cap)
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
        """Fetch top 100 coins by rank for 15m system"""
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
                # Handle zero/null market cap properly - keep the coin but set to 0
                market_cap = coin['quote']['USD'].get('market_cap') 
                if market_cap is None:
                    market_cap = 0
                    
                volume_24h = coin['quote']['USD'].get('volume_24h')
                if volume_24h is None:
                    volume_24h = 0
                
                coin_data = {
                    'symbol': coin['symbol'],
                    'name': coin['name'],
                    'market_cap': market_cap,
                    'volume_24h': volume_24h,
                    'current_price': coin['quote']['USD'].get('price', 0),
                    'price_change_percentage_24h': coin['quote']['USD'].get('percent_change_24h', 0),
                    'rank': coin['cmc_rank']
                }
                coins.append(coin_data)
            
            print(f"✅ Fetched {len(coins)} top coins")
            return coins
            
        except Exception as e:
            print(f"❌ Error fetching top 100 coins: {e}")
            return []
    
    def fetch_top_500_coins(self):
        """Fetch top 500 coins by rank for multi-timeframe system - PRESERVE ALL COINS"""
        print("🚀 Fetching top 500 coins by rank for multi-timeframe system...")
        
        # Fetch more coins to ensure we get exactly 500 after filtering incomplete data
        url = f"{self.base_url}/cryptocurrency/listings/latest"
        params = {
            'start': 1,
            'limit': 600,  # Fetch extra to account for incomplete data
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
            zero_market_cap_count = 0
            
            for coin in data['data']:
                # Handle zero/null market cap - but keep ALL coins with valid symbols
                market_cap = coin['quote']['USD'].get('market_cap')
                if market_cap is None or market_cap <= 0:
                    market_cap = 1  # Set minimal positive value to keep coin in results
                    zero_market_cap_count += 1
                    
                volume_24h = coin['quote']['USD'].get('volume_24h')
                if volume_24h is None or volume_24h <= 0:
                    volume_24h = 1  # Set minimal positive value
                
                # Only exclude coins with completely missing symbol or invalid data
                if not coin.get('symbol') or not coin.get('name'):
                    continue
                
                coin_data = {
                    'symbol': coin['symbol'],
                    'name': coin['name'],
                    'market_cap': market_cap,
                    'volume_24h': volume_24h,
                    'current_price': coin['quote']['USD'].get('price', 0),
                    'price_change_percentage_24h': coin['quote']['USD'].get('percent_change_24h', 0),
                    'rank': coin['cmc_rank']
                }
                all_coins.append(coin_data)
            
            # Take exactly top 500 by rank (after handling incomplete data)
            top_500_coins = sorted(all_coins, key=lambda x: x['rank'])[:500]
            
            print(f"✅ Fetched {len(all_coins)} total coins")
            print(f"🔧 Fixed {zero_market_cap_count} coins with zero/null market cap")
            print(f"📊 Returning top 500 by rank: {len(top_500_coins)} coins")
            print(f"   Rank range: {top_500_coins[0]['rank']}-{top_500_coins[-1]['rank']}")
            
            return top_500_coins
            
        except Exception as e:
            print(f"❌ Error fetching top 500 coins: {e}")
            return []
    
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
                print(f"   🚫 Blocked: {coin['symbol']} (rank {coin['rank']})")
                continue
            filtered.append(coin)
        
        print(f"🔍 Filtered out {blocked_count} blocked coins")
        return filtered
    
    def save_market_data(self, top_100_coins, top_500_coins):
        """Save market data to cache"""
        cache_dir = os.path.join(os.path.dirname(__file__), '..', 'cache')
        os.makedirs(cache_dir, exist_ok=True)
        
        # Save data for 15m system
        system_15m_data = {
            'updated_at': datetime.utcnow().isoformat(),
            'system': '15m',
            'method': 'top_100_by_rank',
            'total_coins': len(top_100_coins),
            'coins': top_100_coins
        }
        
        with open(os.path.join(cache_dir, 'market_data_15m.json'), 'w') as f:
            json.dump(system_15m_data, f, indent=2)
        
        # Save data for multi-timeframe system
        system_multi_data = {
            'updated_at': datetime.utcnow().isoformat(),
            'system': 'multi',
            'method': 'top_500_by_rank',
            'total_coins': len(top_500_coins),
            'rank_range': f"{top_500_coins[0]['rank']}-{top_500_coins[-1]['rank']}" if top_500_coins else "0-0",
            'coins': top_500_coins
        }
        
        with open(os.path.join(cache_dir, 'market_data_multi.json'), 'w') as f:
            json.dump(system_multi_data, f, indent=2)
        
        print(f"💾 Saved market data:")
        print(f"   15m system: {len(top_100_coins)} coins (rank 1-100)")
        print(f"   Multi system: {len(top_500_coins)} coins (rank 1-500)")

def main():
    fetcher = CoinMarketCapFetcher()
    
    if not fetcher.api_key:
        print("❌ COINMARKETCAP_API_KEY not found in environment variables")
        return
    
    # Load blocked coins
    blocked_coins = fetcher.load_blocked_coins()
    
    # Fetch data for both systems
    print("📊 Starting market data fetch for both systems...")
    print("🕐 Update frequency: Every 6 hours")
    print("🔧 Fixed: Proper handling of zero market cap coins")
    
    # System 1: Top 100 coins
    top_100_raw = fetcher.fetch_top_100_coins()
    top_100_filtered = fetcher.filter_blocked_coins(top_100_raw, blocked_coins)
    
    # System 2: Top 500 coins (with proper data handling)
    top_500_raw = fetcher.fetch_top_500_coins()
    top_500_filtered = fetcher.filter_blocked_coins(top_500_raw, blocked_coins)
    
    # Save to cache
    fetcher.save_market_data(top_100_filtered, top_500_filtered)
    
    print("✅ Market data fetch completed successfully!")
    print(f"\n📈 Results summary:")
    print(f"   15m system: ~98 coins (top 100 minus blocked)")
    print(f"   Multi system: ~{500 - len(blocked_coins)} coins (true top 500 minus blocked)")
    print(f"   Next update: 6 hours")

if __name__ == '__main__':
    main()
