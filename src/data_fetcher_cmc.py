#!/usr/bin/env python3
"""
CoinMarketCap Market Data Fetcher for Dual Systems
- System 1: Top 100 coins by rank
- System 2: Top 500+ coins to ensure complete coverage
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
    
    def fetch_extended_coin_list(self):
        """Fetch extended coin list using pagination to ensure top 500+ coverage"""
        print("🚀 Fetching extended coin list for multi-timeframe system...")
        
        all_coins = []
        pages_to_fetch = 2  # Fetch 1000 coins to ensure we get top 500 after filtering
        
        for page in range(pages_to_fetch):
            start = page * 500 + 1
            print(f"   📄 Fetching page {page + 1}/{pages_to_fetch} (coins {start}-{start + 499})")
            
            url = f"{self.base_url}/cryptocurrency/listings/latest"
            params = {
                'start': start,
                'limit': 500,
                'sort': 'market_cap',
                'sort_dir': 'desc',
                'cryptocurrency_type': 'coins',
                'convert': 'USD'
            }
            
            try:
                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                page_coins = []
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
                    page_coins.append(coin_data)
                
                all_coins.extend(page_coins)
                print(f"   ✅ Page {page + 1}: {len(page_coins)} coins fetched")
                
                # Rate limiting between pages
                if page < pages_to_fetch - 1:
                    time.sleep(1)
                    
            except Exception as e:
                print(f"   ❌ Page {page + 1} failed: {e}")
                break
        
        print(f"✅ Total coins fetched: {len(all_coins)}")
        if all_coins:
            print(f"   Rank range: {all_coins[0]['rank']} - {all_coins[-1]['rank']}")
        
        return all_coins
    
    def select_top_500_by_rank(self, coins):
        """Select exactly top 500 coins by rank from extended list"""
        if not coins:
            return []
        
        # Sort by rank to ensure correct order
        sorted_coins = sorted(coins, key=lambda x: x['rank'])
        
        # Take top 500 by rank
        top_500 = sorted_coins[:500]
        
        print(f"🎯 Selected top 500 coins by rank:")
        print(f"   Rank range: {top_500[0]['rank']} - {top_500[-1]['rank']}")
        print(f"   Total selected: {len(top_500)}")
        
        return top_500
    
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
        blocked_symbols = []
        
        for coin in coins:
            if coin['symbol'].upper() in blocked_coins:
                blocked_count += 1
                blocked_symbols.append(coin['symbol'])
                continue
            filtered.append(coin)
        
        print(f"🔍 Filtered out {blocked_count} blocked coins")
        if blocked_symbols:
            print(f"   Blocked: {', '.join(blocked_symbols[:10])}")
            if len(blocked_symbols) > 10:
                print(f"   ... and {len(blocked_symbols) - 10} more")
        
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
        rank_range = f"1-{top_500_coins[-1]['rank']}" if top_500_coins else "none"
        system_multi_data = {
            'updated_at': datetime.utcnow().isoformat(),
            'system': 'multi',
            'method': 'top_500_by_rank_extended_fetch',
            'total_coins': len(top_500_coins),
            'rank_range': rank_range,
            'coins': top_500_coins
        }
        
        with open(os.path.join(cache_dir, 'market_data_multi.json'), 'w') as f:
            json.dump(system_multi_data, f, indent=2)
        
        print(f"💾 Saved market data:")
        print(f"   15m system: {len(top_100_coins)} coins (rank 1-100)")
        print(f"   Multi system: {len(top_500_coins)} coins (rank {rank_range})")

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
    print("🎯 Strategy: Extended fetch to ensure complete top 500 coverage")
    
    # System 1: Top 100 coins
    top_100_raw = fetcher.fetch_top_100_coins()
    top_100_filtered = fetcher.filter_blocked_coins(top_100_raw, blocked_coins)
    
    # System 2: Extended fetch + top 500 selection
    extended_coins = fetcher.fetch_extended_coin_list()
    top_500_selected = fetcher.select_top_500_by_rank(extended_coins)
    top_500_filtered = fetcher.filter_blocked_coins(top_500_selected, blocked_coins)
    
    # Save to cache
    fetcher.save_market_data(top_100_filtered, top_500_filtered)
    
    print("✅ Market data fetch completed successfully!")
    print(f"\n📈 Results summary:")
    print(f"   15m system: ~98 coins (top 100 minus blocked)")
    print(f"   Multi system: ~{len(top_500_filtered)} coins (actual top 500 by rank)")
    print(f"   Next update: 6 hours")

if __name__ == '__main__':
    main()
