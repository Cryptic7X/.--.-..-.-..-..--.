#!/usr/bin/env python3
"""
CoinMarketCap Market Data Fetcher - PAGINATION FIXED
- Uses multiple API calls to fetch ALL available coins
- Proper pagination to get 200+ coins for multi-timeframe system
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
    
    def fetch_all_coins_paginated(self):
        """Fetch coins using proper pagination - multiple API calls"""
        print("🚀 Fetching ALL available coins using pagination...")
        
        all_coins = []
        coins_per_page = 100  # API standard limit
        max_pages = 30  # Fetch up to 3000 coins (30 pages × 100)
        
        for page in range(1, max_pages + 1):
            start_position = (page - 1) * coins_per_page + 1
            
            print(f"   📄 Page {page}/{max_pages}: Fetching coins {start_position} to {start_position + coins_per_page - 1}")
            
            url = f"{self.base_url}/cryptocurrency/listings/latest"
            params = {
                'start': start_position,
                'limit': coins_per_page,
                'sort': 'market_cap',
                'sort_dir': 'desc',
                'cryptocurrency_type': 'coins',
                'convert': 'USD'
            }
            
            try:
                response = self.session.get(url, params=params, timeout=60)
                response.raise_for_status()
                data = response.json()
                
                if not data.get('data') or len(data['data']) == 0:
                    print(f"   📭 No more coins available (stopped at page {page})")
                    break
                
                page_coins = []
                for coin in data['data']:
                    # Skip coins with missing data
                    if (coin['quote']['USD']['market_cap'] is None or 
                        coin['quote']['USD']['volume_24h'] is None):
                        continue
                        
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
                print(f"   ✅ Added {len(page_coins)} valid coins (total: {len(all_coins)})")
                
                # If we got less coins than requested, we've reached the end
                if len(data['data']) < coins_per_page:
                    print(f"   📭 Reached end of available coins (page {page})")
                    break
                
                # Rate limiting between pages
                time.sleep(1)
                
            except requests.exceptions.RequestException as e:
                print(f"   ❌ API error on page {page}: {e}")
                # Continue with next page instead of stopping
                continue
            except Exception as e:
                print(f"   ❌ Unexpected error on page {page}: {e}")
                continue
        
        print(f"✅ Pagination complete: {len(all_coins)} total coins fetched")
        return all_coins
    
    def extract_top_100(self, all_coins):
        """Extract top 100 coins for 15m system"""
        top_100 = all_coins[:100] if len(all_coins) >= 100 else all_coins
        print(f"📊 Extracted top {len(top_100)} coins for 15m system")
        return top_100
    
    def apply_multi_criteria(self, all_coins):
        """Apply market cap and volume filtering for multi-timeframe system"""
        min_market_cap = 50_000_000   # $50M
        min_volume_24h = 20_000_000   # $20M
        
        print(f"🔍 Applying multi-timeframe filtering criteria...")
        print(f"   Market cap >= ${min_market_cap:,}")
        print(f"   Volume 24h >= ${min_volume_24h:,}")
        
        qualified_coins = []
        below_market_cap = 0
        below_volume = 0
        
        for coin in all_coins:
            market_cap = coin.get('market_cap', 0)
            volume_24h = coin.get('volume_24h', 0)
            
            # Apply criteria
            if market_cap < min_market_cap:
                below_market_cap += 1
                continue
                
            if volume_24h < min_volume_24h:
                below_volume += 1
                continue
                
            qualified_coins.append(coin)
        
        print(f"🔍 Filtering results:")
        print(f"   Total coins processed: {len(all_coins):,}")
        print(f"   Below ${min_market_cap/1_000_000:.0f}M market cap: {below_market_cap:,}")
        print(f"   Below ${min_volume_24h/1_000_000:.0f}M volume: {below_volume:,}")
        print(f"   ✅ Qualified coins: {len(qualified_coins):,}")
        
        return qualified_coins
    
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
    
    def save_market_data(self, top_100_coins, multi_coins):
        """Save market data to cache with detailed metadata"""
        cache_dir = os.path.join(os.path.dirname(__file__), '..', 'cache')
        os.makedirs(cache_dir, exist_ok=True)
        
        # Save data for 15m system
        system_15m_data = {
            'updated_at': datetime.utcnow().isoformat(),
            'system': '15m',
            'description': 'Top 100 coins by market cap rank',
            'total_coins': len(top_100_coins),
            'filtering_method': 'top_100_by_rank',
            'coins': top_100_coins
        }
        
        with open(os.path.join(cache_dir, 'market_data_15m.json'), 'w') as f:
            json.dump(system_15m_data, f, indent=2)
        
        # Save data for multi-timeframe system  
        system_multi_data = {
            'updated_at': datetime.utcnow().isoformat(),
            'system': 'multi',
            'description': 'Coins filtered by market cap and volume criteria',
            'total_coins': len(multi_coins),
            'filtering_method': 'criteria_based',
            'filtering_criteria': {
                'min_market_cap': 50_000_000,
                'min_volume_24h': 20_000_000,
                'description': 'Market cap >= $50M AND Volume >= $20M'
            },
            'coins': multi_coins
        }
        
        with open(os.path.join(cache_dir, 'market_data_multi.json'), 'w') as f:
            json.dump(system_multi_data, f, indent=2)
        
        print(f"💾 Saved market data:")
        print(f"   📊 15m system: {len(top_100_coins)} coins (independent dataset)")
        print(f"   📈 Multi system: {len(multi_coins)} coins (independent dataset)")

def main():
    fetcher = CoinMarketCapFetcher()
    
    if not fetcher.api_key:
        print("❌ COINMARKETCAP_API_KEY not found in environment variables")
        return
    
    print("="*80)
    print("🔥 COMPREHENSIVE MARKET DATA FETCH - PAGINATION FIXED")
    print("="*80)
    
    # Load blocked coins
    blocked_coins = fetcher.load_blocked_coins()
    
    # Fetch ALL available coins using pagination
    all_coins = fetcher.fetch_all_coins_paginated()
    
    if not all_coins:
        print("❌ No coins fetched - check API connection and key")
        return
    
    print(f"\n" + "="*60)
    print("📊 PREPARING DATASETS FOR BOTH SYSTEMS")
    print("="*60)
    
    # System 1: Extract top 100 coins
    top_100_raw = fetcher.extract_top_100(all_coins)
    top_100_final = fetcher.filter_blocked_coins(top_100_raw, blocked_coins)
    
    # System 2: Apply filtering criteria
    multi_qualified = fetcher.apply_multi_criteria(all_coins)
    multi_final = fetcher.filter_blocked_coins(multi_qualified, blocked_coins)
    
    # Save both datasets
    fetcher.save_market_data(top_100_final, multi_final)
    
    print(f"\n" + "="*80)
    print("✅ MARKET DATA FETCH COMPLETED SUCCESSFULLY!")
    print("="*80)
    print(f"📈 Results Summary:")
    print(f"   • Total coins fetched: {len(all_coins):,}")
    print(f"   • 15m system dataset: {len(top_100_final)} coins")
    print(f"   • Multi-timeframe dataset: {len(multi_final)} coins")
    
    print(f"\n📝 System Usage:")
    print(f"   • 15m system analyzes ONLY its {len(top_100_final)} coins")
    print(f"   • Multi-timeframe system analyzes ONLY its {len(multi_final)} coins")
    print(f"   • Systems operate independently (no shared datasets)")
    
    if len(multi_final) >= 200:
        print(f"\n🎉 SUCCESS: Multi-timeframe system now has sufficient coins!")
    else:
        print(f"\n⚠️  Multi-timeframe coin count still lower than expected.")
        print(f"   This suggests market conditions have fewer coins meeting your criteria.")
    
    print("="*80)

if __name__ == '__main__':
    main()
