#!/usr/bin/env python3
"""
CoinMarketCap Market Data Fetcher for Dual Systems - FIXED VERSION
- System 1: Top 100 coins by rank  
- System 2: ALL coins meeting criteria (market cap ≥ $50M, volume ≥ $20M)
- Maximizes API calls to get more coins for multi-timeframe system
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
        
        # CoinMarketCap API limits (Basic plan can fetch up to 5000 coins)
        self.max_fetch_limit = 5000
        
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
        """Fetch maximum coins using pagination to get complete dataset"""
        print("🚀 Fetching ALL available coins for comprehensive filtering...")
        
        all_coins = []
        limit_per_call = 5000  # Maximum allowed per call
        start = 1
        
        while True:
            url = f"{self.base_url}/cryptocurrency/listings/latest"
            params = {
                'start': start,
                'limit': limit_per_call,
                'sort': 'market_cap',
                'sort_dir': 'desc',
                'cryptocurrency_type': 'coins',
                'convert': 'USD'
            }
            
            try:
                print(f"   Fetching coins {start} to {start + limit_per_call - 1}...")
                response = self.session.get(url, params=params, timeout=60)
                response.raise_for_status()
                data = response.json()
                
                if not data.get('data') or len(data['data']) == 0:
                    print(f"   No more coins available (stopped at {len(all_coins)} total)")
                    break
                
                batch_coins = []
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
                    batch_coins.append(coin_data)
                
                all_coins.extend(batch_coins)
                print(f"   ✅ Added {len(batch_coins)} coins (total: {len(all_coins)})")
                
                # If we got less than requested, we've reached the end
                if len(data['data']) < limit_per_call:
                    break
                
                start += limit_per_call
                
                # Respect rate limits
                time.sleep(2)
                
                # Safety limit to prevent infinite loops
                if len(all_coins) >= 10000:
                    print(f"   🛑 Reached safety limit of 10,000 coins")
                    break
                    
            except Exception as e:
                print(f"   ❌ Error fetching coins starting at {start}: {e}")
                break
        
        print(f"✅ Total coins fetched: {len(all_coins)}")
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
        no_data_coins = 0
        
        for coin in all_coins:
            market_cap = coin.get('market_cap') 
            volume_24h = coin.get('volume_24h')
            
            # Handle None values
            if market_cap is None or volume_24h is None:
                no_data_coins += 1
                continue
                
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
        print(f"   Missing data: {no_data_coins:,}")
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
        
        for coin in coins:
            if coin['symbol'].upper() in blocked_coins:
                blocked_count += 1
                continue
            filtered.append(coin)
        
        print(f"🔍 Filtered out {blocked_count} blocked coins")
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
        print(f"\n📝 Data Usage:")
        print(f"   • 15m system analyzes ONLY its {len(top_100_coins)} coins")
        print(f"   • Multi-timeframe system analyzes ONLY its {len(multi_coins)} coins") 
        print(f"   • Systems do NOT share or combine datasets")

def main():
    fetcher = CoinMarketCapFetcher()
    
    if not fetcher.api_key:
        print("❌ COINMARKETCAP_API_KEY not found in environment variables")
        return
    
    print("="*80)
    print("🔥 COMPREHENSIVE MARKET DATA FETCH - DUAL SYSTEMS")
    print("="*80)
    
    # Load blocked coins
    blocked_coins = fetcher.load_blocked_coins()
    
    # Fetch ALL available coins
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
    print(f"   • Total coins processed: {len(all_coins):,}")
    print(f"   • 15m system dataset: {len(top_100_final)} coins")
    print(f"   • Multi-timeframe dataset: {len(multi_final)} coins")
    print(f"   • Expected multi-timeframe range: 200-400 coins")
    
    if len(multi_final) < 200:
        print(f"\n⚠️  Multi-timeframe coin count is lower than expected.")
        print(f"   This could be due to:")
        print(f"   • Current market conditions (fewer coins meeting volume criteria)")
        print(f"   • API rate limiting")
        print(f"   • Data availability issues")
    
    print("="*80)

if __name__ == '__main__':
    main()
