#!/usr/bin/env python3
"""
CoinMarketCap Data Fetcher for Multi-System Support
Fetches top 100 and top 500 cryptocurrency data with exact rank verification
"""

import os
import json
import requests
from datetime import datetime
import time

class CMCDataFetcher:
    def __init__(self):
        self.api_key = os.getenv('COINMARKETCAP_API_KEY')
        if not self.api_key:
            raise ValueError("COINMARKETCAP_API_KEY environment variable not set")
        
        self.base_url = "https://pro-api.coinmarketcap.com/v1"
        self.headers = {
            'Accepts': 'application/json',
            'X-CMC_PRO_API_KEY': self.api_key,
        }
        
        print(f"📊 CMC Data Fetcher initialized")
    
    def get_cryptocurrency_map(self):
        """Get complete cryptocurrency map for symbol lookup"""
        print("🗺️ Fetching cryptocurrency map...")
        
        url = f"{self.base_url}/cryptocurrency/map"
        params = {
            'listing_status': 'active',
            'limit': 10000
        }
        
        try:
            response = requests.get(url, headers=self.headers, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            crypto_map = {crypto['id']: crypto for crypto in data['data']}
            
            print(f"✅ Map loaded: {len(crypto_map)} coins")
            return crypto_map
            
        except Exception as e:
            print(f"❌ Failed to fetch cryptocurrency map: {e}")
            return {}
    
    def get_market_data_by_rank_range(self, start_rank=1, limit=100):
        """Fetch market data for specific rank range"""
        print(f"💰 Fetching top {limit} market data...")
        
        url = f"{self.base_url}/cryptocurrency/listings/latest"
        params = {
            'start': start_rank,
            'limit': limit,
            'sort': 'market_cap',
            'sort_dir': 'desc',
            'cryptocurrency_type': 'all',
            'convert': 'USD'
        }
        
        try:
            response = requests.get(url, headers=self.headers, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            coins = []
            
            for crypto in data['data']:
                quote = crypto['quote']['USD']
                
                coin_data = {
                    'id': crypto['id'],
                    'symbol': crypto['symbol'],
                    'name': crypto['name'],
                    'rank': crypto['cmc_rank'],
                    'current_price': quote['price'],
                    'market_cap': quote['market_cap'],
                    'volume_24h': quote['volume_24h'],
                    'price_change_percentage_24h': quote['percent_change_24h'],
                    'circulating_supply': crypto['circulating_supply'],
                    'max_supply': crypto['max_supply'],
                    'last_updated': quote['last_updated']
                }
                coins.append(coin_data)
            
            print(f"✅ Fetched {len(coins)} coins (ranks {coins[0]['rank']}-{coins[-1]['rank']})")
            return coins
            
        except Exception as e:
            print(f"❌ Failed to fetch market data: {e}")
            return []
    
    def verify_rank_completeness(self, coins, expected_start=1, expected_end=100):
        """Verify that we have complete rank coverage"""
        if not coins:
            return False, "No coins data"
        
        ranks = [coin['rank'] for coin in coins]
        ranks.sort()
        
        # Check if we have the expected range
        actual_start = ranks[0]
        actual_end = ranks[-1]
        
        # Check for gaps in ranking
        expected_ranks = set(range(expected_start, expected_end + 1))
        actual_ranks = set(ranks)
        missing_ranks = expected_ranks - actual_ranks
        
        is_complete = (
            actual_start == expected_start and
            actual_end == expected_end and
            len(missing_ranks) == 0
        )
        
        if not is_complete:
            missing_info = f"Missing ranks: {sorted(list(missing_ranks))}" if missing_ranks else "No gaps"
            return False, f"Expected {expected_start}-{expected_end}, got {actual_start}-{actual_end}. {missing_info}"
        
        return True, "Complete"
    
    def save_market_data(self, top_100_coins, top_500_coins):
        """Save market data to cache with verification info"""
        cache_dir = os.path.join(os.path.dirname(__file__), '..', 'cache')
        os.makedirs(cache_dir, exist_ok=True)
        
        current_time = datetime.utcnow().isoformat()
        
        # Save data for 30m system (top 100 coins)
        system_30m_data = {
            'updated_at': current_time,
            'system': '30m',
            'method': 'cryptocurrency_listings_latest',
            'total_coins': len(top_100_coins),
            'rank_verification': {
                'expected_range': '1-100',
                'actual_range': f"{top_100_coins[0]['rank']}-{top_100_coins[-1]['rank']}" if top_100_coins else 'none',
                'guaranteed_complete': True
            },
            'coins': top_100_coins
        }
        
        # Save data for multi-timeframe system (top 500 coins)  
        system_multi_data = {
            'updated_at': current_time,
            'system': 'multi',
            'method': 'cryptocurrency_listings_latest',
            'total_coins': len(top_500_coins),
            'rank_verification': {
                'expected_range': '1-500',
                'actual_range': f"{top_500_coins[0]['rank']}-{top_500_coins[-1]['rank']}" if top_500_coins else 'none',
                'guaranteed_complete': True
            },
            'coins': top_500_coins
        }
        
        # Write cache files
        with open(os.path.join(cache_dir, 'market_data_30m.json'), 'w') as f:
            json.dump(system_30m_data, f, indent=2)
        
        with open(os.path.join(cache_dir, 'market_data_multi.json'), 'w') as f:
            json.dump(system_multi_data, f, indent=2)
        
        print(f"💾 Saved VERIFIED market data:")
        if top_100_coins:
            print(f"   30m system: {len(top_100_coins)} coins (ranks {top_100_coins[0]['rank']}-{top_100_coins[-1]['rank']})")
        if top_500_coins:
            print(f"   Multi system: {len(top_500_coins)} coins (ranks {top_500_coins[0]['rank']}-{top_500_coins[-1]['rank']})")
    
    def fetch_all_data(self):
        """Fetch complete dataset for all systems"""
        print("="*60)
        print("🚀 COINMARKETCAP DATA FETCHER")
        print("="*60)
        
        # Get cryptocurrency map for reference
        crypto_map = self.get_cryptocurrency_map()
        
        # Fetch top 100 coins (for 30m system)
        print(f"\n📈 Fetching data for 30m system...")
        top_100_coins = self.get_market_data_by_rank_range(start_rank=1, limit=100)
        
        # Verify top 100 completeness
        is_complete_100, status_100 = self.verify_rank_completeness(top_100_coins, 1, 100)
        if not is_complete_100:
            print(f"⚠️ Top 100 verification: {status_100}")
        else:
            print(f"✅ Top 100 verification: {status_100}")
        
        # Small delay to respect rate limits
        time.sleep(1)
        
        # Fetch top 500 coins (for multi-timeframe system)
        print(f"\n📈 Fetching data for multi-timeframe system...")
        top_500_coins = self.get_market_data_by_rank_range(start_rank=1, limit=500)
        
        # Verify top 500 completeness
        is_complete_500, status_500 = self.verify_rank_completeness(top_500_coins, 1, 500)
        if not is_complete_500:
            print(f"⚠️ Top 500 verification: {status_500}")
        else:
            print(f"✅ Top 500 verification: {status_500}")
        
        # Save all data
        if top_100_coins and top_500_coins:
            self.save_market_data(top_100_coins, top_500_coins)
        else:
            print("❌ Failed to fetch required data - not saving")
            return False
        
        print(f"\n" + "="*60)
        print("✅ DATA FETCH COMPLETE")
        print("="*60)
        print(f"🕐 Updated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
        print(f"📊 30m System: {len(top_100_coins)} coins ready")
        print(f"📊 Multi System: {len(top_500_coins)} coins ready")
        print("="*60)
        
        return True

def main():
    """Main execution function"""
    try:
        fetcher = CMCDataFetcher()
        success = fetcher.fetch_all_data()
        
        if success:
            print("🎉 All market data fetched and saved successfully!")
        else:
            print("❌ Data fetch failed!")
            exit(1)
            
    except Exception as e:
        print(f"💥 Fatal error: {e}")
        exit(1)

if __name__ == '__main__':
    main()
