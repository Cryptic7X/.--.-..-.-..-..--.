#!/usr/bin/env python3
"""
CoinMarketCap Data Fetcher for Multi-System Support
Fetches top 100 and filtered high-volume cryptocurrency data
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
        
        # Volume filter settings
        self.min_volume_24h = 20_000_000  # $20M minimum 24h volume
        
        print(f"📊 CMC Data Fetcher initialized")
        print(f"💰 Volume filter: ${self.min_volume_24h:,.0f} minimum 24h volume")
    
    def get_cryptocurrency_map(self):
        """Get complete cryptocurrency map for symbol lookup (optional)"""
        print("🗺️ Fetching cryptocurrency map...")
        
        url = f"{self.base_url}/cryptocurrency/map"
        params = {
            'listing_status': 'active',
            'limit': 5000  # Reduced limit to avoid 400 errors
        }
        
        try:
            response = requests.get(url, headers=self.headers, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            crypto_map = {crypto['id']: crypto for crypto in data['data']}
            
            print(f"✅ Map loaded: {len(crypto_map)} coins")
            return crypto_map
            
        except Exception as e:
            print(f"⚠️ Cryptocurrency map failed: {e} (continuing without map)")
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
    
    def filter_by_volume(self, coins, min_volume=None):
        """Filter coins by minimum 24h volume"""
        if min_volume is None:
            min_volume = self.min_volume_24h
        
        original_count = len(coins)
        filtered_coins = [
            coin for coin in coins 
            if coin.get('volume_24h', 0) >= min_volume
        ]
        
        filtered_count = len(filtered_coins)
        removed_count = original_count - filtered_count
        
        print(f"🔍 Volume filter (≥${min_volume:,.0f}): {filtered_count} coins kept, {removed_count} removed")
        
        if filtered_coins:
            min_vol = min(coin['volume_24h'] for coin in filtered_coins)
            max_vol = max(coin['volume_24h'] for coin in filtered_coins)
            print(f"📊 Volume range: ${min_vol:,.0f} - ${max_vol:,.0f}")
        
        return filtered_coins
    
    def verify_data_quality(self, coins, system_name):
        """Verify data quality without strict rank requirements"""
        if not coins:
            return False, "No coins data"
        
        # Check for reasonable data
        valid_coins = [
            coin for coin in coins 
            if coin.get('current_price', 0) > 0 and 
               coin.get('market_cap', 0) > 0 and
               coin.get('volume_24h', 0) > 0
        ]
        
        quality_ratio = len(valid_coins) / len(coins)
        
        if quality_ratio < 0.95:  # 95% of coins should have valid data
            return False, f"Low data quality: {quality_ratio*100:.1f}% valid"
        
        ranks = [coin['rank'] for coin in coins if coin.get('rank')]
        rank_range = f"{min(ranks)}-{max(ranks)}" if ranks else "unknown"
        
        return True, f"Good quality: {len(valid_coins)} valid coins, ranks {rank_range}"
    
    def save_market_data(self, top_100_coins, filtered_multi_coins):
        """Save market data to cache with volume filtering info"""
        cache_dir = os.path.join(os.path.dirname(__file__), '..', 'cache')
        os.makedirs(cache_dir, exist_ok=True)
        
        current_time = datetime.utcnow().isoformat()
        
        # Save data for 30m system (top 100 coins)
        system_30m_data = {
            'updated_at': current_time,
            'system': '30m',
            'method': 'cryptocurrency_listings_latest',
            'total_coins': len(top_100_coins),
            'volume_filter': 'none',
            'rank_range': f"{top_100_coins[0]['rank']}-{top_100_coins[-1]['rank']}" if top_100_coins else 'none',
            'coins': top_100_coins
        }
        
        # Save data for multi-timeframe system (volume-filtered coins)
        system_multi_data = {
            'updated_at': current_time,
            'system': 'multi',
            'method': 'cryptocurrency_listings_latest_filtered',
            'total_coins': len(filtered_multi_coins),
            'volume_filter': f'min_${self.min_volume_24h:,.0f}_24h',
            'volume_stats': {
                'min_volume': min(coin['volume_24h'] for coin in filtered_multi_coins) if filtered_multi_coins else 0,
                'max_volume': max(coin['volume_24h'] for coin in filtered_multi_coins) if filtered_multi_coins else 0,
            },
            'rank_range': f"{min(coin['rank'] for coin in filtered_multi_coins)}-{max(coin['rank'] for coin in filtered_multi_coins)}" if filtered_multi_coins else 'none',
            'coins': filtered_multi_coins
        }
        
        # Write cache files
        with open(os.path.join(cache_dir, 'market_data_30m.json'), 'w') as f:
            json.dump(system_30m_data, f, indent=2)
        
        with open(os.path.join(cache_dir, 'market_data_multi.json'), 'w') as f:
            json.dump(system_multi_data, f, indent=2)
        
        print(f"💾 Saved market data:")
        if top_100_coins:
            print(f"   30m system: {len(top_100_coins)} coins (ranks {top_100_coins[0]['rank']}-{top_100_coins[-1]['rank']})")
        if filtered_multi_coins:
            min_rank = min(coin['rank'] for coin in filtered_multi_coins)
            max_rank = max(coin['rank'] for coin in filtered_multi_coins)
            print(f"   Multi system: {len(filtered_multi_coins)} high-volume coins (ranks {min_rank}-{max_rank})")
    
    def fetch_all_data(self):
        """Fetch complete dataset with volume filtering for multi-timeframe system"""
        print("="*70)
        print("🚀 COINMARKETCAP DATA FETCHER WITH VOLUME FILTERING")
        print("="*70)
        
        # Optional cryptocurrency map (continues if fails)
        crypto_map = self.get_cryptocurrency_map()
        
        # Fetch top 100 coins (for 30m system - no volume filter)
        print(f"\n📈 Fetching data for 30m system (top 100, no volume filter)...")
        top_100_coins = self.get_market_data_by_rank_range(start_rank=1, limit=100)
        
        # Verify top 100 quality
        is_quality_100, status_100 = self.verify_data_quality(top_100_coins, "30m")
        if not is_quality_100:
            print(f"⚠️ 30m data quality: {status_100}")
        else:
            print(f"✅ 30m data quality: {status_100}")
        
        # Small delay to respect rate limits
        time.sleep(1)
        
        # Fetch top 500 coins (for multi-timeframe system)
        print(f"\n📈 Fetching data for multi-timeframe system...")
        coins_500 = self.get_market_data_by_rank_range(start_rank=1, limit=500)
        
        # ✅ Apply volume filter for multi-timeframe system
        print(f"\n🔍 Applying volume filter for day trading suitability...")
        filtered_multi_coins = self.filter_by_volume(coins_500, self.min_volume_24h)
        
        # Verify filtered data quality
        is_quality_multi, status_multi = self.verify_data_quality(filtered_multi_coins, "multi")
        if not is_quality_multi:
            print(f"⚠️ Multi-timeframe data quality: {status_multi}")
        else:
            print(f"✅ Multi-timeframe data quality: {status_multi}")
        
        # Save all data
        if top_100_coins and filtered_multi_coins:
            self.save_market_data(top_100_coins, filtered_multi_coins)
        else:
            print("❌ Failed to fetch required data - not saving")
            return False
        
        print(f"\n" + "="*70)
        print("✅ DATA FETCH COMPLETE WITH VOLUME FILTERING")
        print("="*70)
        print(f"🕐 Updated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
        print(f"📊 30m System: {len(top_100_coins)} coins (all ranks 1-100)")
        print(f"📊 Multi System: {len(filtered_multi_coins)} high-volume coins (≥${self.min_volume_24h:,.0f})")
        print(f"💰 Volume filter removed {len(coins_500) - len(filtered_multi_coins)} low-volume coins")
        print("="*70)
        
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
