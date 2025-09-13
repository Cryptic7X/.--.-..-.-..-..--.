#!/usr/bin/env python3
"""
CoinMarketCap Market Data Fetcher - FINAL FIXED VERSION
- Handles CoinMarketCap's actual API limits correctly
- Uses proper pagination with error handling
- Gets maximum available coins for filtering
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
    
    def fetch_all_coins_properly(self):
        """Fetch coins using proper pagination - handles CMC's actual limits"""
        print("🚀 Fetching ALL available coins using corrected pagination...")
        
        all_coins = []
        coins_per_page = 200  # Increased page size
        current_start = 1
        page_number = 1
        max_attempts = 50  # Maximum pages to try
        
        while page_number <= max_attempts:
            print(f"   📄 Page {page_number}: Requesting coins {current_start} to {current_start + coins_per_page - 1}")
            
            url = f"{self.base_url}/cryptocurrency/listings/latest"
            params = {
                'start': current_start,
                'limit': coins_per_page,
                'sort': 'market_cap',
                'sort_dir': 'desc',
                'cryptocurrency_type': 'coins',
                'convert': 'USD'
            }
            
            try:
                response = self.session.get(url, params=params, timeout=60)
                
                if response.status_code == 429:
                    print(f"   ⏳ Rate limited, waiting 60 seconds...")
                    time.sleep(60)
                    continue
                    
                response.raise_for_status()
                data = response.json()
                
                page_coins_raw = data.get('data', [])
                
                if not page_coins_raw:
                    print(f"   📭 No more coins returned (stopped at page {page_number})")
                    break
                
                # Process coins and filter out invalid data
                page_coins_valid = []
                for coin in page_coins_raw:
                    try:
                        # Skip coins with missing critical data
                        if (coin.get('quote', {}).get('USD', {}).get('market_cap') is None or 
                            coin.get('quote', {}).get('USD', {}).get('volume_24h') is None):
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
                        page_coins_valid.append(coin_data)
                    except (KeyError, TypeError) as e:
                        continue  # Skip malformed coin data
                
                # Check for duplicate coins (indicates we've reached the end)
                new_symbols = {coin['symbol'] for coin in page_coins_valid}
                existing_symbols = {coin['symbol'] for coin in all_coins}
                overlap = len(new_symbols.intersection(existing_symbols))
                
                if overlap > len(new_symbols) * 0.5:  # More than 50% overlap
                    print(f"   🔄 High overlap detected ({overlap} duplicates), stopping pagination")
                    break
                
                all_coins.extend(page_coins_valid)
                print(f"   ✅ Added {len(page_coins_valid)} valid coins (total: {len(all_coins)})")
                
                # Check if we got fewer coins than requested (indicates end of data)
                if len(page_coins_raw) < coins_per_page:
                    print(f"   📭 Received fewer coins than requested ({len(page_coins_raw)} < {coins_per_page})")
                    break
                
                # Update for next iteration
                current_start += len(page_coins_valid)
                page_number += 1
                
                # Rate limiting between pages
                time.sleep(2)
                
            except requests.exceptions.RequestException as e:
                print(f"   ❌ Request error on page {page_number}: {e}")
                if "429" in str(e):
                    print(f"   ⏳ Rate limited, waiting 60 seconds...")
                    time.sleep(60)
                    continue
                else:
                    break
            except Exception as e:
                print(f"   ❌ Unexpected error on page {page_number}: {e}")
                break
        
        # Remove any duplicate coins that might have slipped through
        unique_coins = []
        seen_symbols = set()
        for coin in all_coins:
            if coin['symbol'] not in seen_symbols:
                unique_coins.append(coin)
                seen_symbols.add(coin['symbol'])
        
        print(f"✅ Pagination complete:")
        print(f"   • Raw coins fetched: {len(all_coins)}")
        print(f"   • Unique coins: {len(unique_coins)}")
        print(f"   • Pages processed: {page_number - 1}")
        
        return unique_coins
    
    def extract_top_100(self, all_coins):
        """Extract top 100 coins for 15m system"""
        top_100 = all_coins[:100] if len(all_coins) >= 100 else all_coins
        print(f"📊 Extracted top {len(top_100)} coins for 15m system")
        return top_100
    
    def apply_multi_criteria_verbose(self, all_coins):
        """Apply market cap and volume filtering with detailed breakdown"""
        min_market_cap = 50_000_000   # $50M
        min_volume_24h = 20_000_000   # $20M
        
        print(f"🔍 Applying multi-timeframe filtering criteria...")
        print(f"   Market cap >= ${min_market_cap:,}")
        print(f"   Volume 24h >= ${min_volume_24h:,}")
        
        # Detailed breakdown
        qualified_coins = []
        below_market_cap = 0
        below_volume = 0
        both_criteria_failed = 0
        
        # Sample some coins for debugging
        sample_coins = all_coins[:10] if len(all_coins) >= 10 else all_coins
        print(f"\n   📋 Sample of first 10 coins for debugging:")
        for i, coin in enumerate(sample_coins, 1):
            mc = coin.get('market_cap', 0) / 1_000_000  # Convert to millions
            vol = coin.get('volume_24h', 0) / 1_000_000  # Convert to millions
            status = "✅ PASS" if mc >= 50 and vol >= 20 else "❌ FAIL"
            print(f"      {i:2d}. {coin['symbol']:8s} | Cap: ${mc:8.1f}M | Vol: ${vol:8.1f}M | {status}")
        
        for coin in all_coins:
            market_cap = coin.get('market_cap', 0)
            volume_24h = coin.get('volume_24h', 0)
            
            mc_pass = market_cap >= min_market_cap
            vol_pass = volume_24h >= min_volume_24h
            
            if mc_pass and vol_pass:
                qualified_coins.append(coin)
            elif not mc_pass and not vol_pass:
                both_criteria_failed += 1
            elif not mc_pass:
                below_market_cap += 1
            elif not vol_pass:
                below_volume += 1
        
        print(f"\n🔍 Detailed filtering results:")
        print(f"   Total coins processed: {len(all_coins):,}")
        print(f"   Below ${min_market_cap/1_000_000:.0f}M market cap only: {below_market_cap:,}")
        print(f"   Below ${min_volume_24h/1_000_000:.0f}M volume only: {below_volume:,}")
        print(f"   Failed both criteria: {both_criteria_failed:,}")
        print(f"   ✅ Qualified coins: {len(qualified_coins):,}")
        
        if len(qualified_coins) < 100:
            print(f"\n⚠️  Warning: Only {len(qualified_coins)} coins qualify!")
            print(f"   Consider adjusting criteria:")
            print(f"   • Lower market cap threshold (e.g., $25M)")
            print(f"   • Lower volume threshold (e.g., $10M)")
        
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
        
        print(f"🔍 Blocked coin filtering:")
        print(f"   Removed: {blocked_count} coins")
        if blocked_symbols:
            print(f"   Symbols: {', '.join(blocked_symbols[:10])}")
            if len(blocked_symbols) > 10:
                print(f"   ... and {len(blocked_symbols) - 10} more")
                
        return filtered
    
    def save_market_data(self, top_100_coins, multi_coins):
        """Save market data to cache with comprehensive metadata"""
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
            'sample_coins': multi_coins[:5] if len(multi_coins) >= 5 else multi_coins,
            'coins': multi_coins
        }
        
        with open(os.path.join(cache_dir, 'market_data_multi.json'), 'w') as f:
            json.dump(system_multi_data, f, indent=2)
        
        print(f"\n💾 Saved market data:")
        print(f"   📊 15m system: {len(top_100_coins)} coins")
        print(f"   📈 Multi system: {len(multi_coins)} coins")

def main():
    fetcher = CoinMarketCapFetcher()
    
    if not fetcher.api_key:
        print("❌ COINMARKETCAP_API_KEY not found in environment variables")
        print("   Add this to your GitHub Secrets:")
        print("   COINMARKETCAP_API_KEY=your_api_key_here")
        return
    
    print("="*80)
    print("🔥 COMPREHENSIVE MARKET DATA FETCH - FINAL VERSION")
    print("="*80)
    
    # Load blocked coins
    blocked_coins = fetcher.load_blocked_coins()
    
    # Fetch ALL available coins using corrected pagination
    print(f"\n📡 FETCHING COINS FROM COINMARKETCAP API")
    print("="*60)
    all_coins = fetcher.fetch_all_coins_properly()
    
    if not all_coins:
        print("❌ No coins fetched - check API connection and key")
        return
    
    if len(all_coins) < 1000:
        print(f"\n⚠️  Warning: Only {len(all_coins)} coins fetched")
        print("   This may indicate API limits or connectivity issues")
    
    print(f"\n📊 PREPARING DATASETS FOR BOTH SYSTEMS")
    print("="*60)
    
    # System 1: Extract top 100 coins
    top_100_raw = fetcher.extract_top_100(all_coins)
    top_100_final = fetcher.filter_blocked_coins(top_100_raw, blocked_coins)
    
    # System 2: Apply filtering criteria with verbose output
    multi_qualified = fetcher.apply_multi_criteria_verbose(all_coins)
    multi_final = fetcher.filter_blocked_coins(multi_qualified, blocked_coins)
    
    # Save both datasets
    fetcher.save_market_data(top_100_final, multi_final)
    
    print(f"\n" + "="*80)
    print("✅ MARKET DATA FETCH COMPLETED!")
    print("="*80)
    print(f"📈 Final Results:")
    print(f"   • Total coins from API: {len(all_coins):,}")
    print(f"   • 15m system dataset: {len(top_100_final)} coins")
    print(f"   • Multi-timeframe dataset: {len(multi_final)} coins")
    
    print(f"\n📝 System Usage:")
    print(f"   • 15m system analyzes ONLY its {len(top_100_final)} coins")
    print(f"   • Multi-timeframe system analyzes ONLY its {len(multi_final)} coins")
    print(f"   • Systems operate completely independently")
    
    # Provide recommendations
    if len(multi_final) >= 200:
        print(f"\n🎉 SUCCESS: Multi-timeframe system has sufficient coins!")
    elif len(multi_final) >= 100:
        print(f"\n✅ ACCEPTABLE: Multi-timeframe system has reasonable coin count")
    else:
        print(f"\n💡 RECOMMENDATIONS to increase multi-timeframe coins:")
        print(f"   • Lower market cap to $25M (min_market_cap = 25_000_000)")
        print(f"   • Lower volume to $10M (min_volume_24h = 10_000_000)")
        print(f"   • This should significantly increase qualifying coins")
    
    print("="*80)

if __name__ == '__main__':
    main()
