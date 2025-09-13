#!/usr/bin/env python3
"""
CoinMarketCap Data Fetcher for Dual Trading Systems - CORRECTED
Fixed endpoint: /v1/cryptocurrency/listings/latest
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
    
    def fetch_top_100_coins(self):
        """
        Fetch top 100 coins by market cap for System 1 (15-minute analysis)
        """
        print("🚀 Fetching top 100 coins for 15-minute system...")
        
        # CORRECTED ENDPOINT - Added /cryptocurrency/
        url = f"{self.base_url}/cryptocurrency/listings/latest"
        params = {
            'start': 1,
            'limit': 100,
            'sort': 'market_cap',
            'convert': 'USD'
        }
        
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            coins = data.get('data', [])
            
            print(f"✅ Fetched {len(coins)} top coins for 15m system")
            return self.format_coins_data(coins)
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Failed to fetch top 100 coins: {e}")
            return []
    
    def fetch_filtered_coins(self):
        """
        Fetch filtered coins for System 2 (multi-timeframe analysis)
        Market cap ≥ $50M, Volume ≥ $20M
        """
        print("🚀 Fetching filtered coins for multi-timeframe system...")
        
        # CORRECTED ENDPOINT - Added /cryptocurrency/
        url = f"{self.base_url}/cryptocurrency/listings/latest"
        params = {
            'start': 1,
            'limit': 1000,
            'sort': 'market_cap',
            'convert': 'USD'
        }
        
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            all_coins = data.get('data', [])
            
            # Filter coins by market cap and volume
            filtered_coins = []
            for coin in all_coins:
                try:
                    quote_usd = coin.get('quote', {}).get('USD', {})
                    market_cap = quote_usd.get('market_cap', 0) or 0
                    volume_24h = quote_usd.get('volume_24h', 0) or 0
                    
                    if market_cap >= 50_000_000 and volume_24h >= 20_000_000:
                        filtered_coins.append(coin)
                        
                except (KeyError, TypeError):
                    continue
            
            print(f"✅ Filtered to {len(filtered_coins)} coins for multi-timeframe system")
            print(f"   (Market cap ≥ $50M, Volume ≥ $20M)")
            
            return self.format_coins_data(filtered_coins)
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Failed to fetch filtered coins: {e}")
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
        """
        Run complete market data fetch for both systems
        """
        print("="*80)
        print("📊 COINMARKETCAP DATA FETCH")
        print("="*80)
        print(f"🕐 Fetch Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
        print(f"🔑 API Key: {self.api_key[:8]}...{self.api_key[-4:]}")
        
        try:
            # Fetch data for System 1 (15-minute)
            print("\n📈 System 1: Top 100 Coins")
            top_100_coins = self.fetch_top_100_coins()
            if top_100_coins:
                self.save_market_data(top_100_coins, '15m')
            else:
                print("❌ Failed to fetch top 100 coins")
            
            # Rate limiting between API calls
            print("⏳ Waiting 2 seconds between API calls...")
            time.sleep(2)
            
            # Fetch data for System 2 (multi-timeframe)
            print("\n📊 System 2: Filtered Coins")
            filtered_coins = self.fetch_filtered_coins()
            if filtered_coins:
                self.save_market_data(filtered_coins, 'multi')
            else:
                print("❌ Failed to fetch filtered coins")
            
            print(f"\n✅ Market data fetch completed successfully")
            print(f"📊 15m System: {len(top_100_coins)} coins")
            print(f"📊 Multi System: {len(filtered_coins)} coins")
            
        except Exception as e:
            error_msg = f"Critical error in market data fetch: {str(e)}"
            print(f"💥 {error_msg}")
            
            # Send admin alert
            try:
                from ..alerts.telegram_multi import send_admin_alert
                send_admin_alert("Market Data Fetch Failed", error_msg)
            except ImportError:
                pass

def load_market_data(system_type):
    """
    Load cached market data for specified system
    system_type: '15m' or 'multi'
    """
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
        
        # Check if data is fresh (within 24 hours)
        updated_at = datetime.fromisoformat(data.get('updated_at', ''))
        age_hours = (datetime.utcnow() - updated_at).total_seconds() / 3600
        
        if age_hours > 24:
            print(f"⚠️ Market data is {age_hours:.1f} hours old, consider refreshing")
        
        coins = data.get('coins', [])
        print(f"📊 Loaded {len(coins)} coins for {system_type} system")
        
        return coins
        
    except Exception as e:
        print(f"❌ Error loading market data: {e}")
        return []

if __name__ == '__main__':
    fetcher = CoinMarketCapFetcher()
    fetcher.run_data_fetch()
