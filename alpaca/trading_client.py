import logging
import requests
import pandas as pd
from datetime import datetime
import time

from khepri_utils.alpaca.api import accounts, assets, orders, portfolio, watchlists, calendar, clock, crypto  # Adjust import as necessary
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class TraderClient:
    def __init__(self, api_key, api_secret, base_url=r'https://paper-api.alpaca.markets', api_version='v2', premium=False, printVerbose=False):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url
        self.api_version = api_version
        self.premium = premium
        self.printVerbose = printVerbose

    def get_account(self):
        """Retrieve account information."""
        return accounts.get_account(self.api_key, self.api_secret, self.base_url, self.api_version)

    def get_account_configurations(self):
        """Retrieve account configurations."""
        return accounts.get_account_configurations(self.api_key, self.api_secret, self.base_url, self.api_version)

    def update_account_configurations(self, **configurations):
        """Update account configurations."""
        return accounts.update_account_configurations(self.api_key, self.api_secret, self.base_url, self.api_version, **configurations)

    def get_account_activities(self, activity_type=None, date=None, until=None, after=None, direction=None, page_size=None, page_token=None):
        """Retrieve account activities."""
        return accounts.get_account_activities(self.api_key, self.api_secret, self.base_url, self.api_version, activity_type, date, until, after, direction, page_size, page_token)

    def list_assets(self, status='active', asset_class='us_equity'):
        """List assets based on status and asset class."""
        return assets.list_assets(self.api_key, self.api_secret, self.base_url, self.api_version, status, asset_class)
    
    def get_asset(self, asset_id_or_symbol):
        """Retrieve information about a specific asset."""
        return assets.get_asset(self.api_key, self.api_secret, self.base_url, self.api_version, asset_id_or_symbol)
    
    def submit_order(self, symbol, qty, side, order_type, time_in_force, limit_price=None, stop_price=None, client_order_id=None):
        """Submit an order."""
        return orders.submit_order(self.api_key, self.api_secret, self.base_url, self.api_version, symbol, qty, side, order_type, time_in_force, limit_price, stop_price, client_order_id)
    
    def list_orders(self, status='open', limit=50, after=None, until=None, direction='desc'):
        """List orders with optional filters."""
        return orders.list_orders(self.api_key, self.api_secret, self.base_url, self.api_version, status, limit, after, until, direction)
    
    def get_order(self, order_id):
        """Retrieve details about a specific order."""
        return orders.get_order(self.api_key, self.api_secret, self.base_url, self.api_version, order_id)
    
    def replace_order(self, order_id, qty=None, limit_price=None, stop_price=None, time_in_force=None):
        """Replace an existing order."""
        return orders.replace_order(self.api_key, self.api_secret, self.base_url, self.api_version, order_id, qty, limit_price, stop_price, time_in_force)
    
    def cancel_order(self, order_id):
        """Cancel a specific order."""
        return orders.cancel_order(self.api_key, self.api_secret, self.base_url, self.api_version, order_id)
    
    def get_positions(self):
        """Retrieve the list of positions in the portfolio."""
        return portfolio.get_positions(self.api_key, self.api_secret, self.base_url, self.api_version)
    
    def get_portfolio_history(self, period=None, timeframe=None, date_end=None, extended_hours=False):
        """Retrieve portfolio history."""
        return portfolio.get_portfolio_history(self.api_key, self.api_secret, self.base_url, self.api_version, period, timeframe, date_end, extended_hours)

    def create_watchlist(self, name, symbols):
        """Create a new watchlist."""
        return watchlists.create_watchlist(self.api_key, self.api_secret, self.base_url, self.api_version, name, symbols)

    def get_watchlist(self, watchlist_id):
        """Retrieve a specific watchlist."""
        return watchlists.get_watchlist(self.api_key, self.api_secret, self.base_url, self.api_version, watchlist_id)
    
    def update_watchlist(self, watchlist_id, symbols):
        """Update an existing watchlist."""
        return watchlists.update_watchlist(self.api_key, self.api_secret, self.base_url, self.api_version, watchlist_id, symbols)
    
    def delete_watchlist(self, watchlist_id):
        """Delete a specific watchlist."""
        return watchlists.delete_watchlist(self.api_key, self.api_secret, self.base_url, self.api_version, watchlist_id)

    def get_calendar(self, start=None, end=None):
        """Retrieve market calendar events."""
        return calendar.get_calendar(self.api_key, self.api_secret, self.base_url, self.api_version, start, end)
    
    def get_clock(self):
        """Retrieve market clock information."""
        return clock.get_clock(self.api_key, self.api_secret, self.base_url, self.api_version)

    def list_crypto_assets(self, status='active', asset_class='crypto'):
        """List cryptocurrency assets."""
        return crypto.list_crypto_assets(self.api_key, self.api_secret, self.base_url, self.api_version, status, asset_class)

    def submit_crypto_order(self, symbol, qty, side, order_type, time_in_force, limit_price=None, stop_price=None, client_order_id=None):
        """Submit a cryptocurrency order."""
        return crypto.submit_crypto_order(self.api_key, self.api_secret, self.base_url, self.api_version, symbol, qty, side, order_type, time_in_force, limit_price, stop_price, client_order_id)

    def get_funding_account(self, asset_class='crypto'):
        """Retrieve the funding account information for crypto."""
        return crypto.get_funding_account(self.api_key, self.api_secret, self.base_url, self.api_version, asset_class)

    def create_funding_account(self, crypto_address):
        """Create a funding account for crypto."""
        return crypto.create_funding_account(self.api_key, self.api_secret, self.base_url, self.api_version, crypto_address)

    def get_funding_history(self):
        """Retrieve funding history for crypto."""
        return crypto.get_funding_history(self.api_key, self.api_secret, self.base_url, self.api_version)

    def transfer_funds(self, amount, currency, direction, crypto_address=None):
        """Transfer funds to/from a cryptocurrency account."""
        return crypto.transfer_funds(self.api_key, self.api_secret, self.base_url, self.api_version, amount, currency, direction, crypto_address)

# Example usage:
if __name__ == "__main__":
    client = AlpacaClient('your_api_key', 'your_api_secret')
    
    # Example operations
    account_info = client.get_account()
    print("Account Info:", account_info)

    account_configs = client.get_account_configurations()
    print("Account Configurations:", account_configs)

    updated_configs = client.update_account_configurations(
        dtbp_check='both',
        trade_confirm_email='none',
        suspend_trade=False,
        no_shorting=False
    )
    print("Updated Account Configurations:", updated_configs)

    account_activities = client.get_account_activities()
    print("Account Activities:", account_activities)
    
    assets_list = client.list_assets()
    print("Assets List:", assets_list)
    
    asset_info = client.get_asset('AAPL')
    print("Asset Info:", asset_info)
    
    order = client.submit_order('AAPL', 1, 'buy', 'market', 'gtc')
    print("Order Submitted:", order)
    
    orders_list = client.list_orders()
    print("Orders List:", orders_list)
    
    order_info = client.get_order(order['id'])
    print("Order Info:", order_info)
    
    replaced_order = client.replace_order(order['id'], qty=2)
    print("Order Replaced:", replaced_order)
    
    cancelled_order = client.cancel_order(order['id'])
    print("Order Cancelled:", cancelled_order)
    
    positions = client.get_positions()
    print("Positions:", positions)
    
    portfolio_history = client.get_portfolio_history()
    print("Portfolio History:", portfolio_history)
    
    watchlist = client.create_watchlist('Tech Stocks', ['AAPL', 'GOOGL'])
    print("Watchlist Created:", watchlist)
    
    watchlist_info = client.get_watchlist(watchlist['id'])
    print("Watchlist Info:", watchlist_info)
    
    updated_watchlist = client.update_watchlist(watchlist['id'], ['AAPL', 'AMZN'])
    print("Watchlist Updated:", updated_watchlist)
    
    deleted_watchlist = client.delete_watchlist(watchlist['id'])
    print("Watchlist Deleted:", deleted_watchlist)
    
    calendar_events = client.get_calendar()
    print("Calendar Events:", calendar_events)
    
    market_clock = client.get_clock()
    print("Market Clock:", market_clock)
    
    crypto_assets = client.list_crypto_assets()
    print("Crypto Assets:", crypto_assets)
    
    crypto_order = client.submit_crypto_order('BTCUSD', 0.1, 'buy', 'limit', 'gtc', limit_price=30000)
    print("Crypto Order Submitted:", crypto_order)
    
    funding_account = client.get_funding_account()
    print("Funding Account:", funding_account)
    
    create_funding = client.create_funding_account('your_crypto_address')
    print("Funding Account Created:", create_funding)
    
    funding_history = client.get_funding_history()
    print("Funding History:", funding_history)
    
    transfer = client.transfer_funds(100, 'USD', 'deposit', 'your_crypto_address')
    print("Funds Transferred:", transfer)