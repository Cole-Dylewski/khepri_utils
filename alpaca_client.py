

class alpaca_client():
    def __init__(self, apiKey, apiSecret, base_url = r'https://paper-api.alpaca.markets', api_version = 'v2', premium = False ,printVerbose = False):
        
        global tradeapi, requests
        
        import alpaca_trade_api as tradeapi
        import requests
        
        self.apiKey = apiKey
        self.apiSecret = apiSecret
        self.base_url = base_url
        self.api_version = api_version
        
        self.premium = premium
        
    def __enter__(self, ):
        self.login()
        return self
    
    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.logout()
        
    def login(self):
        try:
            api = tradeapi.REST(
                key_id=self.apiKey,
                secret_key=self.apiSecret,
                base_url=self.base_url,
                api_version=self.api_version
            )
            self.account = api.get_account()
            print("LOGIN SUCCESSFUL")
            #print(api.get_account())


            return True
        except Exception as e:
            print("LOGIN FAILED")
            print(e)

            return False
        return
    
    def logout(self):
        return