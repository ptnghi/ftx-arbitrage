from os import name
from loguru import logger
from dataclasses import dataclass
import time
import hmac
import aiohttp
import json
from bot.constants import ws, rest




class Bot:
    def __init__(self, key, secret, name = None) -> None:
        self.key = key
        self.secret = secret
        if name:
            self.name = name
        
    async def _send_request(self, api, method, params={}):
        
        # All requests require ts
        ts = int(time.time() * 1000)
        signature_payload = f'{ts}{method.upper()}{api}'.encode()
        
        # If params passed, adding to query string
        if params:
            signature_payload += json.dumps(params).encode()
            
        # Security measures by FTX
        signature = hmac.new(self.secret.encode(), signature_payload, 'sha256').hexdigest()
        async with aiohttp.ClientSession(headers={'FTX-KEY': self.key, 'FTX-SIGN': signature, 'FTX-TS': str(ts)}) as session:
            if params:
                async with session.request(method, rest + api, json=params) as response:
                    return await response.json()
            else:
                async with session.request(method, rest + api) as response:
                    return await response.json()
                
    async def getSubAccBalance(self):
        if not name:
            return
        api = '/subaccounts/' + self.name + '/balances'
        method = 'GET'
        return await self._send_request(api, method)
                
    async def buy(self,
                  market,
                  price,
                  size,
                  client_id,
                  reduce_only=False, # True, when we want to close previously sold positions
                  type='limit'):
        
        api = '/api/orders'
        method = 'POST'
        params = {"market": market,
                  "side": "buy",
                  "price": price,
                  "type": type,
                  "size": size,
                  "clientId": client_id,
                  "reduceOnly": reduce_only}
        return await self._send_request(api, method, params)
    
    async def sell(self, 
                   market,
                   price,
                   size,
                   client_id,
                   reduce_only, # True, when we want to close previously bought positions
                   type='limit'):
        
        api = '/api/orders'
        method = 'POST'
        params = {"market": market,
                  "side": "sell",
                  "price": price,
                  "type": type,
                  "size": size,
                  "clientId": client_id,
                  "reduceOnly": reduce_only}
        return await self._send_request(api, method, params)
    
    async def cancel_order_by_client_id(self, client_id):
        api = f'/api/orders/by_client_id/{client_id}'
        method = 'DELETE'
        return await self._send_request(api, method, {})
    
    async def get_order_status_by_client_id(self, client_id):
        api = f'/api/orders/by_client_id/{client_id}'
        method = 'GET'
        return await self._send_request(api, method, {})

        

@dataclass
class Asset:
    symbol: str
    
    bid_p: float = None
    bid_q: float = None
    ask_p: float = None
    ask_q: float = None

    
@dataclass
class AssetPair:
    first_leg: Asset
    second_leg: Asset
    
    volume: float = 0.004
    fees: float = 0.0195 * 4
    
    def __getitem__(self, value):
        return self.__dict__.get(value)
    
    async def get_basis(self):
        if (type(self.second_leg.bid_p) != 'float' or type(self.first_leg.ask_p) != 'float'):
            return 0
        return 100 * (self.second_leg.bid_p - self.first_leg.ask_p) / self.first_leg.ask_p

    async def check_buy_opportunity(self):
        return await self.get_basis() if await self.get_basis() > self.fees else 0


@dataclass
class Order:
    sent: bool = True
    