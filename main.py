from bot import Bot, AssetPair, Asset, logger
from bot.data import Loader
from os import getenv
import asyncio
import datetime

import json
import time


import random
from dataclasses import dataclass
import threading


@dataclass
class Order:
    sent: bool = False
    
buy_price = 0
sell_price = 0
logger.add(f"file_{datetime.datetime.now().date()}.log")




async def currency_watch(asset, bot):

    loader = Loader(asset.symbol, bot).initialize_iterator()
    
    async for item in loader:
        
        data = item.get('data')
        asset.bid_p, asset.bid_q = float(data.get('bid')), float(data.get('bidSize'))
        asset.ask_p, asset.ask_q = float(data.get('ask')), float(data.get('askSize'))
        
        
async def buy(bot, asset, volume, client_id, reduce_only=False):
    return await bot.buy(market=asset.symbol,
                  price=asset.ask_p,
                  size=volume,
                  client_id=client_id,
                  reduce_only=reduce_only,
                  type='limit')
    
async def sell(bot, asset, volume, client_id, reduce_only=False):
    
    return await bot.sell(market=asset.symbol,
                  price=asset.bid_p,
                  size=volume,
                  reduce_only=reduce_only,
                  client_id=client_id,
                  type='limit')

@logger.catch
async def create_orders(buy, sell):
    result = await asyncio.gather(
        asyncio.create_task(buy),
        asyncio.create_task(sell),
    )
    return result
    
        
    
async def check_opportunities(asset_pair, bot, order):
    
    # Giving some time to download data from stream
    await asyncio.sleep(3)
    
    symbol = asset_pair.first_leg.symbol.replace("/USD", "")
    while True:
        # Context switching during the analyze procedure
        await asyncio.sleep(0.001)
        basis = await asset_pair.get_basis()
        
        #print(basis)
        
        if basis >= 0.14 and order.sent == False:
            order.sent = True
            
            buy_price = asset_pair.first_leg.ask_p
            sell_price = asset_pair.second_leg.bid_p
            
            logger.success(f'Profitable entry! {asset_pair.first_leg.ask_p}, {asset_pair.second_leg.bid_p}')
            
            client_ids = [symbol + str(int(random.random() * 10000)), symbol + str(int(random.random() * 10000))]
            result = await create_orders(
                buy(bot, asset_pair.first_leg, asset_pair.volume, client_ids[0]),
                sell(bot, asset_pair.second_leg, asset_pair.volume, client_ids[1])
            )
            
            with open('open.json', 'a+') as fobj:
                json.dump(result, fobj, indent=4)
            
            
            while True:
                first_leg = await bot.get_order_status_by_client_id(client_ids[0])
                second_leg = await bot.get_order_status_by_client_id(client_ids[1])
            
                await asyncio.sleep(3)
                
                   
                if first_leg.get('result'):
                    if first_leg.get('result').get('status') == 'closed':
                        if second_leg.get('result'):
                            if first_leg.get('result').get('status') == 'closed':
                                logger.success(f'Result: {result}')
                                break

                await asyncio.sleep(3)
            
            
        elif basis <= 0.02 and order.sent == True:
            
            logger.success(f'Profitable exit! {asset_pair.second_leg.ask_p}, {asset_pair.first_leg.bid_p}')
            
            client_ids = [symbol + str(int(random.random() * 10000)), symbol + str(int(random.random() * 10000))]

            
            logger.info(client_ids[0])
            logger.info(client_ids[1])
            
            result = await create_orders(
                buy(bot, asset_pair.second_leg, asset_pair.volume, client_ids[0], reduce_only=True),
                sell(bot, asset_pair.first_leg, asset_pair.volume * 0.9999, client_ids[1])
            )
            
            
            with open('close.json', 'a+') as fobj:
                json.dump(result, fobj, indent=4)
                
            continue_ = False
            while not continue_:
                
                await asyncio.sleep(3)
                first_leg = await bot.get_order_status_by_client_id(client_ids[0])
                second_leg = await bot.get_order_status_by_client_id(client_ids[1])
                
            
                
                if first_leg.get('result'):
                    if first_leg.get('result').get('status') == 'closed':
                        if second_leg.get('result'):
                            if second_leg.get('result').get('status') == 'closed':
                                order.sent = False
                                logger.success(f'[CLOSED] {-buy_price + sell_price - asset_pair.second_leg.ask_p + asset_pair.first_leg.bid_p}')
                                
                                break

                await asyncio.sleep(3)
            
            
            



async def main(asset_pair, first_leg, second_leg, bot):
    
    order = Order()

    await asyncio.gather(
        asyncio.create_task(currency_watch(first_leg, bot)),
        asyncio.create_task(currency_watch(second_leg, bot)),
        asyncio.create_task(check_opportunities(asset_pair, bot, order))
        )


def mainThread(pair, bot):
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    first_leg = Asset(symbol=pair["spot"])
    second_leg = Asset(symbol=pair["perp"])
    asset_pair = AssetPair(first_leg=first_leg, second_leg=second_leg)

    loop.run_until_complete(main(asset_pair, asset_pair.first_leg, asset_pair.second_leg, bot))

if __name__ == "__main__":

    symbolFile = open("symbols.json","r")
    pairs = json.load(symbolFile)
    
    keyFile = open("config.json", "r")
    keys = json.load(keyFile) 

    bot = Bot(keys["public_key"], keys["secret_key"], name = keys["sub_acc_name"])

    #pairs = [('MEDIA/USD', 'MEDIA-PERP'), ('BNB/USD', 'BNB-PERP'), ('DOT/USD', 'DOT-PERP')]

    threads = []

    for pair in pairs[:10]:
        print(pair)
        x = threading.Thread(target=mainThread, args=(pair, bot))
        threads.append(x)
        x.start()

    for thread in threads:
        thread.join()
    
    # bot = Bot(keys["public_key"], keys["secret_key"])
    # first_leg = Asset(symbol='ETH/USD')
    # second_leg = Asset(symbol='ETH-PERP')
    # asset_pair = AssetPair(first_leg=first_leg, second_leg=second_leg)
    
    
    # asyncio.run(main(asset_pair, asset_pair.first_leg, asset_pair.second_leg, bot))