import websockets
from bot.constants import ws
import time
import json
import hmac
from bot import logger


class Loader:
    def __init__(self, currency, bot) -> None:
        self._key = bot.key
        self._secret = bot.secret
        if hasattr(bot, "name"):
            self._name = bot.name
        else:
            self._name = None
        self._currency = currency
        
    async def initialize_iterator(self):
        ts = int(time.time() * 1000)
        
        try:
            async with websockets.connect(ws) as client:
                
                # Logging in websocket server
                if self._name:
                    await client.send(
                        json.dumps(
                            {'op': 'login', 'args': {'key': self._key,'subaccount': self._name,'sign': hmac.new(
                                self._secret.encode(), f'{ts}websocket_login'.encode(), 'sha256').hexdigest(), 'time': ts}}).encode())
                else:
                    await client.send(
                        json.dumps(
                            {'op': 'login', 'args': {'key': self._key, 'sign': hmac.new(
                                self._secret.encode(), f'{ts}websocket_login'.encode(), 'sha256').hexdigest(), 'time': ts}}).encode())
                
                # Subscribing for data listening
                if isinstance(self._currency, list):
                    for currency in self._currency:
                        await client.send(json.dumps({'op': 'subscribe', 'channel': 'ticker', 'market': currency}))
                
                else:
                    # Subscribing for data listening
                    await client.send(json.dumps({'op': 'subscribe', 'channel': 'ticker', 'market': self._currency}))
                
                # If we didnt subscribe for a stream, than there is a problem
                resp = json.loads(await client.recv())
                if not resp.get('type') == 'subscribed':
                    print(resp)
                    logger.error('Didnt subscribe')
                    return
                
                # Listening to the stream of data
                while True:
                    yield json.loads(await client.recv())
        except Exception as e:
            logger.error(e, repr(e))
                