import asyncio
from binance.client import AsyncClient
from pathlib import Path


def make_path(path):
    Path(path).mkdir(parents=True, exist_ok=True)


async def create_client(api_key, api_secret, url):
    client = await AsyncClient(api_key, api_secret)
    client.API_URL = url
    return client


async def get_balance(client, asset):
    balance = await client.futures_account_balance()
    for bal in balance:
        if bal["asset"] == asset:
            return float(bal["balance"])


async def get_precision(tick):
    tick_str = str(tick)
    precision = tick_str.split(".")
    if len(precision) > 1:
        precision = list(precision[1])
        while precision and precision[-1] == "0":
            precision.pop()
    else:
        precision = []
    return len(precision)


async def get_account_future_position(self, client):
    positions = await client.futures_position_information()
    unpnl = {}
    total_unpnl = 0
    order_size = {}

    for pos in positions:
        size = float(pos["positionAmt"])
        if size != 0:
            symbol = pos["symbol"]
            unpnl[symbol] = float(pos["unrealizedPnl"])
            total_unpnl += unpnl[symbol]
            order_size[symbol] = size
    return unpnl, total_unpnl, order_size
