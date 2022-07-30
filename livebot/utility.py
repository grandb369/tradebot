import asyncio
from binance.client import Client
from pathlib import Path


def make_path(path):
    Path(path).mkdir(parents=True, exist_ok=True)


async def create_client(api_key, api_secret, url):
    client = await Client(api_key, api_secret)
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
