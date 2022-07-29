from binance.client import Client
from binance import BinanceSocketManager
from sympy import re

from data_structure.orderbook import Orderbook
from data_structure.client_params import ClientParams

import asyncio
import time


async def create_client(api_key, api_secret, url):
    client = await Client(api_key, api_secret)
    client.API_URL = url
    return client


async def order_filled_socket(client, symbol, logger, err, orderbook, params):
    bsm = BinanceSocketManager(client)
    timer = time.time()
    keep_alive = 60*50
    async with bsm.user_socket() as stream:
        while True:
            # Keep socket alive
            if time.time() - timer > keep_alive:
                keep_alive = time.time()
                listen_key = await stream.get_listen_key()
                res = await client.keep_alive(listen_key)
                logger.info(
                    "Keep alive: {}, Listen Key: {}".format(res, listen_key))
            if err.status:
                await stream.close()
                await bsm.stop()
                await logger.err("order_filled_socket: socket closed")
                break
            try:
                res = await stream.recv()
            except Exception as e:
                await logger.err("order_filled_socket: {}".format(e))
                continue
            else:
                if res['e'] == "ORDER_TRADE_UPDATE":
                    order = res['o']
                    if order['s'] == symbol and order['x'] == 'FILLED':
                        regular_orders = await params.get_param("regular_orders")
                        take_profit_orders = await params.get_param("take_profit_orders")
                        stoploss_orders = await params.get_param("stoploss_orders")

                        order_id = order['i']
                        side = order['S']
                        filled_qty = order['l']
                        fill_price = float(order['L'])

                        if order_id in regular_orders:
                            regular_orders.remove(order_id)
                            await params.update_param("regular_orders", regular_orders)
                            await logger.info("Regular Order Filled: id: {}, side: {}, filled_qty: {}, fill_price: {}".format(order_id, side, filled_qty, fill_price))

                            await send_take_profit_order(client, symbol, logger, err, orderbook, params, order_id, side, filled_qty, fill_price)
                            await send_stop_loss_order(client, symbol, logger, err, orderbook, params, order_id, side, filled_qty, fill_price)
                        elif order in take_profit_orders:
                            await logger.info("Take Profit Order Filled: id: {}, side: {}, filled_qty: {}, fill_price: {}".format(order_id, side, filled_qty, fill_price))
                            prev_id = take_profit_orders[order_id]
                            st_id = stoploss_orders[prev_id]
                            del take_profit_orders[order_id]
                            del take_profit_orders[prev_id]

                            await cancel_stoploss_order(client, symbol, logger, err, orderbook, params, st_id)
                        elif order in stoploss_orders:
                            await logger.info("Stop Loss Order Filled: id: {}, side: {}, filled_qty: {}, fill_price: {}".format(order_id, side, filled_qty, fill_price))
                            prev_id = stoploss_orders[order_id]
                            tp_id = take_profit_orders[prev_id]
                            del stoploss_orders[order_id]
                            del stoploss_orders[prev_id]

                            await cancel_take_profit_order(client, symbol, logger, err, orderbook, params, tp_id)
                        else:
                            await logger.err("Unknow Order Filled: id: {}, side: {}, filled_qty: {}, fill_price: {}".format(order_id, side, filled_qty, fill_price))
                            print("Unknow Order Filled: id: {}, side: {}, filled_qty: {}, fill_price: {}".format(
                                order_id, side, filled_qty, fill_price))
                elif res['e'] == 'ACCOUNT_UPDATE':
                    position = res['a']['P']
                    for pos in position:
                        if pos['s'] == symbol and pos['ps'] == "BOTH":
                            posAmt = pos['pa']
                            pnl = pos['up']
                            await params.update_param("posAmt", posAmt)
                            await params.update_param("pnl", pnl)
        logger.info("order_filled_socket: socket closed")


async def market_data_socket(client, symbol, logger, err, orderbook, params):
    bsm = BinanceSocketManager(client)
    orderboom_stream = symbol.lower()+'@bookTicker'

    keepalive = 60*50
    timer = time.time()

    price_timer = time.time()

    async with bsm._get_socket(orderboom_stream)as stream:
        while True:
            if time.time() - timer > keep_alive:
                keep_alive = time.time()
                listen_key = await stream.get_listen_key()
                res = await client.keep_alive(listen_key)
                logger.info(
                    "Keep alive: {}, Listen Key: {}".format(res, listen_key))
            if err.status:
                await stream.close()
                await bsm.stop()
                await logger.err("market_data_socket: socket closed")
                break
            try:
                res = await stream.recv()
            except Exception as e:
                await logger.err("market_data_socket: {}".format(e))
                continue
            else:
                if 'stream' in res and res['stream'] == orderboom_stream and 'data' in res:
                    data = res['data']
                    bid, ask = float(data['b']), float(data['a'])

                    temp_orderbook = {'bid': bid, 'ask': ask}
                    if price_timer+1 <= time.time():
                        # further indicator calculation
                        price_timer = time.time()
                    orderbook.updates(temp_orderbook)
                elif res['e'] == 'error':
                    await logger.err("market_data_socket: {}".format(res))
                    err.status = True
        print("market_data_socket: socket closed")
