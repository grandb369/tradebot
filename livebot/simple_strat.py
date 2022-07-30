from cmath import log
from binance.client import Client
from binance import BinanceSocketManager
from sympy import re

from data_structure.orderbook import Orderbook
from data_structure.client_params import ClientParams

import asyncio
import time
import datetime

SELL = 'sell'
BUY = 'BUY'
LIMIT = 'LIMIT'
MARKET = 'MARKET'
BID = 'bid'
ASK = 'ask'


async def create_client(api_key, api_secret, url):
    client = await Client(api_key, api_secret)
    client.API_URL = url
    return client


async def execute_order(
    client,
    symbol,
    side,
    order_type,
    amount,
    price,
    precision,
    params,
    logger,
    is_stop_loss_order=False,
    is_take_profit_order=False,
    pre_order_id=-1,
    timeInForce='GTC'
):
    price = round(price, precision)
    if order_type == LIMIT:
        res = await client.create_order(
            symbol=symbol,
            side=side,
            type=order_type,
            timeInForce=timeInForce,
            quantity=amount,
            price=price
        )
        order_id = res['orderId']

        regular_orders = await params.get_param("regular_orders")
        take_profit_orders = await params.get_param("take_profit_orders")
        stoploss_orders = await params.get_param("stoploss_orders")
        if is_stop_loss_order:
            stoploss_orders[order_id] = pre_order_id
            stoploss_orders[pre_order_id] = order_id
            await params.update_param("stoploss_orders", stoploss_orders)
            await logger.info("Stoploss order - price: {}, amount: {}, id: {}, pre_order_id: {}".format(price, amount, order_id, pre_order_id))
        elif is_take_profit_order:
            take_profit_orders[order_id] = pre_order_id
            take_profit_orders[pre_order_id] = order_id
            await params.update_param("take_profit_orders", take_profit_orders)
            await logger.info("Take profit order - price: {}, amount: {}, id: {}, pre_order_id: {}".format(price, amount, order_id, pre_order_id))
        else:
            regular_orders.add(order_id)
            await params.update_param("regular_orders", regular_orders)
            await logger.info("Regular order - price: {}, amount: {}, id: {}".format(price, amount, order_id))


async def send_take_profit_order(client, symbol, logger, err, orderbook, params, order_id, side, filled_qty, fill_price):
    tp_price = fill_price
    precision = await orderbook.get('precision')
    if side.upper() == BUY:
        tp_price *= 1.1
        await execute_order(
            client=client,
            symbol=symbol,
            side=SELL,
            order_type=LIMIT,
            amount=filled_qty,
            price=tp_price,
            precision=precision,
            params=params,
            is_take_profit_order=True,
            pre_order_id=order_id
        )
    else:
        tp_price *= 0.9
        await execute_order(
            client=client,
            symbol=symbol,
            side=BUY,
            order_type=LIMIT,
            amount=filled_qty,
            price=tp_price,
            precision=precision,
            params=params,
            is_take_profit_order=True,
            pre_order_id=order_id
        )


async def send_stop_loss_order(client, symbol, logger, err, orderbook, params, order_id, side, filled_qty, fill_price):
    sl_price = fill_price
    precision = await orderbook.get('precision')
    if side.upper() == BUY:
        sl_price *= 0.9
        await execute_order(
            client=client,
            symbol=symbol,
            side=SELL,
            order_type=LIMIT,
            amount=filled_qty,
            price=sl_price,
            precision=precision,
            params=params,
            is_stop_loss_order=True,
            pre_order_id=order_id
        )
    else:
        sl_price *= 1.1
        await execute_order(
            client=client,
            symbol=symbol,
            side=BUY,
            order_type=LIMIT,
            amount=filled_qty,
            price=sl_price,
            precision=precision,
            params=params,
            is_stop_loss_order=True,
            pre_order_id=order_id
        )


async def cancel_order(client, symbol, logger, params, order_id):
    try:
        await client.cancel_order(symbol=symbol, orderId=order_id)
    except Exception as e:
        await logger.error("Error cancelling order: {}".format(e))
    else:
        regular_orders = await params.get_param("regular_orders")
        regular_orders.remove(order_id)
        await params.update_param("regular_orders", regular_orders)
        await logger.info("Cancelled order: {}".format(order_id))


async def cancel_stoploss_order(client, symbol, logger, params, order_id):
    try:
        await client.cancel_order(symbol=symbol, orderId=order_id)
    except Exception as e:
        await logger.error("Error cancelling order: {}".format(e))
    else:
        stoploss_orders = await params.get_param("stoploss_orders")
        prev_id = stoploss_orders[order_id]
        del stoploss_orders[order_id]
        del stoploss_orders[prev_id]
        await params.update_param("stoploss_orders", stoploss_orders)
        await logger.info("Cancelled order: {}".format(order_id))


async def cancel_take_profit_order(client, symbol, logger, params, order_id):
    try:
        await client.cancel_order(symbol=symbol, orderId=order_id)
    except Exception as e:
        await logger.error("Error cancelling order: {}".format(e))
    else:
        take_profit_orders = await params.get_param("take_profit_orders")
        prev_id = take_profit_orders[order_id]
        del take_profit_orders[order_id]
        del take_profit_orders[prev_id]
        await params.update_param("take_profit_orders", take_profit_orders)
        await logger.info("Cancelled order: {}".format(order_id))


async def cancel_all_unfilled_orders(client, symbol, logger, params):
    regular_orders = await params.get_param("regular_orders")
    for order_id in list(regular_orders):
        await cancel_order(client, symbol, logger, params, order_id)


async def close_open_orders(client, symbol, logger, params):
    orders = await client.futures_get_open_orders(symbol=symbol)
    for order in orders:
        if order["status"] == "NEW" or order["status"] == "PARTIALLY_FILLED":
            try:
                await client.cancel_order(symbol=symbol, orderId=order["orderId"])
            except Exception as e:
                await logger.error("Error cancelling order: {}".format(e))
    await params.update_param("regular_orders", set())
    await params.update_param("stoploss_orders", {})
    await params.update_param("take_profit_orders", {})


async def close_position(client, symbol, logger, params):
    position = await client.get_position(symbol=symbol)
    if position:
        await client.close_position(symbol=symbol)
        await logger.info("Closed position: {}".format(position["id"]))


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
                        side = order['S'].lower()
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

                            await cancel_stoploss_order(client, symbol, logger, params, st_id)
                        elif order in stoploss_orders:
                            await logger.info("Stop Loss Order Filled: id: {}, side: {}, filled_qty: {}, fill_price: {}".format(order_id, side, filled_qty, fill_price))
                            prev_id = stoploss_orders[order_id]
                            tp_id = take_profit_orders[prev_id]
                            del stoploss_orders[order_id]
                            del stoploss_orders[prev_id]

                            await cancel_take_profit_order(client, symbol, logger, params, tp_id)
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


async def regular_order_stream(client, symbol, logger, err, orderbook, params):
    await logger.info("regular_order_stream: started")
    order_interval = await params.get_param("order_interval")
    order_interval = int(order_interval)
    max_order = await params.get_param("max_order")
    enable_close_position = await params.get_param("close_position")
    enable_close_open_orders = await params.get_param("close_open_orders")

    await logger.info("Regular Order Interval: {}".format(order_interval))
    await asyncio.sleep(5)
    timer = time.time()
    while True:
        if err.status:
            await logger.err("regular_order_stream: terminated")
            if enable_close_open_orders:
                await close_open_orders(client, symbol, logger, params)
            if enable_close_position:
                await close_position(client, symbol, logger, params)
            break
        if time.time() - timer > order_interval or len(await params.get_param("regular_orders")) < max_order:
            await cancel_all_unfilled_orders(client, symbol, logger, params)
            best_bid = await orderbook.get('bid')
            best_ask = await orderbook.get('ask')
            amount = await orderbook.get("amount")
            precision = await orderbook.get('precision')

            # calculate bid/ask depending on the other indicators
            # end

            await logger.info("Regular Order: best_bid: {}, best_ask: {}".format(best_bid, best_ask))
            # BID
            await execute_order(
                client=client,
                symbol=symbol,
                side=BUY,
                order_type=LIMIT,
                amount=amount,
                price=best_bid,
                precision=precision,
                logger=logger
            )
            # ASK
            await execute_order(
                client=client,
                symbol=symbol,
                side=SELL,
                order_type=LIMIT,
                amount=amount,
                price=best_ask,
                precision=precision,
                logger=logger
            )
            timer = time.time()
        await asyncio.sleep(1)
    await logger.info("regular_order_stream: terminated")
