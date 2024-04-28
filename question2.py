import csv
import json
import asyncio
import logging
import aiofiles
import traceback
import websockets
import numpy as np
from asyncio import Queue

# we declares constants
sma_period = 5
uri_klines = "wss://stream.binance.com:9443/ws/btcusdt@kline_5m"
uri_depth = "wss://stream.binance.com:9443/ws/btcusdt@depth"
closing_prices = []
sma_values = []
timestamps = []
mid_prices = []

#get sma with 2 pointers approach using list 
def get_sma():
    global closing_prices, sma_values
    if len(closing_prices) < sma_period:
        return None
    start = max(0, len(closing_prices) - sma_period)
    end = len(closing_prices) - 1
    if len(sma_values) == 0:
        if len(closing_prices) ==5: 
            sma = np.mean(closing_prices); 
            return sma
        else: return 
    else:
        removed_value = closing_prices[start+1]  
        new_value = closing_prices[end]
        sma = sma_values[-1] + (new_value - removed_value) / sma_period
    return sma

async def save_indicator(filename, side, btc_mid_price, sma, unix_timestamp):
    async with aiofiles.open(filename, 'a') as file:
        writer = csv.writer(file)
        await writer.writerow([side, btc_mid_price, sma, unix_timestamp])

#message handler for klines
async def handle_message_klines(message):
    data = json.loads(message)
    async with aiofiles.open('btcusdt_5m_klines.csv', mode='a') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=data.keys())
        await writer.writerow(data)
    if 'k' in data:
        closing_prices.append(float(data['k']['c']))
        sma = get_sma()
        if sma is not None:
            sma_values.append(sma)
            timestamps.append(data['k']['t'])

#depth handler every 1s 
async def handle_message_depth(message):
    data = json.loads(message)
    if 'b' in data and 'a' in data:
        best_bid = float(data['b'][0][0]) #bids are key ,value pairs where [0][0] is highest bid 
        best_ask = float(data['a'][0][0]) #bids are key ,value pairs where [0][0] is lowest  ask 
        mid_price = (best_bid + best_ask) / 2
        mid_prices.append(mid_price)
        async with aiofiles.open('btcusdt_p_order_book_depth_1s.csv', mode='a') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=data.keys())
                await writer.writerow(data)

        if len(mid_prices) >= sma_period and len(sma_values) >= 2:
            if mid_price > sma_values[-1] and mid_prices[-2] <= sma_values[-2]: #bottom up 
                side = "BUY"
                print(f"{side} BTC @ {mid_price} | SMA: {sma_values[-1]} | UNIX TIMESTAMP: {data['E']}")
                await save_indicator('sma_indicator.csv', side, mid_price, sma_values[-1], data['E'])
            elif mid_price < sma_values[-1] and mid_prices[-2] >= sma_values[-2]: #top to bottom 
                side = "SELL"
                print(f"{side} BTC @ {mid_price} | SMA: {sma_values[-1]} | UNIX TIMESTAMP: {data['E']} ")
                await save_indicator('sma_indicator.csv', side, mid_price, sma_values[-1], data['E'])

#just some logger helpers 
async def on_close_klines():
    logger.info("Connection [Kline] has been closed")

async def on_close_depth():
    logger.info("Connection [Depth] has been closed")

async def on_open_klines():
    logger.info("Connection to Klines has been successfully opened")

async def on_open_depth():
    logger.info("Connection to Depth has been successfully opened")

#async connect to sockets 
async def main():
    klines_task = asyncio.create_task(connect_and_receive(uri_klines, handle_message_klines, on_close_klines, on_open_klines))
    depth_task = asyncio.create_task(connect_and_receive(uri_depth, handle_message_depth, on_close_depth, on_open_depth))
    await asyncio.gather(klines_task, depth_task)

async def connect_and_receive(uri, message_handler, close_handler, open_handler):
    async with websockets.connect(uri) as ws:
        await open_handler()
        while True:
            try:
                message = await ws.recv()
                await message_handler(message)
            except websockets.ConnectionClosed:
                await close_handler()
                break
            except Exception: 
                    logger.error(traceback.format_exc())

if __name__ == "__main__":
    #set up logger for logs
    logging.basicConfig(level=logging.INFO, filename='logs.log', filemode='a', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    #run main 
    asyncio.run(main())