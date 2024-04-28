import csv
import aiohttp
import asyncio
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta, timezone

async def fetch_data(session, start_time, end_time):
    endpoint = "https://api.binance.com/api/v3/klines"
    interval = "1h"
    symbol = "BTCUSDT"

    start_time_str = str(int(start_time.timestamp()) * 1000) #need times 1000 for unix timestamp 
    end_time_str = str(int(end_time.timestamp()) * 1000) #need times 1000 for unix timestamp 

    payload = {
        "interval": interval,
        "symbol": symbol,
        "endTime": end_time_str,
        "startTime": start_time_str
    }

    async with session.get(endpoint, params=payload) as response:
        data = await response.json()
        return data

async def main():
    #declare the endtime to be today, start time to be 3 months ago
    total_end_time = datetime.now().replace(minute = 0, second = 0)
    total_start_time = datetime.now() - relativedelta(months=3)

    #we add all the tasks as a list under the variable tasks, then we run them concurrently 
    tasks = []
    async with aiohttp.ClientSession() as session: 
        while total_end_time > total_start_time:
            start_time = total_start_time
            end_time = min(total_end_time, total_start_time + timedelta(weeks =1))
            tasks.append(fetch_data(session, start_time, end_time))
            total_start_time += timedelta(weeks =1)
        results = await asyncio.gather(*tasks)
    
    #unpack the values 
    data = []
    for query in results: 
        for record in query: 
            data += [record]

    # Convert timestamp from unix timestamp to human readable format 
    for row in data:
        row[0] = datetime.utcfromtimestamp(row[0] / 1000).strftime('%Y-%m-%d %H:%M:%S')
        row[6] = datetime.utcfromtimestamp(row[6] / 1000).strftime('%Y-%m-%d %H:%M:%S')

    output_file = "btcusdt_historical_data.csv"

    #'w' overwrites with new file 
    with open(output_file, mode='w', newline='') as file:
        writer = csv.writer(file)
        #the headers of the response that we get from https://binance-docs.github.io/apidocs/spot/en/#compressed-aggregate-trades-list
        writer.writerow(["Kline Open time", "Open", "High", "Low", "Close", "Volume", "Kline Close time", "Quote asset volume", "Number of trades", "Taker buy base asset volume", "Taker buy quote asset volume", "Unused field, ignore."])
        for row in data:
            writer.writerow(row)

    print("Data has been saved to ", output_file)

if __name__ == "__main__":
    asyncio.run(main())
