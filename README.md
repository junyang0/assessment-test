For question 1 and 2, concurrency is heavily used to speed up the process of downloading and receiving data. This is because API interactions are heavily IO-bound process, so a lot of time is just spent waiting for the server to respond. 

For Question 1, concurrency is used to break down the request of 3 months to smaller weekly requests. This allows us to run the requests concurrently and reduce the time waiting from the server. 

For Question 2, as said in the  interview earlier, we used a 2 pointer approach to calculate the SMA. Although this approach might not be that useful here since we are only calculating a period of 5 datapoints every 5min, the 2 pointer approach, in the event reduce the period to a very small timeframe (e.g. in milliseconds), would reduce our time significantly, since the 2 pointer approach takes O(1) time. We could release the space for unused data, but to do this we may have to change the data structure to a linked list. 

For the 2 streams, we do them concurrently using asyncio. This allows us to switch between the 2 streams and allows us to reduce the time from just waiting. 

Loggers are also implemented so that in production, we can look back at how the software is performing, in case we need any details or in the occurrence of any errors. 