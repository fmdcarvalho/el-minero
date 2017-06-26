from bittrex import Bittrex
from pymongo import MongoClient
from itertools import cycle

import json
import time
import os

class Gather(object):
    def __init__(self):
	print os.environ['KEY']
	print os.environ['SECRET']
        self.bittrex = Bittrex(os.environ['KEY'], os.environ['SECRET'])
        self.client = MongoClient('mongodb://user:12341234@mongo:27017')

    def gather(self, market):
        return self.bittrex.get_market_history(market, 50)['result']

    def insert_documents(self, documents, collection):
	sales = []
	buys  = []
        for mov in documents:
            if mov['OrderType'] == "SELL":
                #self.client[collection].sales.find({'id': mov['Id']}, mov, True)
		sales.append(mov)
            else:
                #self.client[collection].buys.update({'id': mov['Id']}, mov, True)
		buys.append(mov)
	self.client[collection].sales.insert_many(sales)
	self.client[collection].buys.insert_many(buys)

    def main(self):
        market_strs = [
            'BTC-LTC',
            'BTC-ETH',
            'BTC-ARK',
            'BTC-DGB',
            'BTC-SC',
            'BTC-XRP',
            'BTC-DOGE',
        ]
        markets = cycle(market_strs)
        for market in markets:
            try:
                start = time.time()
                print market
                documents = self.gather(market)
                self.insert_documents(documents, market)
                delay = time.time() - start
                print delay
                time.sleep(max(0, 30/len(market_strs)-delay))
                delay = time.time() - start
                print delay

            except Exception as e:
                print e


if __name__ == '__main__':
    gather = Gather()
    gather.main()

