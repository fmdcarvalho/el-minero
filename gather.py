from bittrex import Bittrex
from pymongo import MongoClient
from itertools import cycle

import json
import time


class Gather(object):
    def __init__(self):
        with open("secrets.json") as secrets_file:
            self.secrets = json.load(secrets_file)
            secrets_file.close()
        self.bittrex = Bittrex(self.secrets['key'], self.secrets['secret'])
        self.client = MongoClient('192.168.2.8', 27017)

    def gather(self, market):
        return self.bittrex.get_market_history(market, 50)['result']

    def insert_documents(self, documents, collection):
        for mov in documents:
            if mov['OrderType'] == "SELL":
                self.client[collection].sales.update({'id': mov['Id']}, mov, True)
            else:
                self.client[collection].buys.update({'id': mov['Id']}, mov, True)

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

            except Exception:
                pass


if __name__ == '__main__':
    gather = Gather()
    gather.main()

