import pandas as pd

import pymongo
# 股票追踪
class LH:
    def __init__(self):
        self.client = pymongo.MongoClient('localhost', 27017)
        self.db = self.client['stock']
        self.collection = self.db['追踪']
        pass
    def save(self,item):
        collection = self.collection
        try:
            res = collection.find_one({'code': item['code']})
            print(res)
            if res != None:
                collection.update({'code': item['code']},item)
            else:
                collection.save(item)
        except Exception as e:
            print(e)
        else:
            print("成功")
        pass

    def query(self,endDate=None):
        stocks = pd.DataFrame(columns=('股票代码', '股票名称', '日期', '涨幅', '净买入', '游资', '关联'))
        collection = self.db['龙虎列表']
        query = {
            'date': endDate
        }
        sort = [('date', 1)]
        res = collection.find(query, sort=sort)

        # del stock['_id']
        stocks = pd.DataFrame(list(res))
        del stocks['_id']
        # print(stock_list)
        stocks = stocks.rename(columns={
            'code': '股票代码',
            'name': '股票名称',
            'date': '日期',
            'buy': '净买入',
            'group': '游资',
            'num': '关联',
            'T': '做T',
            'city': '同城协作',
        })
        # print(stocks)
        return stocks
        pass

lh = LH()

# lh.query(endDate='2018-06-21')