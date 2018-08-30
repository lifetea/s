import pandas as pd

import pymongo
# 股票追踪
class Flow:
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

    def query(self,endDate=None,code=None):
        collection = self.db['资金流']
        query = {
            'date': endDate,
            'code': code
        }
        sort = [('date', 1)]
        stock = collection.find_one(query, sort=sort)
        del stock['_id']
        print(stock)
        print(stock['detail'])
        # stock_list = pd.DataFrame(res)
        # del stock_list['_id']
        # print(stock_list)
        # stock_list = stock_list.rename(columns={
        #     'code': '股票代码',
        #     'name': '股票名称',
        #     'date': '添加日期',
        #     'type': '类型',
        #     'status': '状态',
        #     'remark': '备注'
        # })
        return stock
        pass

flow = Flow()

# flow.query(endDate='2018-06-15',code='002897')