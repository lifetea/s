import pandas as pd

import pymongo
# 股票追踪
class Observe:
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

    def query(self,startDate=None,endDate=None):
        stock_list = pd.DataFrame(columns=('股票代码', '股票名称', '添加日期','类型','状态','备注'))
        collection = self.db['追踪']
        query = {

        }
        sort = [('date', 1)]
        res = list(collection.find(query, sort=sort))

        stock_list = pd.DataFrame(res)
        del stock_list['_id']
        stock_list = stock_list.rename(columns={
            'code': '股票代码',
            'name': '股票名称',
            'date': '添加日期',
            'type': '类型',
            'status': '状态',
            'remark': '备注'
        })
        return stock_list
        pass

observe = Observe()

