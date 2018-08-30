import os
import pymongo
import pandas as pd
import re

from scrapy.WenCai import WenCai


class Base:
    def __init__(self):
        self.client     = pymongo.MongoClient('localhost', 27017)
        self.db         = self.client['stock']
        pass
    def get_stock_base(self,code,startDate,endDate):
        stocks = pd.DataFrame(columns=('代码', '名称', '日期'))
        # 数据库信息
        self.collection = self.db['base']
        query = {
            'date':{
                '$gte': startDate,
                '$lte': endDate
            },
            'code': code
        }
        res = list(self.collection.find(query))
        df = pd.DataFrame(res)
        del df['_id']
        df = df.sort_values(by=['date'], ascending=False)
        # df = df.set_index(['date'])

        stocks = df.rename(columns={
            'bkcode': '板块代码',
            'bkname': '板块名称',
            'bkzf': '板块涨幅',
            'code': '股票代码',
            'name': '股票名称',
            'date': '日期',
            'increase': '涨幅',
            'title': '异动原因',
            'ltgs': '流通股数',
            'ltsz': '流通市值',
            'ts': '上市天数',
            'hight' : '最高价',
            'zf': '振幅',
            'main': '主力',
            'volume': '成交量',
            'amount': '成交额',
            'close': '收盘价',
        })
        # print(df)
        # industry = self.get_industry_list()
        # for code in df.index:
        #     item = {
        #         '代码': code,
        #         '名称': df.name[code],
        #         '日期': df.date[code],
        #         '流通市值': df.ltsz[code],
        #         '流通股数': df.ltgs[code],
        #         # '隐单': j['yin'],
        #         # '主单': j['zhu'],
        #         # '原因': zts.reason[code],
        #         '行业': industry.industry[code],
        #         '概念': industry.concept[code]
        #     }
        #     stocks = stocks.append(item, ignore_index=True)
        # file_name = '../files/base/base' + '|' + date + '.xlsx'
        # writer = pd.ExcelWriter(file_name)
        # stocks.to_excel(writer, 'Sheet1')
        # writer.save()
        return stocks
    def get_stock_base_list(self,date):
        stocks = pd.DataFrame(columns=('代码', '名称', '日期'))
        file_name = '../files/base/base'+ '|' + date + '.xlsx'
        if not os.path.exists(file_name):
            # 数据库信息
            self.collection = self.db['base']
            query = {
                'date':date
            }
            res = list(self.collection.find(query))
            if len(res) == 0:
                wencai = WenCai(end=date)
                wencai.scrapy_base_tasks()
                res = list(self.collection.find(query))
            df = pd.DataFrame(res)
            del df['_id']
            df = df.set_index(['code'])
            industry = self.get_industry_list()
            for code in df.index:
                item = {
                    '代码': code,
                    '名称': df.name[code],
                    '日期': df.date[code],
                    '流通市值': df.ltsz[code],
                    '流通股数': df.ltgs[code],
                    # '隐单': j['yin'],
                    # '主单': j['zhu'],
                    # '原因': zts.reason[code],
                    '行业': industry.industry[code],
                    '概念': industry.concept[code]
                }
                stocks = stocks.append(item, ignore_index=True)
            file_name = '../files/base/base' + '|' + date + '.xlsx'
            writer = pd.ExcelWriter(file_name)
            stocks.to_excel(writer, 'Sheet1')
            writer.save()
        else:
            print("存在")
            stocks = pd.read_excel(file_name,dtype={'代码':str})
            stocks = stocks.set_index(['代码'])
        return stocks
    def get_industry_list(self):
        file_name = '../files/base/industry.xlsx'
        if not os.path.exists(file_name):
            self.collection = self.db['industry']
            res                 = list(self.collection.find({}))
            df                  = pd.DataFrame(res)
            del df['_id']
            df                  = df.set_index(['code'])
            file_name = '../files/base/industry.xlsx'
            writer = pd.ExcelWriter(file_name)
            df.to_excel(writer, 'Sheet1')
            writer.save()
        else:
            print("行业-存在")
            df = pd.read_excel(file_name,dtype={'code':str})
            df = df.set_index(['code'])
        return df
    def get_zt_list(self,startDate,endDate):
        self.collection = self.db['zt']
        query   = {
            'date':{
                '$gte': startDate,
                '$lte': endDate
            }
        }
        res = list(self.collection.find(query))
        df = pd.DataFrame(res)
        # print(df)
        df = df.drop_duplicates("code")
        del df['_id']
        df = df.set_index(['code'])
        return df

    def get_stock_search_list(self,content):
        self.collection = self.db['industry']

        if content.isdigit():
            query   = {
                'code':{'$regex':content}
            }
        elif re.search('^[a-zA-Z]+',content) != None:
            print('alpha',content)
            query   = {
                'letter':{'$regex':content.upper()}
            }
        else:
            print(content)
            query = {
                'name': {'$regex': content}
            }
            pass

        res         = list(self.collection.find(query,{'code':1,'_id':0,'name':1,'letter':1}).limit(10))
        return res
    pass

base = Base()

# d = re.search('^[a-zA-Z]+','3zxtx')
# print(d)
# base.get_stock_base(code='300706',startDate='2018-01-01',endDate='2018-06-04')
# base.get_stock_search_list('3002222')
# print(res)
# stock_base_list = base.get_stock_base_list("2018-03-09")

# print(stock_base_list)


# stock_industry_list = base.get_industry_list()

# print(stock_industry_list.industry["3004
#
# 31"])