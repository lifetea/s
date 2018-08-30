
import random

import datetime
import time
# import backports.ssl.monkey as monkey
import requests
import ssl
import numpy as np
import os
import tushare as ts
from bs4 import BeautifulSoup
import requests
from pandas.compat import long
import json
import pymongo
import pandas as pd
import re
from multiprocessing.dummy import Pool as ThreadPool
from scrapy.conf import Conf
from scrapy.Base import base
from api.Observe import observe
class LH:
    # 爬虫模式初始化
    def __init__(self):
        self.headers    = Conf.get_headers()
        # 数据库信息
        self.client     = pymongo.MongoClient('localhost', 27017)
        self.db         = self.client['stock']
        self.userAgent = '4gbu9M4ZnViImWxfqF+jEpBfx6sXYX2h'
        self.index      = 0
        self.total      = 0
        self.stocks     = pd.DataFrame(columns=('代码', '名称','日期','隐单','主单','涨幅'))

        pass

    def scrapy_zt_hs_tasks(self,startDate,endDate):
        # df = pd.merge(df, base_list,on='日期',how='right')
        stock_list = base.get_zt_list(startDate=startDate,endDate=endDate)
        observe_list = observe.query()
        ob_code_list = observe_list['股票代码'].values
        zt_code_list = stock_list.index.values
        code_list = np.unique(np.append(ob_code_list,zt_code_list))
        code_list = np.append(code_list,'100000')
        # print(stock_list)

        self.total      = len(code_list)
        self.endDate    = endDate
        pool            = ThreadPool(10)#创建10个容量的线程池并发执行
        pool.map(self.stock_lh_list_scrapy, code_list)
        pool.close()
        pool.join()

        pass

    def stock_lh_list_scrapy(self,date):
        self.collection = self.db['龙虎列表']
        t = time.time()
        ts = int(t)
        try:
            # 同花顺
            # url         = 'https://data.hexin.cn/interface/lhStocks/cate/all/field/jmre/sort/desc/ajax/1/date/2018-06-01/'
            url = 'https://lhb.kaipanla.com/w1/api/index.php'
            headers = {
                'User-Agent': '%E5%BC%80%E7%9B%98%E5%95%A6/3 CFNetwork/811.5.4 Darwin/16.7.0'
            }
            data = {
                'a': 'GetStockList',
                'Token': '1e62254f4e4dbf98d34201bf22d6db02',
                'st': 300,
                'c': 'LongHuBang',
                'UserID': '338646',
                'DeviceID': 'e1e3299eaa666a13d02f104a6a8c6c4ecfb9b0bf',
                'Type': 1,
                'Index' :0,
                'apiv': 'w11',
                'PhoneOsNew': 2,
                'Time': date
            }
            # cookies = {'_ga': 'GA1.2.135vFoCddOrM8Ah8U2CQsoYbkFT51Q6eOGV233702.1506825732'}
            # cookies = {
            #     '_ga': 'GA1.2.135233702.1506825732'
            # }
            # r = requests.get(url,headers=headers,cookies=cookies,verify=True)
            r = requests.post(url,headers=headers, data=data,verify=True)
            #

            res         = json.loads(r.text)

            stocks = res['list']
            group = res['GroupIcon']
            city = res['LikeCity']
            t = res['T']
            dz = res['DZJY']

            # print(stocks)
            print(group)
            items = []
            for s in stocks:
                item={
                    'code': s['ID'],
                    'name': s['Name'],
                    '涨幅': s['IncreaseAmount'],
                    'num': s['JoinNum'],
                    'buy': s['BuyIn'],
                    'score': s['Score'],
                    'd3': s['D3'],
                    '风口': s['FengKou'],
                    'date': date
                }
                if item['code'] in group.keys():
                    item['group'] = group[item['code']]
                else:
                    item['group'] = ''
                if item['code'] in city:
                    item['city'] = 1
                else:
                    item['city'] = 0
                if item['code'] in t:
                    item['T'] = 1
                else:
                    item['T'] = 0
                if item['code'] in dz:
                    item['大宗'] = 1
                else:
                    item['大宗'] = 0
                items.append(item)

            self.collection.insert_many(items, ordered=True)
        except Exception as e:
            print("出错"+date)
            print(e)
        else:
            print("爬取成功"+date)
        pass

    def stock_lh_detail_scrapy(self,code):
        if code[0] == '0':
            type = 'SZ'
        if code[0] == '3':
            type = 'SZ'
        if code[0] == '6':
            type = 'SS'
        if code[0] == '1':
            type = 'SS'
            code = '000001'
        self.collection = self.db['资金流']
        t = time.time()
        ts = int(t)
        try:
            url         = 'https://serverplus.huanshoulv.com/aimapp/stock/fundflowStock/'+code+'.'+type+'?device_id=831af806a8eaf457d220b3291789844979a9acef&device_token=895372e91b6e18b6e74e8cc4a9bbdabbacb016e80c4176ba3280408774660fae&div=IOSH010905&fundflow_min_time=&hsl_id=592214dde7332a04d44c9ccc&min_time=0930&org=1&ts='+str(ts)
            headers = {
                'User-Agent': self.userAgent,
                'userId':'592214dde7332a04d44c9ccc',
                'mobiledevice':'1',
                'deviceId': '831af806a8eaf457d220b3291789844979a9acef'
            }

            # cookies = {'_ga': 'GA1.2.135vFoCddOrM8Ah8U2CQsoYbkFT51Q6eOGV233702.1506825732'}
            cookies = {
                '_ga': 'GA1.2.135233702.1506825732'
            }
            r = requests.get(url,headers=headers,cookies=cookies,verify=True)
            #


            res         = json.loads(r.text)
            data        = res['data']
            # # 另外一种随机设置请求头部信息的方法

            if code == '000001' and type == 'SS':
                data['code'] = '100000'
            else:
                data['code'] = code
            data['date'] = self.endDate

            time.sleep(3)

            url = 'https://serverplus.huanshoulv.com/aimapp/stock/basicCa/'+code+'.'+type+'?device_id=831af806a8eaf457d220b3291789844979a9acef&device_token=895372e91b6e18b6e74e8cc4a9bbdabbacb016e80c4176ba3280408774660fae&div=IOSH010905&fundflow_min_time=&hq_type_code=ESA.SMSE&hsl_id=592214dde7332a04d44c9ccc&min_time=&new_tick=true&tick=1&ts='+str(ts)+'&with_lastday=1'
            r = requests.get(url, headers=headers, cookies=cookies, verify=True)
            res = json.loads(r.text)
            data['lastTrend'] = res['data']['lastTrend']

            time.sleep(3)

            url = 'https://serverplus.huanshoulv.com/aimapp/stock/fundflowLineJG/'+code+'.'+type+'?device_id=831af806a8eaf457d220b3291789844979a9acef&device_token=895372e91b6e18b6e74e8cc4a9bbdabbacb016e80c4176ba3280408774660fae&div=IOSH010905&hsl_id=592214dde7332a04d44c9ccc&min_time=&ts='+str(ts)+'&with_ca=1'

            r = requests.get(url,headers=headers,cookies=cookies,verify=True)
            #
            res         = json.loads(r.text)
            data['detail']        = res['data']['list']
            self.collection.insert(data)
        except Exception as e:
            print("出错"+code)
            print(e)
        else:
            print("爬取成功"+code)
        pass

    def query_flow_detail(self,date,code):
        self.collection = self.db['资金流']
        query = {

            'date': date,
            # 'pCode':i,
            'code': code
        }

        pass




lh = LH()
# lh.stock_lh_list_scrapy('2018-06-21')