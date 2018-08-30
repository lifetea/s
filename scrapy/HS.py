
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
class HS:
    # 爬虫模式初始化
    def __init__(self):
        self.headers    = Conf.get_headers()
        # 数据库信息
        self.client     = pymongo.MongoClient('localhost', 27017)
        self.db         = self.client['stock']
        self.userAgent = '9RM05HdXrkcLBbYxnCRc8ve71yz3IDkc'
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
        pool.map(self.stock_hs_scrapy, code_list)
        pool.close()
        pool.join()

        pass

    def stock_hs_scrapy(self,code):
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

    # def query_yin_zt_list(self,startDate,endDate):
    #     file_name_zt = '../files/yin/' + startDate + '|' + endDate + '-zt.xlsx'
    #     if not os.path.exists(file_name_zt):
    #         self.stocks = pd.DataFrame(columns=('代码', '名称', '日期', '隐单', '主单', '涨幅'))
    #         base = Base()
    #         industry = base.get_industry_list()
    #         base_list = base.get_stock_base_list(endDate)
    #         query = {
    #             'yin': {
    #                 '$gte': 100,
    #                 # '$lte': time_end
    #             },
    #             # 'zhu': {
    #             #     # '$gte': 0,
    #             #     '$lte': -100
    #             # },
    #             'zf': {
    #                 '$gte': -6.0,
    #                 '$lte': 6.0
    #             },
    #             'date': endDate,
    #             # 'pCode':i,
    #             # 'code': code
    #         }
    #         res = self.collection.find(query)
    #         res_list = list(res)
    #         self.total = len(res_list)
    #         for j in res_list:
    #             code = j['code']
    #             date = j['date']
    #             yin = float(j['yin'])
    #             zhu = float(j['zhu'])
    #             zj = float(j['zj'])
    #             zt_list = base.get_zt11_list(startDate).index
    #             try:
    #                 item = {
    #                     '代码': code,
    #                     '名称': j['name'],
    #                     '日期': endDate,
    #                     '隐单': j['yin'],
    #                     '主单': j['zhu'],
    #                     '涨幅': j['zf'],
    #                     '流通股': long(base_list['流通股数'][j['code']]),
    #                     '力度': round((long(j['yin']) * 10000) / (long(base_list['流通股数'][j['code']])), 2),
    #                     '行业': industry.industry[j['code']],
    #                     '概念': industry.concept[j['code']]
    #                 }
    #             except Exception as e:
    #                 print("出错"+code)
    #                 print(e)
    #                 continue
    #
    #             # for i, zf in enumerate(zfs):
    #             #     # print(i)
    #             #     item['第' + str(i + 2) + '天涨幅'] = zf
    #             # if zhu != 0 and yin / abs(zhu) < 0.3:
    #             #     continue
    #             # if zhu > 0 and zhu > yin:
    #             #     continue
    #             # if abs(zj) <= 100:
    #             #     continue
    #
    #             if code in zt_list:
    #                 self.stocks = self.stocks.append(item, ignore_index=True)
    #             # else:
    #             #     global stocks
    #             #     stocks = stocks.append(item, ignore_index=True)
    #             # zfs = []
    #             self.index += 1
    #             print(str(self.index)+"/"+str(self.total))
    #             pass
    #         file_name_zt = '../files/yin/' + startDate + '|' + endDate + '-zt.xlsx'
    #         writer_zt = pd.ExcelWriter(file_name_zt)
    #         self.stocks.to_excel(writer_zt, 'Sheet1')
    #         writer_zt.save()
    #     else:
    #         print("存在")
    #         self.stocks = pd.read_excel(file_name_zt,dtype={'代码':str})
    #         # print(self.stocks)
    #
    #     return self.stocks
    #     pass
    #
    # def query_yin_list(self,endDate):
    #     file_name = '../files/yin/' + endDate + '.xlsx'
    #     if not os.path.exists(file_name):
    #         self.stocks_zt = pd.DataFrame(columns=('代码', '名称', '日期', '隐单', '主单', '涨幅'))
    #         base = Base()
    #         base_list = base.get_stock_base_list(endDate)
    #         query = {
    #             'yin': {
    #                 '$gte': 100,
    #                 # '$lte': time_end
    #             },
    #             # 'zhu': {
    #             #     # '$gte': 0,
    #             #     '$lte': -100
    #             # },
    #             'zf': {
    #                 '$gte': -6.0,
    #                 '$lte': 6.0
    #             },
    #             'date': endDate,
    #             # 'pCode':i,
    #             # 'code': code
    #         }
    #         res = self.collection.find(query)
    #         res_list = list(res)
    #         self.total = len(res_list)
    #         for j in res_list:
    #             code = j['code']
    #             date = j['date']
    #             # zfs = get_stock_later_info(code, date)
    #             yin = float(j['yin'])
    #             zhu = float(j['zhu'])
    #             zj  = float(j['zj'])
    #             item = {
    #                 '代码': code,
    #                 '名称': j['name'],
    #                 '日期': endDate,
    #                 '隐单': j['yin'],
    #                 '主单': j['zhu'],
    #                 '涨幅': j['zf'],
    #                 '流通股': long(base_list['流通股数'][j['code']]),
    #                 '力度': round((long(j['yin']) * 10000) / (long(base_list['流通股数'][j['code']])), 2),
    #                 '行业': base_list.industry[j['code']],
    #                 '概念': base_list.concept[j['code']]
    #             }
    #             # for i, zf in enumerate(zfs):
    #             #     # print(i)
    #             #     item['第' + str(i + 2) + '天涨幅'] = zf
    #             if zhu != 0 and yin / abs(zhu) < 0.3:
    #                 continue
    #             if zhu > 0 and zhu > yin:
    #                 continue
    #             if abs(zj) <= 100:
    #                 continue
    #
    #             self.stocks = self.stocks.append(item, ignore_index=True)
    #             self.index += 1
    #             print(str(self.index)+"/"+str(self.total))
    #             pass
    #         # print(self.stocks.to_json(orient='split'))
    #         file_name = '../files/yin/' + endDate + '.xlsx'
    #         writer = pd.ExcelWriter(file_name)
    #         self.stocks.to_excel(writer, 'Sheet1')
    #         writer.save()
    #     else:
    #         print("存在")
    #         self.stocks = pd.read_excel(file_name,dtype={'代码':str})
    #         # print(self.stocks)
    #     return self.stocks
    #     pass
    #
    # def query_yin_detail(self,code,startDate,endDate):
    #     stock = pd.DataFrame(columns=('代码', '名称', '日期', '隐单', '主单', '涨幅'))
    #     base = Base()
    #     base_list = base.get_stock_base_list(endDate)
    #     query = {
    #         'date': {
    #             '$gte': startDate,
    #             '$lte': endDate
    #         },
    #         'code': code
    #     }
    #     res = self.collection.find(query)
    #     print(list(res))
    #     # res_list = list(res)
    #     # self.total = len(res_list)
    #     # for j in res_list:
    #     #     code = j['code']
    #     #     date = j['date']
    #     #     # zfs = get_stock_later_info(code, date)
    #     #     yin = float(j['yin'])
    #     #     zhu = float(j['zhu'])
    #     #     zj  = float(j['zj'])
    #     #     item = {
    #     #         '代码': code,
    #     #         '名称': j['name'],
    #     #         '日期': endDate,
    #     #         '隐单': j['yin'],
    #     #         '主单': j['zhu'],
    #     #         '涨幅': j['zf'],
    #     #         '流通股': long(base_list['流通股数'][j['code']]),
    #     #         '力度': round((long(j['yin']) * 10000) / (long(base_list['流通股数'][j['code']])), 2),
    #     #         '行业': base_list.industry[j['code']],
    #     #         '概念': base_list.concept[j['code']]
    #     #     }
    #     #     # for i, zf in enumerate(zfs):
    #     #     #     # print(i)
    #     #     #     item['第' + str(i + 2) + '天涨幅'] = zf
    #     #     if zhu != 0 and yin / abs(zhu) < 0.3:
    #     #         continue
    #     #     if zhu > 0 and zhu > yin:
    #     #         continue
    #     #     if abs(zj) <= 100:
    #     #         continue
    #     #
    #     #     self.stocks = self.stocks.append(item, ignore_index=True)
    #     #     self.index += 1
    #     #     print(str(self.index)+"/"+str(self.total))
    #     pass



hs = HS()
# hs.scrapy_zt_hs_tasks(startDate="2018-04-25",endDate="2018-05-25")
# yin.query_yin_detail('002181','2018-05-01','2018-05-18')
# hs.stock_yin_scrapy('600536','2018-05-29')
# def query_yin_zt_list(date):
#     yin = Yin()
#     return  yin.query_yin_zt_list(startDate=startDate,endDate=)