
import random

import datetime
import time

import re
import tushare as ts
from bs4 import BeautifulSoup
import requests
import json
import pymongo
import pandas as pd
import numpy as np
from scrapy.conf import Conf
from pypinyin import pinyin, Style
from multiprocessing.dummy import Pool as ThreadPool

class WenCai:

    def __init__(self,start=None,end=None):
        self.headers        = Conf.get_headers()
        self.cookies        = {
            "PHPSESSID": "63aa05bad945b37d7d74843939aceb25",
            "cid": "63aa05bad945b37d7d74843939aceb251520148300",
            "ComputerID": "63aa05bad945b37d7d74843939aceb251520148300",
            "v": "AlpvI_i61M4eU1hiNEcC5yVcrQt4i9zSUANSD2TFBzu4nPS9TBsudSCfoIY3"
        }
        self.token          = "4cfa09359ef1704a89e4f0c1e552f979"
        self.page           = '1'
        self.perpage        = '3600'
        self.total          = 0
        self.index          = 0
        self.start          = start
        self.end            = end
        # 数据库信息
        self.client         = pymongo.MongoClient('localhost', 27017)
        self.db             = self.client['stock']
        self.collection     = self.db['industry']

    def get_industry_token(self):
        header              = random.choice(self.headers)
        headers             = {
            'User-Agent': header,
        }
        url = 'https://www.iwencai.com/stockpick/load-data?typed=0&preParams=&ts=1&f=1&qs=result_original&selfsectsn=&querytype=stock&searchfilter=&tid=stockpick&w=%E6%A6%82%E5%BF%B5&queryarea='
        r                   = requests.get(url=url, headers=headers, cookies=self.cookies)
        print(url)
        res         = json.loads(r.text)
        data        = res['data']
        result      = data['result']
        self.total  = int(result['total'])
        self.token  = result['token']

    def scrapy_industry_tasks(self):
        self.perpage        = 700
        self.collection     = self.db['industry']
        self.get_industry_token()
        count = np.math.ceil(self.total / self.perpage)
        pages = np.array(range(1,count+1))
        # print(pages)
        pool = ThreadPool(4)#创建10个容量的线程池并发执行
        pool.map(self.scrapy_industry, pages)
        pool.close()
        pool.join()
        pass

    def scrapy_industry(self,page):
        header              = random.choice(self.headers)
        url                 = 'https://www.iwencai.com/stockpick/cache?token='+self.token+'&p='+str(page)+'&perpage='+str(self.perpage)+'&showType=[%22%22,%22%22,%22onTable%22,%22onTable%22,%22onTable%22,%22onTable%22,%22onTable%22,%22onTable%22,%22onTable%22,%22onTable%22,%22onTable%22,%22onTable%22,%22onTable%22,%22onTable%22,%22onTable%22]'
        print(url)
        r                   = requests.get(url=url, headers={'User-Agent': header}, cookies=self.cookies)
        res                 = json.loads(r.text)
        stock_list          = res['result']

        items               = []
        # for i in res:
        #     pinyin('中心', style=Style.FIRST_LETTER)
        for s in stock_list:
            letter = pinyin(s[1], style=Style.FIRST_LETTER)
            item = {
                'code': s[0][0:6],
                'name': s[1],
                'concept':s[4],
                'industry':s[-1],
                'letter': ''.join(str(e[0]) for e in letter).upper()
            }
            items.append(item)
        try:
            self.collection.insert_many(items,ordered=True)
        except Exception as e:
            print(e)
        else:
            print("爬取成功")
        pass
    def query_industry_list(self):
        res                 = list(self.collection.find({}))
        df                  = pd.DataFrame(res)
        del df['_id']
        df                  = df.set_index(['code'])
        return df



    def get_base_token(self):
        header              = random.choice(self.headers)
        headers             = {
            'User-Agent': header,
        }
        url = 'https://www.iwencai.com/stockpick/robot-search?w='+self.end+'%E6%B6%A8%E5%B9%85%3B%20'+self.end+'%E6%88%90%E4%BA%A4%E9%87%8F%3B%20'+self.end+'%E6%88%90%E4%BA%A4%E9%A2%9D%3B%20'+self.end+'%E4%B8%BB%E5%8A%9B%E8%B5%84%E9%87%91%E6%B5%81%E5%90%91%3B%20'+self.end+'%E5%B8%82%E7%9B%88%E7%8E%87(pe)%3B%20'+self.end+'%E6%9C%AA%E5%81%9C%E7%89%8C&querytype=stock&robotResultPerpage=70'
        r                   = requests.get(url=url, headers=headers, cookies=self.cookies)
        print(url)
        # print(r.text)
        content     = r.text
        search      = re.search(r'var allResult = (.*)',content)
        result      = search.group(1).strip()[0:-1]
        data        = json.loads(result)
        self.total  = int(data['total'])
        self.token  = data['token']

    def scrapy_base_tasks(self):
        self.perpage        = 700
        self.collection     = self.db['base']
        self.get_base_token()
        count = np.math.ceil(self.total / self.perpage)
        pages = np.array(range(1,count+1))
        # print(pages)
        pool = ThreadPool(4)#创建10个容量的线程池并发执行
        pool.map(self.scrapy_base, pages)
        pool.close()
        pool.join()
        pass
    def scrapy_base(self,page):
        header = random.choice(self.headers)
        url         = 'https://www.iwencai.com/stockpick/cache?token='+self.token+'&p='+str(page)+'&perpage='+str(self.perpage)+'&showType=[%22%22,%22%22,%22onTable%22,%22onTable%22,%22onTable%22,%22onTable%22,%22onTable%22,%22onTable%22,%22onTable%22,%22onTable%22,%22onTable%22,%22onTable%22,%22onTable%22,%22onTable%22,%22onTable%22,%22onTable%22,%22onTable%22,%22onTable%22,%22onTable%22,%22onList%22,%22onTable%22,%22onTable%22,%22onTable%22,%22onTable%22,%22onTable%22,%22onTable%22]'

        try:
            r = requests.get(url=url, headers={'User-Agent': header}, cookies=self.cookies)
            res = json.loads(r.text)
            stock_list = res['result']

            items = []

            for s in stock_list:
                item = {
                    #日期
                    'date': self.end,
                    #代码
                    'code': s[0][0:6],
                    'name': s[1],
                    #涨幅
                    'increase': s[-22],
                    #成交量
                    'volume': s[-21],
                    #成交额
                    'amount': s[-20],
                    #主力净
                    'main': s[-19],
                    #PE
                    'pe': s[-18],
                    '状态': s[-17],
                    #开盘价
                    'open': s[-16],
                    'high': s[-15],
                    'low': s[-14],
                    'close': s[-13],
                    #振幅
                    'zf': s[-12],
                    'main-in': s[-10],
                    'main-out': s[-9],
                    '总市值': s[-6],
                    '毛利率': s[-4],
                    '净利率': s[-3],
                    'ts': s[-2],
                    'ltsz': float(s[-1]),
                    'ltgs': float(s[-1])/float(s[12]),
                }
                items.append(item)
            self.collection.insert_many(items,ordered=True)
        except Exception as e:
            print("出错"+str(page))
            # print(r.text)
            print(e)
        else:
            print("爬取成功"+str(page))
        pass
    def get_zt_token(self):
        header              = random.choice(self.headers)
        headers             = {
            'User-Agent': header,
        }
        url = 'http://www.iwencai.com/stockpick/search?typed=1&preParams=&ts=1&f=3&qs=pc_~soniu~stock~stock~history~query&selfsectsn=&querytype=stock&searchfilter=&tid=stockpick&w='+self.end+'+%E6%94%B6%E7%9B%98%E4%BB%B7%E6%A0%BC%E7%AD%89%E4%BA%8E%E6%B6%A8%E5%81%9C%E4%BB%B7'
        r                   = requests.get(url=url, headers=headers, cookies=self.cookies)
        print(url)
        # print(r.text)
        content     = r.text
        search      = re.search(r'var allResult = (.*)',content)
        result      = search.group(1).strip()[0:-1]
        data        = json.loads(result)
        self.token  = data['token']
    def scrapy_zt(self):
        self.get_zt_token()
        collection = self.db['zt']
        header = random.choice(self.headers)

        url = 'http://www.iwencai.com/stockpick/cache?token='+self.token+'&p=1&perpage=200&changeperpage=1&showType=[%22%22,%22%22,%22onTable%22,%22onTable%22,%22onTable%22,%22onTable%22,%22onTable%22,%22onTable%22,%22onTable%22,%22onTable%22,%22onTable%22,%22onTable%22,%22onTable%22,%22onTable%22,%22onTable%22,%22onTable%22]'
        try:
            r = requests.get(url=url, headers={'User-Agent': header}, cookies=self.cookies)

            demo = r.text

            res = json.loads(demo)
            # print(res)
            stock_list = res['result']
            items = []
            for s in stock_list:
                print(s[0][0:6])
                # print(s)
                item = {
                    'code': s[0][0:6],
                    'date': self.end,
                    'name': s[1],
                    # 'zf': s[3],
                    # 'sczt': s[5],
                    # 'zzzt': s[6],
                    # 'detail': s[7],
                    # 'count': s[8],
                    # 'reason': s[9],
                    # 'fdl': s[10],
                    # 'fde': s[11],
                    # 'fcb': s[12],
                    # 'flb': s[13],
                    # 'kbs': s[14],
                    # 'ssts': s[15],
                    'status':0,
                    'remark':""
                }
                items.append(item)
            collection.insert_many(items, ordered=True)
        except Exception as e:
            print("出错" + self.end)
            # print(r.text)
            print(e)
        else:
            print("爬取成功" + self.end)
        pass

    def scrapy_yd(self,date):
        collection  = self.db['异动']
        header      = random.choice(self.headers)
        url_date        = date.replace('-','')
        url         = 'https://eq.10jqka.com.cn/dpyddata/apidata_'+ url_date +'.txt'
        print(url)
        try:
            r = requests.get(url=url, headers={'User-Agent': header}, cookies=self.cookies)

            demo = r.text

            yd_list = json.loads(demo)
            items = []
            for index,d in enumerate(yd_list):
                tag    =    index
                info   =    d['info'][0]
                title  =    info['title']
                id     =    info['id']
                time   =    info['time']

                stocklist = info['stocklist']
                bkname = "无"
                bkzf   = 0
                bkcode = ""
                if stocklist[0]['stockCode'][0] != '0'  and stocklist[0]['stockCode'][0] != '6' and stocklist[0]['stockCode'][0] != '3':
                    bkname = stocklist[0]['stockName']
                    bkzf   = float(stocklist[0]['dzf'][0:-1])
                    bkcode = stocklist[0]['stockCode']
                for s in stocklist:
                    if s['stockCode'][0] == '0' or s['stockCode'][0] == '6' or s['stockCode'][0] == '3':
                        zf   = float(s['dzf'][0:-1])
                        if zf >=9.9:
                            iszt =  1
                        elif zf <= -9.9:
                            iszt = -1
                        else:
                            iszt = 0
                        item = {
                            'date': date,
                            'code': s['stockCode'],
                            'name': s['stockName'],
                            'zf': zf,
                            'iszt': iszt,
                            'tag'   :tag,
                            'title' :title,
                            'id'    :id,
                            'time'  :time,
                            'bkname':bkname,
                            'bkcode':bkcode,
                            'bkzf'  :bkzf,
                            'marketId':s['marketId']
                        }
                        items.append(item)
            collection.insert_many(items, ordered=True)
        except Exception as e:
            print("出错",e)
        else:
            print("爬取成功",date)
        pass


    def query_yd(self,tag):
        collection = self.db['异动']
        query = {
            'tag': int(tag)
        }
        sort = [('date', -1)]
        res = collection.find(query,sort=sort)
        if res.count() > 0:
            df = pd.DataFrame(list(res))
            del df['_id']
            date_list = np.unique(df['date'])
            for d in date_list:
                data = df[df['date'] == d]
                print(d)
                item = {
                    d : '555',
                    'index':int(tag)
                }
                # item[d] = data
                # print(j)
                pass
                s = pd.Series(item)
                print('s:',s)
                self.stocks = self.stocks.append(s)
        else:
            print("没有记录")
        pass
    def query_yd_list(self,startDate,endDate):
        stocks = pd.DataFrame(columns=('股票代码', '股票名称', '日期','涨幅'))
        collection = self.db['异动']
        query = {
            'date': {
                '$gte': startDate,
                '$lte': endDate
            },
            'tag':{
                '$gte': 1,
            }

        }
        sort = [('date', 1)]
        res = collection.find(query,sort=sort)
        stocks = pd.DataFrame(list(res))
        del stocks['_id']
        stocks = stocks.rename(columns={
            'code': '股票代码',
            'name': '股票名称',
            'date': '日期',
            'zf': '涨幅',
            'bkcode': '板块代码',
            'bkname': '板块名称',
            'bkzf': '板块涨幅',
            'title': '异动原因',
            'time': '时间',
            'iszt': '是否涨停',

        })
        return stocks
        pass


def base_tasks(date):
    wencai =  WenCai(end=date)
    wencai.scrapy_base_tasks()

def zt_tasks():
    wencai =  WenCai()
    wencai.query_yd_list(startDate="2018-03-23",endDate="2018-04-27")
    # wencai.scrapy_yd(date="2018-03-23")

# zt_tasks()
#
wencai = WenCai()


# wencai.scrapy_industry_tasks()
