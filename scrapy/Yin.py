
import random

import datetime
import time

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
class Yin:
    # 爬虫模式初始化
    def __init__(self):
        self.headers    = Conf.get_headers()
        # 数据库信息
        self.client     = pymongo.MongoClient('localhost', 27017)
        self.db         = self.client['stock']
        self.collection = self.db['yin']
        self.index      = 0
        self.total      = 0
        self.stocks     = pd.DataFrame(columns=('代码', '名称','日期','隐单','主单','涨幅'))

        pass

    def scrapy_yin_tasks(self,date):
        stock_list      = base.get_stock_base_list(date)
        code_list       = stock_list.index.values
        self.total      = len(code_list)
        pool            = ThreadPool(10)#创建10个容量的线程池并发执行
        pool.map(self.stock_yin_scrapy, code_list)
        pool.close()
        pool.join()

    def scrapy_yin_zt_tasks(self,startDate,endDate):
        stock_list = base.get_zt_list(startDate=startDate, endDate=endDate)
        code_list       = stock_list.index.values
        self.total      = len(code_list)
        pool            = ThreadPool(10)#创建10个容量的线程池并发执行
        pool.map(self.stock_yin_scrapy, code_list)
        pool.close()
        pool.join()

    def stock_yin_scrapy(self,code):
        if code[0] == '0':
            type = 'SZ'
        if code[0] == '3':
            type = 'SZ'
        if code[0] == '6':
            type = 'SH'

        # 另外一种随机设置请求头部信息的方法
        header      = random.choice(self.headers)
        url         = 'http://www.bestgo.com/topview/'+type+code+'.html'

        print(url)

        try:
            r = requests.get(url=url, headers={'User-Agent': header})
            time.sleep(3)
            r.encoding = r.apparent_encoding
            demo = r.text
            # print(demo)
            print('开始爬取' + code)
            #
            soup = BeautifulSoup(demo, 'html.parser')  # 解析器：html.parser

            items = []
            tbodys = soup.select(".tabPanel .panes tbody")
            trs_1 = tbodys[0].select("tr")
            trs_2 = tbodys[1].select("tr")
            trs_3 = tbodys[2].select("tr")
            name = soup.select("#stock_name a")[0].get_text(strip=True)
            for i, d in enumerate(trs_1):
                tds = d.select("td")
                t = tds[0].get_text(strip=True).split("-")

                date = datetime.datetime(2018, int(t[0]), int(t[1])).strftime("%Y-%m-%d")
                zf = float(tds[2].get_text(strip=True))
                yin = float(tds[3].get_text(strip=True))

                item = {
                    'date': date,
                    'yin': yin,
                    'zf': zf,
                    'code': code,
                    'name': name
                }
                items.append(item)

            for i, d in enumerate(trs_2):
                tds = d.select("td")
                zhu = float(tds[3].get_text(strip=True))
                items[i]["zhu"] = zhu

            for i, d in enumerate(trs_3):
                tds = d.select("td")
                zj = float(tds[3].get_text(strip=True))
                items[i]["zj"] = zj
            self.collection.insert_many(items,ordered=True)
        except Exception as e:
            print("出错"+code)
            print(e)
        else:
            print("爬取成功"+code)
        finally:
            self.index += 1
            print(str(self.index)+"/"+str(self.total))

        pass


    def stock_top_scrapy(self,date,type):

        # 另外一种随机设置请求头部信息的方法
        # header      = random.choice(self.headers)
        # url         = 'http://www.bestgo.com/HD/'+date+'/'+type+'/'
        # self.collection = self.db['yin_top']
        # print(url)
        #
        # try:
        #     r = requests.get(url=url, headers={'User-Agent': header})
        #     r.encoding = r.apparent_encoding
        #     demo = r.text
        #     # print(demo)
        #     #
        #     soup = BeautifulSoup(demo, 'html.parser')  # 解析器：html.parser
        #
        #     items = []
        #     tbodys = soup.select(".tabPanel .panes tbody")
        #     trs_1 = tbodys[0].select("tr")
        #     name = soup.select("#stock_name a")[0].get_text(strip=True)
        #     for i, d in enumerate(trs_1):
        #         tds = d.select("td")
        #         t = tds[0].get_text(strip=True).split("-")
        #
        #         date = datetime.datetime(2018, int(t[0]), int(t[1])).strftime("%Y-%m-%d")
        #         zf = float(tds[2].get_text(strip=True))
        #         yin = float(tds[3].get_text(strip=True))
        #
        #         item = {
        #             'date': date,
        #             'yin': yin,
        #             'zf': zf,
        #             'code': code,
        #             'name': name
        #         }
        #         items.append(item)
        #     self.collection.insert_many(items,ordered=True)
        # except Exception as e:
        #     print("出错"+code)
        #     print(e)
        # else:
        #     print("爬取成功"+code)
        # finally:
        #     self.index += 1
        #     print(str(self.index)+"/"+str(self.total))

        pass

    def query_yin_zt_list(self,startDate,endDate):
        file_name_zt = '../files/yin/' + startDate + '|' + endDate + '-zt.xlsx'
        if not os.path.exists(file_name_zt):
            self.stocks = pd.DataFrame(columns=('代码', '名称', '日期', '涨幅', '隐单', '主单','力度','流通股','概念','行业'))
            industry = base.get_industry_list()
            base_list = base.get_stock_base_list(endDate)
            query = {
                'yin': {
                    '$gte': 100,
                    # '$lte': time_end
                },
                # 'zhu': {
                #     # '$gte': 0,
                #     '$lte': -100
                # },
                'zf': {
                    '$gte': -6.0,
                    '$lte': 6.0
                },
                'date': endDate,
                # 'pCode':i,
                # 'code': code
            }
            res = self.collection.find(query)
            res_list = list(res)
            self.total = len(res_list)
            zt_list = base.get_zt_list(startDate=startDate, endDate=endDate).index
            for j in res_list:
                code = j['code']
                date = j['date']
                yin = float(j['yin'])
                zhu = float(j['zhu'])
                zj = float(j['zj'])
                try:
                    item = {
                        '代码': code,
                        '名称': j['name'],
                        '日期': endDate,
                        '隐单': j['yin'],
                        '主单': j['zhu'],
                        '涨幅': j['zf'],
                        '流通股': long(base_list['流通股数'][j['code']]),
                        '力度': round((long(j['yin']) * 10000) / (long(base_list['流通股数'][j['code']])), 2),
                        '行业': industry.industry[j['code']],
                        '概念': industry.concept[j['code']]
                    }
                except Exception as e:
                    print("出错"+code)
                    print(e)
                    continue

                # for i, zf in enumerate(zfs):
                #     # print(i)
                #     item['第' + str(i + 2) + '天涨幅'] = zf
                # if zhu != 0 and yin / abs(zhu) < 0.3:
                #     continue
                # if zhu > 0 and zhu > yin:
                #     continue
                # if abs(zj) <= 100:
                #     continue

                if code in zt_list:
                    self.stocks = self.stocks.append(item, ignore_index=True)

                if round((long(j['yin']) * 10000) / (long(base_list['流通股数'][j['code']])), 2) <= -0.01:
                    continue
                # else:
                #     global stocks
                #     stocks = stocks.append(item, ignore_index=True)
                # zfs = []
                self.index += 1
                print(str(self.index)+"/"+str(self.total))
                pass
            file_name_zt = '../files/yin/' + startDate + '|' + endDate + '-zt.xlsx'
            writer_zt = pd.ExcelWriter(file_name_zt)
            self.stocks.to_excel(writer_zt, 'Sheet1')
            writer_zt.save()
        else:
            print("存在")
            self.stocks = pd.read_excel(file_name_zt,dtype={'代码':str})
            # print(self.stocks)

        return self.stocks
        pass

    def query_yin_list(self,endDate):
        file_name = '../files/yin/' + endDate + '.xlsx'
        if not os.path.exists(file_name):
            self.stocks_zt = pd.DataFrame(columns=('代码', '名称', '日期', '涨幅' '隐单', '主单','力度','流通股'))
            base_list = base.get_stock_base_list(endDate)
            query = {
                'yin': {
                    '$gte': 100,
                    # '$lte': time_end
                },
                # 'zhu': {
                #     # '$gte': 0,
                #     '$lte': -100
                # },
                'zf': {
                    '$gte': -6.0,
                    '$lte': 6.0
                },
                'date': endDate,
                # 'pCode':i,
                # 'code': code
            }
            res = self.collection.find(query)
            res_list = list(res)
            self.total = len(res_list)
            for j in res_list:
                code = j['code']
                date = j['date']
                # zfs = get_stock_later_info(code, date)
                yin = float(j['yin'])
                zhu = float(j['zhu'])
                zj  = float(j['zj'])
                item = {
                    '代码': code,
                    '名称': j['name'],
                    '日期': endDate,
                    '隐单': j['yin'],
                    '主单': j['zhu'],
                    '涨幅': j['zf'],
                    '流通股': long(base_list['流通股数'][j['code']]),
                    '力度': round((long(j['yin']) * 10000) / (long(base_list['流通股数'][j['code']])), 2),
                    '行业': base_list['行业'][j['code']],
                    '概念': base_list['概念'][j['code']]
                }

                # for i, zf in enumerate(zfs):
                #     # print(i)
                #     item['第' + str(i + 2) + '天涨幅'] = zf
                # if zhu != 0 and yin / abs(zhu) < 0.3:
                #     continue
                # if zhu > 0 and zhu > yin:
                #     continue
                if round((long(j['yin']) * 10000) / (long(base_list['流通股数'][j['code']])), 2) <= -0.01:
                    continue

                self.stocks = self.stocks.append(item, ignore_index=True)
                self.index += 1
                print(str(self.index)+"/"+str(self.total))
                pass
            # print(self.stocks.to_json(orient='split'))
            file_name = '../files/yin/' + endDate + '.xlsx'
            writer = pd.ExcelWriter(file_name)
            self.stocks.to_excel(writer, 'Sheet1')
            writer.save()
        else:
            print("存在")
            self.stocks = pd.read_excel(file_name,dtype={'代码':str})
            # print(self.stocks)
        return self.stocks
        pass

    def query_yin_detail(self,code,startDate,endDate):
        stocks = pd.DataFrame(columns=('股票代码','股票名称','日期', '涨幅', '隐单', '主单','资金'))
        base_list = base.get_stock_base(code=code,startDate=startDate,endDate=endDate)
        # print(base_list)
        base_list = base_list.set_index(['日期'])
        query = {
            'date': {
                '$gte': startDate,
                '$lte': endDate
            },
            'code': code
        }

        res = self.collection.find(query)
        df = pd.DataFrame(list(res))

        del df['_id']
        # print(long(base_list['流通股数'][code]))
        df = df.rename(columns={
            'code': '股票代码',
            'name': '股票名称',
            'date': '日期',
            'zf': '涨幅',
            'yin': '隐单',
            'zhu': '主单',
            'zj': '资金',
        })

        df = df.sort_values(by=['日期'], ascending=False)
        df = df.set_index(['日期'])

        for s in df.index:
            item = {
                '股票代码': df['股票代码'][s],
                '股票名称': df['股票名称'][s],
                '日期': s,
                '涨幅': df['涨幅'][s],
                '隐单': df['隐单'][s],
                '主单': df['主单'][s],
                '资金': df['资金'][s],
                '力度': round((long(df['隐单'][s]) * 10000) / (long(base_list['流通股数'][s])), 2),
                '流通股数': round(base_list['流通股数'][s],0),
                '成交量': float(base_list['成交量'][s]),
                '成交额': round(float(base_list['成交额'][s])),
                
                # '主单': j['zhu'],
                # '原因': zts.reason[code],

            }
            stocks = stocks.append(item, ignore_index=True)

        # base_list = base_list.rename(columns={
        #     'date': '日期'
        # })

        #
        # # print(base_list)
        # # for i in df['日期']:
        # #     print(i)
        # df = pd.merge(df, base_list,on='日期',how='right')
        # print(df)

        # print(stocks)
        return stocks
        # print(list(data))
        # res_list = list(res)
        # self.total = len(res_list)
        # for j in res_list:
        #     code = j['code']
        #     date = j['date']
        #     # zfs = get_stock_later_info(code, date)
        #     yin = float(j['yin'])
        #     zhu = float(j['zhu'])
        #     zj  = float(j['zj'])
        #     item = {
        #         '代码': code,
        #         '名称': j['name'],
        #         '日期': endDate,
        #         '隐单': j['yin'],
        #         '主单': j['zhu'],
        #         '涨幅': j['zf'],
        #         '流通股': long(base_list['流通股数'][j['code']]),
        #         '力度': round((long(j['yin']) * 10000) / (long(base_list['流通股数'][j['code']])), 2),
        #         '行业': base_list.industry[j['code']],
        #         '概念': base_list.concept[j['code']]
        #     }
        #     # for i, zf in enumerate(zfs):
        #     #     # print(i)
        #     #     item['第' + str(i + 2) + '天涨幅'] = zf
        #     if zhu != 0 and yin / abs(zhu) < 0.3:
        #         continue
        #     if zhu > 0 and zhu > yin:
        #         continue
        #     if abs(zj) <= 100:
        #         continue
        #
        #     self.stocks = self.stocks.append(item, ignore_index=True)
        #     self.index += 1
        #     print(str(self.index)+"/"+str(self.total))
        pass

yin = Yin()
# yin.query_yin_detail('002181','2018-05-01','2018-05-18')
# yin.scrapy_yin_tasks('2018-05-21')
# def query_yin_zt_list(date):
#     yin = Yin()
#     return  yin.query_yin_zt_list(startDate=startDate,endDate=)