__name__ = 'stock-detail-query'
import datetime
import tushare as ts
from bs4 import BeautifulSoup
import requests
from pyjsparser import PyJsParser
import json
import pymongo
import pandas as pd
import re

#数据库信息
client      = pymongo.MongoClient('localhost',27017)
db          = client['stock']
collection  = db['detail']

time_start      = datetime.datetime(2018, 1, 14).strftime("%Y-%m-%d")
time_end        = datetime.datetime(2018, 3, 9).strftime("%Y-%m-%d")
code            = "002035"
global stocks
stocks      = pd.DataFrame()



def get_participant_list():
    query = {
        'date':{
            '$gte': time_start,
            '$lte': time_end
        },
        'code':code
    }
    res = collection.find(query).distinct('pCode')
    p_list = list(res)
    # print(p_list)
    # n = 0
    # for i in stock_list:
    #     print(n)
    #     n += 1
    #     query_detail(i['code'])
    # writer = pd.ExcelWriter('output.xlsx')
    # stocks.to_excel(writer, 'Sheet1')
    # writer.save()
    return  p_list
    pass


def get_stock_detail(p_list):
    for i in p_list:
        query = {
            'date':{
                '$gte': time_start,
                '$lte': time_end
            },
            'pCode':i,
            'code': code
        }
        res = collection.find(query)
        res_list = list(res)
        item = {
            '0机构':i,
        }
        for j in res_list:
            item[j['date']] = j['hold']
            item['1机构'] = j['pName']
            # print(j)
            pass
        global stocks
        s = pd.Series(item)
        stocks= stocks.append(item,ignore_index=True)
        file_name = './detail/'+code+'|'+time_end+'.xlsx'
        writer = pd.ExcelWriter(file_name)
        stocks.to_excel(writer, 'Sheet1')
        writer.save()


p_list =get_participant_list()

get_stock_detail(p_list)
#

#

#
# stocks['111']=['22']
#
# print(stocks)

