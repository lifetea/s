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
collection  = db['yin']

global stocks
stocks      = pd.DataFrame()
global stocks_zt
stocks_zt      = pd.DataFrame()
time_start      = datetime.datetime(2018, 3, 1).strftime("%Y-%m-%d")
time_end        = datetime.datetime(2018, 3, 9).strftime("%Y-%m-%d")


def get_stock_later_info(code,date):
    query = {
        'code':code,
        'date': {
            '$gt': date,
        }
    }
    sort = [('date', 1)]
    data = pd.DataFrame(list(collection.find(query,sort=sort,limit=10)))
    if data.empty:
        return []
    else:
        return data.zf
    pass

def get_stock_detail():
    query = {
        'yin':{
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
        'date': {
            '$gte': time_start,
            '$lte': time_end
        },
        # 'pCode':i,
        # 'code': code
    }
    res = collection.find(query)
    res_list = list(res)
    # data = pd.DataFrame(list(collection.find(query)))
    # del data['_id']
    # data['r']= []
    # print(res_list)
    for j in res_list:
        code = j['code']
        date = j['date']
        zfs  = get_stock_later_info(code, date)
        yin  = float(j['yin'])
        zhu  = float(j['zhu'])
        zj   = float(j['zj'])
        zt_list = get_zt_65list(time_start)
        item = {
            '0代码':code,
            '0名称':j['name'],
            '0日期':date,
            '1隐单': j['yin'],
            '1主单': j['zhu'],
            '2当天涨幅':j['zf'],
        }
        for i,zf in enumerate(zfs):
            # print(i)
            item['第'+str(i+2)+'天涨幅'] = zf
        if zhu !=0 and yin/abs(zhu) < 0.3:
            continue
        if zhu > 0 and zhu > yin:
            continue
        if abs(zj) <= 100:
            continue
        if code in zt_list:
            global stocks_zt
            stocks_zt = stocks_zt.append(item, ignore_index=True)
        else:
            global stocks
            stocks= stocks.append(item,ignore_index=True)
        zfs = []
        pass
    file_name = './yin/'+time_end+'.xlsx'
    writer = pd.ExcelWriter(file_name)
    stocks.to_excel(writer, 'Sheet1')
    writer.save()
    file_name_zt = './yin/'+time_end+'-zt.xlsx'
    writer_zt = pd.ExcelWriter(file_name_zt)
    stocks_zt.to_excel(writer_zt, 'Sheet1')
    writer_zt.save()


def get_zt_1list(date):
    c = pymongo.MongoClient('localhost', 27017)
    db = c['stock']
    con = db['zt']
    query={
        'date':{
            '$gte':date
        }
    }
    res = con.find(query)
    zt_list = pd.DataFrame(list(res))
    return list(zt_list.code)
    pass



def get_stock_single(code,start,end):
    query = {
        'code':code,
        'date': {
            '$gte': start,
            '$lte': end
        },
        # 'pCode':i,
        # 'code': code
    }
    sort = [('date', 1)]
    res = collection.find(query,sort=sort)
    res_list = list(res)


    for j in res_list:
        code = j['code']
        date = j['date']
        yin  = float(j['yin'])
        zhu  = float(j['zhu'])
        zj   = float(j['zj'])

        item = {
            '0代码':code,
            '0名称':j['name'],
            '0日期':date,
            '1隐单': j['yin'],
            '1主单': j['zhu'],
            '1当天涨幅':j['zf'],
            '2量能':j['volume'],
            '2价格': j['close']
        }
        global stocks
        stocks= stocks.append(item,ignore_index=True)
        zfs = []
        pass
    file_name = './yin/'+code+'|'+end+'.xlsx'
    writer = pd.ExcelWriter(file_name)
    stocks.to_excel(writer, 'Sheet1')
    writer.save()

# get_stock_single("002137","2018-01-01","2018-03-06")
get_stock_detail()

