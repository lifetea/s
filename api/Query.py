import pandas as pd
from scrapy.Base import base
from scrapy.Yin import Yin
from scrapy.HGT import HGT
from scrapy.conf import conf
from scrapy.WenCai import wencai
from api.Observe import observe
from api.Flow import flow
from api.LH import lh
import pymongo
import json
class Query:
    def __init__(self):
        self.client     = pymongo.MongoClient('localhost', 27017)
        self.db         = None
        pass
    def get_zt_list(self,startDate,endDate,page,limit,status):
        stocks = pd.DataFrame(columns=('代码', '名称', '日期', '备注', '状态'))
        zts = base.get_zt_list(startDate=startDate,endDate=endDate)
        industry = base.get_industry_list()
        # print(zts)
        for code in zts.index:
            item = {
                '代码': code,
                '名称': zts.name[code],
                '日期': zts.date[code],
                # '隐单': j['yin'],
                # '主单': j['zhu'],
                # '原因': zts.reason[code],
                '状态': zts.status[code],
                '备注': zts.remark[code],
                '行业': industry.industry[code],
                '概念': industry.concept[code]
            }
            stocks = stocks.append(item, ignore_index=True)
        if status == -1:
            stocks = stocks[stocks['状态'] < 0]
        if status == 1:
            stocks = stocks[stocks['状态'] >= 0]
        if status == 0:
            stocks = stocks[stocks['状态'] >= 0]
        # print(zt)
        total = len(stocks)
        str = stocks[(page - 1) * limit:page * limit].to_json(orient='split')
        res = json.loads(str)
        res['total'] = total
        return json.dumps(res)
        pass

    def query_base_list(self,startDate,endDate,page,limit):
        base_list   = base.get_stock_base_list(endDate)
        return base_list[0:10].to_json(orient='split')
        pass


    def get_yin_zt_list(self,startDate,endDate,page,limit):
        yin     = Yin()
        stocks  = yin.query_yin_zt_list(startDate=startDate,endDate=endDate)
        total = len(stocks)
        stocks  = stocks.sort_values(by=['力度','行业'],ascending=False)
        str = stocks[(page - 1) * limit:page * limit].to_json(orient='split')
        res = json.loads(str)
        res['total'] = total
        return json.dumps(res)
    def get_yin_list(self,endDate,page,limit):
        yin     = Yin()
        stocks  = yin.query_yin_list(endDate=endDate)
        total = len(stocks)
        stocks  = stocks.sort_values(by=['力度','行业'],ascending=False)
        str = stocks[(page - 1) * limit:page * limit].to_json(orient='split')
        res = json.loads(str)
        res['total'] = total
        return json.dumps(res)

    def query_yin_detail(self,code,startDate,endDate,page,limit):
        yin     = Yin()
        stocks  = yin.query_yin_detail(code=code,startDate=startDate,endDate=endDate)
        total = len(stocks)
        # stocks  = stocks.sort_values(by='行业',ascending=False)
        str = stocks[(page - 1) * limit:page * limit].to_json(orient='split')
        res = json.loads(str)
        res['total'] = total
        return json.dumps(res)

    def query_hgt_list(self,code,startDate,endDate,page,limit,ma):
        hgt     = HGT(start=startDate,end=endDate)
        stocks  = hgt.query_multiple()
        total   = len(stocks)
        print(ma)
        ma_pd     = stocks[stocks.columns[-4]]-stocks[stocks.columns[-(4+int(ma))]]

        stocks['MA'] = ma_pd

        if code != None and code != "":
            return stocks[stocks['代码'] == code].to_json(orient='split')
        else:
            stocks  = stocks.sort_values(by='MA',ascending=False)
            str     = stocks[(page-1)*limit:page*limit].to_json(orient='split')
            res     = json.loads(str)
            res['total'] = total
            return json.dumps(res)
        pass
    # 获取hgt明细
    def query_stock_detail(self, startDate,endDate,code,page,limit):
        hgt     = HGT(start=startDate,end=endDate)
        stocks  = hgt.query_hold_detail(code)
        stocks = stocks[stocks[stocks.columns[-1]] > 0]
        last_column = stocks.columns[-1]
        total = len(stocks)
        stocks  = stocks.sort_values(by=last_column,ascending=False)
        ma1     = stocks[stocks.columns[-1]]-stocks[stocks.columns[-2]]
        stocks['MA1'] = ma1
        str = stocks[(page - 1) * limit:page * limit].to_json(orient='split')
        res = json.loads(str)
        res['total'] = total
        return json.dumps(res)
        pass

    def query_stock_search_list(self, content):
        return  json.dumps(base.get_stock_search_list(content))
        pass

    def query_yd(self,tag):
        # collection = self.db['异动']
        # result = collection.aggregate([
        #
        #     # Group the documents and "count" via $sum on the values
        #     {"$group": {
        #         "_id": {
        #             "tag": "$tag",
        #         }
        #     }}
        # ])
        # print(list(result))
        pass
    def query_yd_list(self,code,startDate,endDate,page,limit,type,query):
        stocks = wencai.query_yd_list(startDate=startDate,endDate=endDate)
        if type == 0:
            pass
        if type == 1:
            stocks = stocks[stocks['涨幅'] > 0]
        if type == 2:
            stocks = stocks[stocks['涨幅'] > 6]
        if type == 3:
            stocks = stocks[stocks['涨幅'] > 9]
        if query != None and query != "":
            stocks = stocks[stocks['异动原因'].str.contains(query)]
        if code != None and code != "":
            stocks = stocks[stocks['股票代码'] == code]
        total = len(stocks)
        str = stocks[(page - 1) * limit:page * limit].to_json(orient='split')
        res = json.loads(str)
        res['total'] = total
        return json.dumps(res)
        pass

    def query_calendar_list(self,startDate,endDate,page,limit):
        stocks = conf.get_calendar_list(startDate=startDate,endDate=endDate)
        total = len(stocks)
        str = stocks[(page - 1) * limit:page * limit].to_json(orient='split')
        res = json.loads(str)
        res['total'] = total
        return json.dumps(res)
    # 获取跟踪列表
    def query_observe_list(self,startDate,endDate,page,limit):
        stocks = observe.query(startDate=startDate,endDate=endDate)
        total = len(stocks)
        str = stocks[(page - 1) * limit:page * limit].to_json(orient='split')
        res = json.loads(str)
        res['total'] = total
        return json.dumps(res)

    def query_industry_list(self,page,limit):
        stocks = base.get_industry_list()
        total = len(stocks)
        str = stocks[(page - 1) * limit:page * limit].to_json(orient='split')
        res = json.loads(str)
        res['total'] = total
        return json.dumps(res)
        pass

    # 资金流
    def query_flow_detail(self,endDate,code):
        stock = flow.query(endDate=endDate,code=code)
        # str = stocks[(page - 1) * limit:page * limit].to_json(orient='split')
        # res = json.loads(str)
        # res['total'] = total
        print(stock)
        return json.dumps(stock)
        pass

    # 龙虎列表
    def query_lh_list(self,startDate,endDate,page,limit):
        stocks = lh.query(endDate=endDate)
        total = len(stocks)
        str = stocks[(page - 1) * limit:page * limit].to_json(orient='split')
        res = json.loads(str)
        res['total'] = total
        return json.dumps(res)
        pass

query = Query()
