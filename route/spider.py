from flask import Blueprint, request, jsonify
from api.Spider import spider as s
import json
spider = Blueprint('spider', __name__)

# @scrapy.route('/base',methods=['POST'])
# def base():
#     date = request.form.get("date")
#     base_tasks(date)
#     return '<h1>base</h1>'
#
# @scrapy.route('/zt',methods=['POST'])
# def zt():
#     date = request.form.get("date")
#     zt_tasks(date)
#     return '<h1>zt</h1>'
#
#
@spider.route('/yinList',methods=['POST'])
def yinList():
    endDate = request.form.get("endDate")
    s.spider_yin_list(endDate=endDate)
    res = {"code":200}
    return jsonify(res)

#爬取异动
@spider.route('/yin',methods=['POST'])
def yin():
    code = request.json['code']
    s.spider_yin(code=code)
    res = {"code":200}
    return jsonify(res)

@spider.route('/ztYin',methods=['POST'])
def ztYin():
    startDate  = request.json['startDate']
    endDate    = request.json['endDate']
    s.scrapy_yin_zt_tasks(startDate=startDate,endDate=endDate)
    res = {"code":200}
    return jsonify(res)

@spider.route('/ztHs',methods=['POST'])
def ztHs():
    startDate  = request.json['startDate']
    endDate    = request.json['endDate']
    s.scrapy_zt_hs_tasks(startDate=startDate,endDate=endDate)
    res = {"code":200}
    return jsonify(res)

#爬取异动
@spider.route('/yd',methods=['POST'])
def yd():
    endDate = request.json['endDate']
    s.spider_yd(endDate=endDate)
    res = {"code":200}
    return jsonify(res)

#爬取涨停
@spider.route('/zt',methods=['POST'])
def zt():
    endDate = request.json['endDate']
    s.scrapy_zt(endDate=endDate)
    res = {"code":200}
    return jsonify(res)

#爬取行业
@spider.route('/industry',methods=['POST'])
def industry():
    s.spider_industry()
    res = {"code":200}
    return jsonify(res)

#爬取基础
@spider.route('/baseList',methods=['POST'])
def baseList():
    endDate = request.json['endDate']
    s.spider_base_list(endDate=endDate)
    res = {"code":200}
    return jsonify(res)


@spider.route('/hgtList',methods=['POST'])
def hgt_list():
    endDate = request.form.get("endDate")
    s.spider_hgt_list(endDate=endDate)
    res = {"code":200}
    return jsonify(res)

#
@spider.route('/hgtDetail',methods=['POST'])
def hgt_detail():
    startDate  = request.json['startDate']
    endDate    = request.json['endDate']
    stock_list = request.json['stockList']
    s.spider_hgt_detail(startDate=startDate,endDate=endDate,stock_list=stock_list)
    res = {"code":200}
    return jsonify(res)


#爬取龙虎列表
@spider.route('/lhList',methods=['POST'])
def lhList():
    endDate = request.json['endDate']
    s.spider_hl_list(endDate=endDate)
    res = {"code":200}
    return jsonify(res)