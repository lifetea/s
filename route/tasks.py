from flask import Blueprint, request
from scrapy.WenCai import base_tasks,zt_tasks
from scrapy.Yin import scrapy_yin_tasks
from scrapy.HGT import scrapy_stock_tasks,scrapy_detail_tasks
scrapy = Blueprint('scrapy', __name__)

@scrapy.route('/base',methods=['POST'])
def base():
    date = request.form.get("date")
    base_tasks(date)
    return '<h1>base</h1>'

@scrapy.route('/zt',methods=['POST'])
def zt():
    date = request.form.get("date")
    zt_tasks(date)
    return '<h1>zt</h1>'


@scrapy.route('/yin',methods=['POST'])
def yin():
    date = request.form.get("date")
    scrapy_yin_tasks(date)
    return '<h1>yin</h1>'


@scrapy.route('/hgtList',methods=['POST'])
def hgt_list():
    date = request.form.get("date")
    scrapy_stock_tasks(date)
    return '<h1>hgt-list</h1>'


@scrapy.route('/hgtDetail',methods=['POST'])
def hgt_detail():
    start = request.form.get("start")
    end = request.form.get("end")
    scrapy_detail_tasks(start,end)
    return '<h1>hgtDetail</h1>'