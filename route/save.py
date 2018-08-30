from flask import Blueprint, request, jsonify
from api.Save import save as s
save = Blueprint('save', __name__)


@save.route('/observe',methods=['POST'])
def observe():
    item = request.json
    s.save_observe(item)
    return '<h1>成功</h1>'

@save.route('/zt',methods=['POST'])
def zt():
    item = request.json
    print(item)
    s.save_zt(item)
    res = {"code":200}
    return jsonify(res)

@save.route('/ztClear',methods=['POST'])
def ztClear():
    item = request.json
    print(item)
    s.clear_zt(startDate=item['startDate'],endDate=item['endDate'])
    res = {"code":200}
    return jsonify(res)

@save.route('/calendar',methods=['POST'])
def calendar():
    item = request.json
    s.save_calendar(item)
    res = {"code":200}
    return jsonify(res)

# @spider.route('/hgtList',methods=['POST'])
# def hgt_list():
#     endDate = request.form.get("endDate")
#     s.spider_hgt_list(endDate=endDate)
#     return '<h1>hgt-list</h1>'
#
# #
# @spider.route('/hgtDetail',methods=['POST'])
# def hgt_detail():
#     startDate  = request.json['startDate']
#     endDate    = request.json['endDate']
#     stock_list = request.json['stockList']
#     s.spider_hgt_detail(startDate=startDate,endDate=endDate,stock_list=stock_list)
#     return '<h1>hgtDetail</h1>'