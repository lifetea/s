from flask import Blueprint, request, jsonify
from api.Query import query as q
query = Blueprint('query', __name__)

@query.route('/hgtList',methods=['POST'])
def hgtList():
    code      = request.json["code"]
    startDate = request.json["startDate"]
    endDate   = request.json["endDate"]
    limit     = request.json["limit"]
    page      = request.json["page"]
    ma        = request.json["ma"]
    res_json  = q.query_hgt_list(code=code,startDate=startDate,endDate=endDate,page=page,limit=limit,ma=ma)
    return res_json

@query.route('/hgtDetail',methods=['POST'])
def hgtDetail():
    code      = request.json["code"]
    startDate = request.json["startDate"]
    endDate   = request.json["endDate"]
    limit     = request.json["limit"]
    page      = request.json["page"]
    if code == "":
        res = {"code": 404}
        return jsonify(res)
    else:
        res_json  = q.query_stock_detail(code=code,startDate=startDate,endDate=endDate,limit=limit,page=page)
        return res_json

@query.route('/yinZtList',methods=['POST'])
def yinZtList():
    startDate   = request.form.get("startDate")
    endDate     = request.form.get("endDate")
    page        = int(request.form.get("page"))
    limit       = int(request.form.get("limit"))
    res_json    = q.get_yin_zt_list(startDate=startDate,endDate=endDate,limit=limit,page=page)
    return res_json

#隐单列表
@query.route('/yinList',methods=['POST'])
def yinList():
    # code      = request.json["code"]
    startDate = request.json["startDate"]
    endDate   = request.json["endDate"]
    limit     = request.json["limit"]
    page      = request.json["page"]
    res_json    = q.get_yin_list(endDate=endDate,limit=limit,page=page)
    return res_json

#隐单详情
@query.route('/yinDetail',methods=['POST'])
def yinDetail():
    code      = request.json["code"]
    startDate = request.json["startDate"]
    endDate   = request.json["endDate"]
    limit     = request.json["limit"]
    page      = request.json["page"]
    res_json    = q.query_yin_detail(code=code,startDate=startDate,endDate=endDate,limit=limit,page=page)
    return res_json


# 涨停列表
@query.route('/ztList',methods=['POST'])
def ztList():
    startDate = request.json["startDate"]
    endDate   = request.json["endDate"]
    limit     = request.json["limit"]
    status = request.json["status"]
    page      = request.json["page"]
    res_json        = q.get_zt_list(startDate=startDate,endDate=endDate,page=page,limit=limit,status=status)
    return res_json

# 异动列表
@query.route('/ydList',methods=['POST'])
def ydList():
    code        = request.json["code"]
    startDate   = request.json["startDate"]
    endDate     = request.json["endDate"]
    page        = request.json["page"]
    limit       = request.json["limit"]
    type        = request.json["type"]
    query        = request.json["query"]
    res_json = q.query_yd_list(code=code,startDate=startDate, endDate=endDate, limit=limit, page=page,type=type,query=query)
    return res_json

# 日历列表
@query.route('/calendarList',methods=['POST'])
def calendarList():
    startDate = request.json["startDate"]
    endDate   = request.json["endDate"]
    limit     = request.json["limit"]
    page      = request.json["page"]
    res_json        = q.query_calendar_list(startDate=startDate, endDate=endDate, limit=limit, page=page)
    return res_json

# 追踪列表
@query.route('/observeList',methods=['POST'])
def observeList():
    startDate = request.json["startDate"]
    endDate   = request.json["endDate"]
    limit     = request.json["limit"]
    page      = request.json["page"]
    print(request.json)
    res_json = q.query_observe_list(startDate=startDate, endDate=endDate, limit=limit, page=page)
    return res_json

# 基本列表
@query.route('/baseList',methods=['POST'])
def baseList():
    startDate = request.json["startDate"]
    endDate   = request.json["endDate"]
    limit     = request.json["limit"]
    page      = request.json["page"]
    res_json        = q.query_base_list(startDate=startDate,endDate=endDate,limit=limit, page=page)
    return res_json

# 行业列表
@query.route('/industry',methods=['POST'])
def industry():
    limit     = request.json["limit"]
    page      = request.json["page"]
    res_json  = q.query_industry_list(limit=limit, page=page)
    return res_json


# 搜索列表
@query.route('/searchList',methods=['POST'])
def searchList():
    content         = request.form.get("code")
    res_json        = q.query_stock_search_list(content)
    return res_json

# 资金明细
@query.route('/flowDetail',methods=['POST'])
def flowDetail():
    code        = request.json["code"]
    startDate   = request.json["startDate"]
    endDate     = request.json["endDate"]
    page        = request.json["page"]
    limit       = request.json["limit"]
    # type        = request.json["type"]
    # query       = request.json["query"]
    res_json = q.query_flow_detail(endDate=endDate,code=code)
    return res_json


# 龙虎列表
@query.route('/lhList',methods=['POST'])
def lhList():
    # code        = request.json["code"]
    startDate   = request.json["startDate"]
    endDate     = request.json["endDate"]
    page        = request.json["page"]
    limit       = request.json["limit"]
    # type        = request.json["type"]
    # query       = request.json["query"]
    res_json = q.query_lh_list(startDate=startDate,endDate=endDate,limit=limit, page=page)
    return res_json