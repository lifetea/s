from flask import Blueprint, request, jsonify
from api.Delete import delete as d
delete = Blueprint('delete', __name__)


@delete.route('/hgtList',methods=['POST'])
def hgtList():
    item = request.json
    file_name = '../files/hgt/' + item['startDate'] + '|' + item['endDate'] + '.xlsx'
    d.delete_file(file_name)
    res = {"code":200}
    return jsonify(res)

@delete.route('/hgtDetail',methods=['POST'])
def hgtDetail():
    item = request.json
    file_name = '../files/detail/' + item['code'] + '|' + item['startDate'] + '-' + item['endDate'] + '.xlsx'
    d.delete_file(file_name)
    res = {"code":200}
    return jsonify(res)

@delete.route('/industry',methods=['POST'])
def industry():
    file_name = '../files/base/industry.xlsx'
    d.delete_file(file_name)
    d.delete_db(db_name='industry',query={})
    res = {"code":200}
    return jsonify(res)


@delete.route('/base',methods=['POST'])
def base():
    item = request.json
    file_name = '../files/base/base' + '|' + item['endDate'] + '.xlsx'
    d.delete_file(file_name)
    d.delete_db(db_name='base', query={'date': item['endDate']})
    res = {"code":200}
    return jsonify(res)

