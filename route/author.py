from flask import Blueprint, jsonify

author = Blueprint('author', __name__)

@author.route('/login',methods=['POST'])
def login():
    res = {"code":200}
    print(res)
    return jsonify(res)