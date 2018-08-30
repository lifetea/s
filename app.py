from flask import Flask
from route.author import author
from route.spider import spider
from route.query import query
from route.save import save
from route.delete import delete

app = Flask(__name__)
app.register_blueprint(author,url_prefix='/author')
app.register_blueprint(query,url_prefix='/query')
app.register_blueprint(spider,url_prefix='/spider')
app.register_blueprint(save,url_prefix='/save')
app.register_blueprint(delete,url_prefix='/delete')
@app.route('/')
def index():
    return '<h1>root test</h1>'


if __name__ == '__main__':
    app.debug = True
    app.run()
    # threaded=True, port = 5000