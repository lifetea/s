# server

生成requirements.txt文件
pip freeze > requirements.txt

安装requirements.txt依赖
pip install -r requirements.txt

激活 venv
source venv/bin/activate

退出 venv
deactivate

启动 gunicorn
gunicorn -w 4 -b 127.0.0.1:5000 app:app