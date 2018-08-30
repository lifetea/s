import tushare as ts
import logging
logging.basicConfig(level=logging.INFO)

# list = ts.get_hist_data('300708',start='2017-11-11',end='2017-12-12')
ts.get_stock_basics()