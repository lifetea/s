import tushare as ts
import pandas as pd
import logging
logging.basicConfig(level=logging.INFO)

list = ts.get_hist_data('300033',start='2017-12-01',end='2018-01-24')


#九日线
MA4         = list.close.values[0:4].mean()

#收盘价
close       = list.close

# print(close)

#今天开盘价
price       = float(close.values[0])
print("price:",price)
#今日最低价
low_price   = price * 0.92

ma5_rate    = round(abs(price - (MA4 * 4 + price) / 5) / price * 100, 2)
print("10日乖离率:",ma5_rate,"Ma9:",MA4)
suit_rate   = 0.012

logging.info("MA9: %s ",(MA4))

leve = ""

if ma5_rate >= 0.5 and ma5_rate < 1.10:
    leve = "a"
    suit_rate = 0
if ma5_rate >= 1.10 and ma5_rate < 1.40:
    leve = "b-1"
    suit_rate = 0.03
if ma5_rate >= 1.40 and ma5_rate < 1.80:
    leve = "b-2"
    suit_rate = 0.006
if ma5_rate >= 1.80 and ma5_rate < 2.00:
    leve = "c-1"
    suit_rate = 0.009
if ma5_rate >= 2.00 and ma5_rate < 2.30:
    leve = "c-2"
    suit_rate = 0.010
if ma5_rate >= 2.30 and ma5_rate < 2.50:
    leve = "d-1"
    suit_rate = 0.011
if ma5_rate >= 2.50 and ma5_rate < 2.80:
    leve = "d-2"
    suit_rate = 0.012
if ma5_rate >= 2.80 and ma5_rate <= 3.10:
    leve = "e"
    suit_rate = 0.014
if ma5_rate > 3.10 and ma5_rate <= 3.40:
    leve = "f"
    suit_rate = 0.016
if ma5_rate > 3.40 and ma5_rate <= 3.70:
    leve = "g-1"
    suit_rate = 0.018
if ma5_rate > 3.70 and ma5_rate <= 4.10:
    leve = "g-2"
    suit_rate = 0.018
if ma5_rate > 4.10 and ma5_rate <= 4.40:
    leve = "h-1"
    suit_rate = 0.020
if ma5_rate > 4.40 and ma5_rate <= 4.70:
    leve = "h-2"
    suit_rate = 0.020
if ma5_rate > 4.70 and ma5_rate <= 5.00:
    leve = "i"
    suit_rate = 0.019
if ma5_rate > 5.00 and ma5_rate <= 5.40:
    leve = "j"
    suit_rate = 0.028
if ma5_rate > 5.40 and ma5_rate <= 5.90:
    leve = "k"
    suit_rate = 0.034
if ma5_rate > 5.90 and ma5_rate <= 6.40:
    leve = "l"
    suit_rate = 0.038
if ma5_rate > 6.90 and ma5_rate <= 7.40:
    leve = "l"
    suit_rate = 0.038
if ma5_rate > 7.40 and ma5_rate <= 7.90:
    leve = "m"
    suit_rate = 0.040
if ma5_rate > 7.90 and ma5_rate <= 8.40:
    leve = "n"
    suit_rate = 0.038
suit = 0.0

while (price >= low_price):
    price = price - 0.01
    MA10 = (MA4 * 4 + price) / 5
    suit = MA10 + float(close.values[0]) * suit_rate
    if price <= suit:
        break

print("股票代码:","预买入价格:",round(price,2))
