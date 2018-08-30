import tushare as ts
import logging
logging.basicConfig(level=logging.INFO)

list = ts.get_hist_data('300686',start='2017-11-1',end='2018-01-18')

#九日线
MA19         = list.close.values[0:19].mean()

#收盘价
close       = list.close
# print(close)
#今天开盘价
price       = float(close.values[0])
#今日最低价
low_price   = price * 0.92

ma20_rate    = round(abs(price - (MA19 * 19 + price) / 20) / price * 100, 2)
print("20日乖离率:",ma20_rate,"Ma19:",MA19)
suit_rate   = 0.012


leve = ""

if ma20_rate >= 0.5 and ma20_rate < 1.10:
    leve = "a"
    suit_rate = 0
if ma20_rate >= 1.10 and ma20_rate < 1.40:
    leve = "b-1"
    suit_rate = 0.03
if ma20_rate >= 1.40 and ma20_rate < 1.80:
    leve = "b-2"
    suit_rate = 0.006
if ma20_rate >= 1.80 and ma20_rate < 2.00:
    leve = "c-1"
    suit_rate = 0.009
if ma20_rate >= 2.00 and ma20_rate < 2.30:
    leve = "c-2"
    suit_rate = 0.010
if ma20_rate >= 2.30 and ma20_rate < 2.50:
    leve = "d-1"
    suit_rate = 0.011
if ma20_rate >= 2.50 and ma20_rate < 2.80:
    leve = "d-2"
    suit_rate = 0.012
if ma20_rate >= 2.80 and ma20_rate <= 3.10:
    leve = "e"
    suit_rate = 0.014
if ma20_rate > 3.10 and ma20_rate <= 3.40:
    leve = "f"
    suit_rate = 0.016
if ma20_rate > 3.40 and ma20_rate <= 3.70:
    leve = "g-1"
    suit_rate = 0.018
if ma20_rate > 3.70 and ma20_rate <= 4.10:
    leve = "g-2"
    suit_rate = 0.018
if ma20_rate > 4.10 and ma20_rate <= 4.40:
    leve = "h-1"
    suit_rate = 0.018
if ma20_rate > 4.40 and ma20_rate <= 4.70:
    leve = "h-2"
    suit_rate = 0.020
if ma20_rate > 4.70 and ma20_rate <= 5.00:
    leve = "i"
    suit_rate = 0.019
if ma20_rate > 5.00 and ma20_rate <= 5.40:
    leve = "j"
    suit_rate = 0.026
if ma20_rate > 5.40 and ma20_rate <= 5.90:
    leve = "k"
    suit_rate = 0.034
if ma20_rate > 5.90:
    leve = "l"
    suit_rate = 0.038
suit = 0.0

while (price >= low_price):
    price = price - 0.01
    MA20 = (MA19 * 9 + price) / 10
    suit = MA20 + float(close.values[0]) * suit_rate
    if price <= suit:
        break


print("股票代码:","预买入价格:",round(price,2),"偏移率:",ma20_rate,"修正值:",suit_rate,"MA9:",MA19)
