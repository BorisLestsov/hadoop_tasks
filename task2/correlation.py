
from __future__ import print_function

import sys
from collections import namedtuple

import pyspark
import pyspark.sql
from pyspark.sql import SparkSession
from operator import itemgetter

import re
# pattern = re.compile(".*")
# candle_width = 300000
# date_start_begin = 19000101
# date_start_end =  20200101
# candle_start_begin =  1000
# candle_start_end =  1800

pattern = re.compile(".*")
candle_width = 300000
date_start_begin = 19000101
date_start_end =  20200101
candle_start_begin =  1000
candle_start_end =  2000


Value = namedtuple('Value', ['price', 'close_time', 'close_id'])

PairS = namedtuple('PairS', ["s1", "s2"])
PairP = namedtuple('PairP', ["moment", "p1", "p2"])

def map_f(line):
    line_split = line.split(',')

    symbol = line_split[0]
    if not pattern.match(symbol):
        return None, None

    moment = line_split[2]

    if len(moment) != 17:
        return None, None


    hhmm = int(moment[8:8+4])
    yyyymmdd = int(moment[0:0+8])


    if yyyymmdd < date_start_begin or yyyymmdd >= date_start_end:
        return None, None

    if hhmm < candle_start_begin or hhmm >= candle_start_end:
        return None, None

    moment_long = int(line_split[2])

    moment_mil = int(moment[8:8+2])*60*60*1000 + \
                 int(moment[10:10+2])*60*1000 + \
                 int(moment[12:12+2])*1000 + \
                 int(moment[14:14+3])

    candle_start = (moment_mil / candle_width) * candle_width
    hh = candle_start/1000/60/60
    mm = (candle_start - hh*60*60*1000)/1000/60
    ss = (candle_start - hh*60*60*1000 - mm*60*1000)/1000
    fff = (candle_start - hh*60*60*1000 - mm*60*1000 - ss*1000)
    candle_moment = int('{}{:.2g}{:.2g}{:.2g}{:03g}'.format(yyyymmdd, hh, mm, ss, fff))

    key_out = (symbol, candle_moment)
    
    moment_id = int(line_split[3])
    price = float(line_split[4])

    value_out = Value(price, moment_long, moment_id)

    return key_out, value_out


def reduce_f(val1, val2):
    if val1 is None or val2 is None:
        return

    if val1.close_time > val2.close_time:
        close_price = val1.price
        close_time = val1.close_time
        close_id = val1.close_id
    elif val1.close_time < val2.close_time:
        close_price = val2.price
        close_time = val2.close_time
        close_id = val2.close_id
    else:
        if val1.close_id > val2.close_id:
            close_price = val1.price
            close_time = val1.close_time
            close_id = val1.close_id
        else:
            close_price = val2.price
            close_time = val2.close_time
            close_id = val2.close_id

    return Value(close_price, close_time, close_id)

def map_pairs(x):
    moment, s1, p1, s2, p2 = x
    key = PairS(s1, s2)
    value = PairP(moment, p1, p2)

    return key, value

import math

def average(x):
    assert len(x) > 0
    return float(sum(x)) / len(x)

def pearson_def(x, y):
    assert len(x) == len(y)
    n = len(x)
    assert n > 0
    avg_x = average(x)
    avg_y = average(y)
    diffprod = 0
    xdiff2 = 0
    ydiff2 = 0
    for idx in range(n):
        xdiff = x[idx] - avg_x
        ydiff = y[idx] - avg_y
        diffprod += xdiff * ydiff
        xdiff2 += xdiff * xdiff
        ydiff2 += ydiff * ydiff

    return diffprod / math.sqrt(xdiff2 * ydiff2)

def map_diff(x):
    key, val = x
    if key.s1 >= key.s2:
        return key, None
    val = list(val)
    val.sort(key=itemgetter(0))
    diffs1 = tuple( (val[i+1][1]-val[i][1])/val[i][1] for i in xrange(len(val)-1))
    diffs2 = tuple( (val[i+1][2]-val[i][2])/val[i][2] for i in xrange(len(val)-1))

    if len(diffs1) <= 1:
        res = None
    else:
        res = pearson_def(diffs1, diffs2)
    return key, res

if __name__=="__main__":
    if len(sys.argv) != 2:
        print("Usage: <file>", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession.builder\
        .appName("PythonCandle")\
        .getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')

    text = spark.read.text(sys.argv[1])
    header = text.first()
    rdd = text.rdd
    lines = rdd.map(lambda r: r[0])
    # lines = rdd

    field_map = {v: k for k, v in enumerate(header.value[1:].split(','))}

    counts = lines.map(map_f).reduceByKey(reduce_f).filter(lambda x: x[0] is not None)
    #out_df = counts.flatMap(lambda x:  (x[0][0], x[0][1], x[1].price, x[1].close_time, x[1].close_id))
    out_df = counts.flatMap(lambda x:  ((x[0][0], x[0][1], x[1].price), ))
    out_df = out_df.toDF(["SYMBOL", "MOMENT", "CLOSE_PRICE"])

    pairs = out_df.join(out_df, ["MOMENT"], 'inner')
    pairs = pairs.sort("MOMENT")

    res = pairs.rdd.map(map_pairs).groupByKey().map(map_diff).toDF().na.drop()
    res.show()

#    output = counts.collect()
    
#     for (k, v) in output:
#         if k is None or v is None:
#             continue
#         print(k[0], k[2], v.price)

    #for (word, count) in output:
    #    print("LEL", "%s: %i" % (word, count))

    spark.stop()
