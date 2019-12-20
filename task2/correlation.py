
from __future__ import print_function

import sys
from collections import namedtuple
import re
import math
from operator import itemgetter
import copy

import pyspark
import pyspark.sql
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

import argparse
def list_str(values):
    return tuple(map(int, values.split(',')))



Value = namedtuple('Value', ['price', 'close_time', 'close_id'])

# TODO: should not be shift ???
PairS = namedtuple('PairS', ["s1", "s2", "width", "shift"])
PairP = namedtuple('PairP', ["moment", "p1", "p2"])

def parse_csv(line):
    line_split = line[0].split(',')
    symbol, _, moment, moment_id, price, _, _, _ = line_split

    if not pattern.match(symbol):
        return None, None
    if len(moment) != 17:
        return None, None

    # str, str, int, float
    return (symbol, moment, int(moment_id), float(price))


def to_out(data):
  return ' '.join(str(d) for d in data)


def map_f(line):

    symbol, moment, moment_id, price, candle_width= line
    moment_long = int(moment)

    hhmm = int(moment[8:8+4])
    yyyymmdd = int(moment[0:0+8])

    moment_mil = int(moment[8:8+2])*60*60*1000 + \
                 int(moment[10:10+2])*60*1000 + \
                 int(moment[12:12+2])*1000 + \
                 int(moment[14:14+3])

    candle_start = (moment_mil / candle_width) * candle_width 
    hh = candle_start/1000/60/60
    mm = (candle_start - hh*60*60*1000)/1000/60
    ss = (candle_start - hh*60*60*1000 - mm*60*1000)/1000
    fff = (candle_start - hh*60*60*1000 - mm*60*1000 - ss*1000)
    candle_moment = int('{}{:02g}{:02g}{:02g}{:03g}'.format(yyyymmdd, hh, mm, ss, fff))

    candle_start_out = 1e9*yyyymmdd + candle_start

    key_out = (symbol, candle_start_out, candle_width)
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


def filter_out(x):
    moment = x[1]
    yyyymmdd = moment // 1e9
    hhmm_mil = moment - yyyymmdd*1e9

    # rotate out
    if yyyymmdd < date_start_begin or yyyymmdd >= date_start_end:
        return False
    if hhmm_mil < candle_start_begin_mil or hhmm_mil >= candle_start_end_mil:
        return False
    return True

def map_pairs(x):
    s1, m1, w1, p1, s2, m2, w2, p2, sh = x
    key = PairS(s1, s2, w1, sh)
    value = PairP(m1, p1, p2)

    return key, value


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
    if xdiff2 == 0 or ydiff2 == 0:
        return None

    return diffprod / math.sqrt(xdiff2 *  ydiff2)

def map_diff(x):
    key, val = x
    if key.s1 >= key.s2:
        return key, None
    val = list(val)
    val.sort(key=itemgetter(0))
    diffs1 = tuple( (val[i+1][1]-val[i][1])/val[i][1] for i in xrange(len(val)-1))
    diffs2 = tuple( (val[i+1][2]-val[i][2])/val[i][2] for i in xrange(len(val)-1))

    if len(diffs1) <= 1:
        res = (None, None)
    else:
        res = (pearson_def(diffs1, diffs2), len(diffs1))
        #res = float(len(diffs1))
    return key, res






if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--securities', type=str, default="SVH1|EDH1")
    parser.add_argument('--date_from', type=int, default=20110111)
    parser.add_argument('--date_to', type=int, default=20110112)
    parser.add_argument('--time_from', type=int, default=1000)
    parser.add_argument('--time_to', type=int, default=1800)
    parser.add_argument('--widths', type=list_str, default="1,5,10")
    parser.add_argument('--shifts', type=list_str, default="0,1,2,3,4,5")
    parser.add_argument('--num_reducers', type=int, default=1)
    parser.add_argument('input', type=str)
    parser.add_argument('output', type=str)
    args = parser.parse_args()

    pattern = re.compile(args.securities)
    date_start_begin = int(args.date_from)
    date_start_end = int(args.date_to)
    candle_start_begin = args.time_from
    candle_start_end = args.time_to
    candle_widths = [i*1000 for i in args.widths]
    shifts = args.shifts
    num_reducers = args.num_reducers
    fi = args.input
    fo = args.output

    moment = "0"*8+str(candle_start_begin)+"0"*5
    candle_start_begin_mil = int(moment[8:8+2])*60*60*1000 + \
                             int(moment[10:10+2])*60*1000 + \
                             int(moment[12:12+2])*1000 + \
                             int(moment[14:14+3])

    moment = "0"*8+str(candle_start_end)+"0"*5
    candle_start_end_mil = int(moment[8:8+2])*60*60*1000 + \
                           int(moment[10:10+2])*60*1000 + \
                           int(moment[12:12+2])*1000 + \
                           int(moment[14:14+3])

    spark = SparkSession.builder\
        .appName("PythonCandle")\
        .config("spark.default.parallelism", num_reducers)\
        .getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')
    #sql_context = pyspark.sql.SQLContext(spark.sparkContext)

    text = spark.read.text(fi)
    header = text.first()
    field_map = {v: k for k, v in enumerate(header.value[1:].split(','))}
    data = text.rdd.map(parse_csv).filter(lambda x: x[0] is not None)

    w_df = spark.createDataFrame(candle_widths, T.IntegerType())
    data = data.cartesian(w_df.rdd)
    data = data.map(lambda x:  (x[0][0], x[0][1], x[0][2], x[0][3], x[1].value))

    data = data.map(map_f).reduceByKey(reduce_f)
    data = data.flatMap(lambda x:  ((x[0][0], x[0][1], x[0][2], x[1].price), ))
    data = data.toDF(["SYMBOL", "MOMENT", "WIDTH", "CLOSE_PRICE"])
#     data.show()



    data1 = data.toDF("SYMBOL1", "MOMENT1", "WIDTH1", "CLOSE_PRICE1")
    data2 = data.toDF("SYMBOL2", "MOMENT2", "WIDTH2", "CLOSE_PRICE2")

    pairs = []
    for s in shifts:
        if s == 0:
            pairs += (s, )
        else:
            pairs += (s, -s)

    s_df = spark.createDataFrame(pairs, T.IntegerType())
#     s_df.show()
    data2 = data2.crossJoin(s_df)
#     data2.show()

    data2 = data2.withColumn('MOMENT2', F.col("MOMENT2") + F.col("WIDTH2")*F.col("value"))

#     print("data1")
#     data1.show()
#     print("data2")
#     data2.show()

    #print(data1.first())
    data1 = data1.rdd.filter(filter_out).toDF()
    data2 = data2.rdd.filter(filter_out).toDF()


    data = data1.join(
        data2, 
        (F.col("WIDTH1") == F.col("WIDTH2")) & (F.col("MOMENT1") == F.col("MOMENT2")),
        'inner'
    )
    data = data.filter(data.SYMBOL1 < data.SYMBOL2)
#     data.show()


    data = data.sort("MOMENT1")
#     data.show()


    data = data.rdd.map(map_pairs)
    data = data.groupByKey().map(map_diff)
    #data = data.flatMap(lambda x: ( (x[0].s1, x[0].s2, x[0].width, x[0].shift, x[1][0], x[1][1]), ))
    data = data.flatMap(lambda x: ( (x[0].s1, x[0].s2, x[0].width, x[0].shift, x[1][0]),) ) #, x[1][1]), ) )
    #res = res.toDF(["sym1", "sym2", "p_corr"]).na.drop()
    schema = T.StructType([
        T.StructField("sec1", T.StringType(), True),
        T.StructField("sec2", T.StringType(), True),
        T.StructField("width", T.IntegerType(), True),
        T.StructField("shift", T.IntegerType(), True),
        T.StructField("p_corr", T.DoubleType(), True),
        #T.StructField("len", T.IntegerType(), True),
        ])
    res = spark.createDataFrame(data, schema=schema).na.drop()

    res = res.withColumn('abs_corr', F.abs(res.p_corr))
    res = res.withColumn('shift', F.abs(res.shift))
    res = res.withColumn('width', F.round(F.abs(res.width)/1e3).cast("int"))
    res = res.sort(['sec1', 'sec2', 'width', 'shift', 'abs_corr'], ascending=[1, 1, 0, 1, 1])
    res = res.drop('abs_corr')
    res = res.withColumn('corr', res.p_corr)
    res = res.drop('p_corr')

    #res = res.select('sym1', 'sym2', 'width', 'shift', 'p_corr')


    res.show()
    res.rdd.coalesce(1).map(to_out).saveAsTextFile(fo)
    res.coalesce(1).write.parquet("parquet_result")

    spark.stop()
