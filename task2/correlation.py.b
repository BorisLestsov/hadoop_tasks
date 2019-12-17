
from __future__ import print_function

import sys
from collections import namedtuple
import re
import math
from operator import itemgetter

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
PairS = namedtuple('PairS', ["s1", "s2", "width"])
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


def map_f(line):
    symbol, moment, moment_id, price, candle_width, shift = line

    hhmm = int(moment[8:8+4])
    yyyymmdd = int(moment[0:0+8])


    # rotate out
    if yyyymmdd < date_start_begin or yyyymmdd >= date_start_end:
        return None, None

    if hhmm < candle_start_begin or hhmm >= candle_start_end:
        return None, None

    moment_long = int(moment)

    moment_mil = int(moment[8:8+2])*60*60*1000 + \
                 int(moment[10:10+2])*60*1000 + \
                 int(moment[12:12+2])*1000 + \
                 int(moment[14:14+3])

    candle_start = (moment_mil / candle_width) * candle_width + candle_width*shift
    hh = candle_start/1000/60/60
    mm = (candle_start - hh*60*60*1000)/1000/60
    ss = (candle_start - hh*60*60*1000 - mm*60*1000)/1000
    fff = (candle_start - hh*60*60*1000 - mm*60*1000 - ss*1000)
    candle_moment = int('{}{:02g}{:02g}{:02g}{:03g}'.format(yyyymmdd, hh, mm, ss, fff))

    key_out = (symbol, candle_moment, candle_width, shift)
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
    moment, width, shift, s1, p1, s2, p2 = x
    key = PairS(s1, s2, width)
    value = PairP(moment, p1, p2)

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

    return diffprod / math.sqrt(xdiff2 * ydiff2)

def map_diff(x):
    key, val = x
    if key.s1 >= key.s2:
        return key, None
    val = list(val)
    #val.sort(key=itemgetter(0))
    diffs1 = tuple( (val[i+1][1]-val[i][1])/val[i][1] for i in xrange(len(val)-1))
    diffs2 = tuple( (val[i+1][2]-val[i][2])/val[i][2] for i in xrange(len(val)-1))

    if len(diffs1) <= 1:
        res = None
    else:
        res = pearson_def(diffs1, diffs2)
        #res = float(len(diffs1))
    return key, res






if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--securities', type=str, default=".*")
    parser.add_argument('--date_from', type=int, default=19000101)
    parser.add_argument('--date_to', type=int, default=20200101)
    parser.add_argument('--time_from', type=int, default=1000)
    parser.add_argument('--time_to', type=int, default=2000)
    parser.add_argument('--widths', type=list_str, default="30")
    parser.add_argument('--shifts', type=list_str, default="0")
    parser.add_argument('--num_reducers', type=int, default=1)
    parser.add_argument('input', type=str)
    parser.add_argument('output', type=str)
    args = parser.parse_args()

    pattern = re.compile(args.securities)
    date_start_begin = args.date_from
    date_start_end = args.date_to
    candle_start_begin = args.time_from
    candle_start_end = args.time_to
    candle_widths = [i*1000 for i in args.widths]
    shifts = args.shifts
    num_reducers = args.num_reducers
    fi = args.input
    fo = args.output

    spark = SparkSession.builder\
        .appName("PythonCandle")\
        .getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')
    #sql_context = pyspark.sql.SQLContext(spark.sparkContext)

    text = spark.read.text(fi)
    header = text.first()
    field_map = {v: k for k, v in enumerate(header.value[1:].split(','))}
    df_start = text.rdd.map(parse_csv).filter(lambda x: x[0] is not None)

    params_schema = T.StructType([T.StructField("widths", T.IntegerType(), True), \
                                  T.StructField("shifts", T.IntegerType(), True)])
    params_list=[]
    for w in candle_widths:
        for s in shifts:
            if s != 0:
                params_list.append((w, s))
                params_list.append((w, -s))
            else:
                params_list.append((w, s))

    params_df = spark.createDataFrame(params_list,schema=params_schema) 
    counts = df_start.cartesian(params_df.rdd).map(lambda x: x[0]+x[1])
    #counts.toDF(["symbol", "moment", "moment_id", "price", "width", "shift"]).show()
    counts = counts.map(map_f).reduceByKey(reduce_f)
    #out_df = counts.flatMap(lambda x:  (x[0][0], x[0][1], x[1].price, x[1].close_time, x[1].close_id))
    out_df = counts.flatMap(lambda x:  ((x[0][0], x[0][1], x[0][2], x[0][3], x[1].price), ))
    out_df = out_df.toDF(["SYMBOL", "MOMENT", "WIDTH", "SHIFT", "CLOSE_PRICE"])
    out_df.show()

    if False:
        tmp_df = out_df.sort("MOMENT", ascending=True)
        tmp_df.rdd.map(lambda x: ",".join(map(str, x))).coalesce(1).saveAsTextFile("file.csv")
        tmp_df.collect()
        tmp_df.write.format("csv").save("out_dbg")
        # spark.stop()
        # sys.exit()

    pairs = out_df.join(out_df, ["MOMENT", "WIDTH", "SHIFT"], 'inner')
    pairs = pairs.sort("MOMENT")
    pairs.show()

    res = pairs.rdd.map(map_pairs).groupByKey().map(map_diff).flatMap(lambda x:  ((x[0].s1, x[0].s2, x[0].width) + x[1:], ))
    #res = res.toDF(["sym1", "sym2", "p_corr"]).na.drop()
    schema = T.StructType([
        T.StructField("sym1", T.StringType(), True),
        T.StructField("sym2", T.StringType(), True),
        T.StructField("width", T.IntegerType(), True),
        #T.StructField("shift", T.IntegerType(), True),
        T.StructField("p_corr", T.FloatType(), True),
        ])
    res = spark.createDataFrame(res, schema=schema).na.drop()

    res = res.withColumn('abs_corr', F.abs(res.p_corr))
    res = res.sort('abs_corr', ascending=False)
    res = res.drop('abs_corr')

    #res = res.select('sym1', 'sym2', 'width', 'shift', 'p_corr')


    res.show()
    res.rdd.map(lambda x: " ".join(map(str, x))).coalesce(1).saveAsTextFile(fo)
    res.write.parquet("parquet_result")

    spark.stop()
