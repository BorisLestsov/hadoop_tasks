import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Writable;
 
import java.lang.System;
import java.io.DataOutput;
import java.io.DataInput;
import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import java.io.DataOutputStream;

import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.mapreduce.Mapper; 
import org.apache.hadoop.mapreduce.Reducer; 
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.util.StringTokenizer; 
import java.io.IOException; 



 
public class candle { 
    public static final Log log = LogFactory.getLog(candle.class);

    public static class CandleKey implements WritableComparable<CandleKey> {

        public String symbol;
        public long candle_start;
        public long candle_moment;

        public CandleKey() {}

        public CandleKey(String symbol, long candle_start, long candle_moment) {
            super();
            this.set(symbol, candle_start, candle_moment);
        }

        public void set(String symbol, long candle_start, long candle_moment) {
            this.symbol = (symbol == null) ? "" : symbol;
            this.candle_start = candle_start;
            this.candle_moment = candle_moment;
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 29 * hash + this.symbol.hashCode();
            hash = 29 * hash + (int) this.candle_start;
            return hash;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(symbol);
            out.writeLong(candle_start);
            out.writeLong(candle_moment);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            symbol = in.readUTF();
            candle_start = in.readLong();
            candle_moment = in.readLong();
        }

        @Override
        public int compareTo(CandleKey v) {
            int str_cmp_res = symbol.compareTo(v.symbol);
            if (str_cmp_res == 0) {
                return Long.compare(candle_start, v.candle_start);
            } else {
                return str_cmp_res;
            }
        }
    }


    public static class CandleValue implements Writable {

        public double min_price;
        public double max_price;
        public double open_price;
        public double close_price;
        public long open_time;
        public long close_time;
        public long open_id;
        public long close_id;

        public CandleValue() {}

        public CandleValue(double min_price, 
                           double max_price, 
                           double open_price, 
                           double close_price, 
                           long open_time, 
                           long close_time, 
                           long open_id, 
                           long close_id) {
            super();
            this.set(min_price, max_price, open_price, close_price, open_time, close_time, open_id, close_id);
        }

        public void set(double min_price, double max_price, double open_price, double close_price, long open_time, long close_time, long open_id, long close_id) {
            this.min_price = min_price;
            this.max_price = max_price;
            this.open_price = open_price;
            this.close_price = close_price;
            this.open_time = open_time;
            this.close_time = close_time;
            this.open_id = open_id;
            this.close_id = close_id;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeDouble(min_price);
            out.writeDouble(max_price);
            out.writeDouble(open_price);
            out.writeDouble(close_price);
            out.writeLong(open_time);
            out.writeLong(close_time);
            out.writeLong(open_id);
            out.writeLong(close_id);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            min_price = in.readDouble();
            max_price = in.readDouble();
            open_price = in.readDouble();
            close_price = in.readDouble();
            open_time = in.readLong();
            close_time = in.readLong();
            open_id = in.readLong();
            close_id = in.readLong();
        }
    }

    public static class TransMapper extends Mapper<Object, Text, CandleKey, CandleValue> {

        public String regex_pattern = ".*";
        public long candle_width = 0;
        public long date_start_begin = 0;
        public long date_start_end = 0;
        public long candle_start_begin = 0;
        public long candle_start_end = 0;

        private CandleKey key_out = new CandleKey();
        private CandleValue value_out = new CandleValue();

        @Override
        public void setup(Context context) throws java.io.IOException,java.lang.InterruptedException {
            Configuration conf = context.getConfiguration();
            regex_pattern = conf.get("candle.securities", ".*");
            candle_width = conf.getLong("candle.width", 300000);
            date_start_begin = conf.getLong("candle.date.from", 19000101);
            date_start_end = conf.getLong("candle.date.to", 20200101);
            candle_start_begin = conf.getLong("candle.time.from", 1000);
            candle_start_end = conf.getLong("candle.time.to", 1800);
        }
        
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
 
            String line = value.toString();
            String[] line_split = line.split(",");
            
            // #SYMBOL,SYSTEM,MOMENT,ID_DEAL,PRICE_DEAL,VOLUME,OPEN_POS,DIRECTION
            
            String symbol = line_split[0];
            if (!symbol.matches(regex_pattern)) {
                return;
            }

            String moment = line_split[2];

            if (moment.length() != 17) {
                log.warn("LOL");
                log.warn(moment.length());
                return;
            }


            long hhmm = Long.parseLong(moment.substring(8, 8+4));
            long yyyymmdd = Long.parseLong(moment.substring(0, 0+8));


            if (yyyymmdd < date_start_begin || yyyymmdd >= date_start_end) {
                return;
            }
            if (hhmm < candle_start_begin || hhmm >= candle_start_end) {
                return;
            }

            long moment_long = Long.parseLong(line_split[2]);

            long moment_mil = Long.parseLong(moment.substring(8, 8+2))*60*60*1000 + 
                              Long.parseLong(moment.substring(10, 10+2))*60*1000 + 
                              Long.parseLong(moment.substring(12, 12+2))*1000 +
                              Long.parseLong(moment.substring(14, 14+3));
            long candle_start = (moment_mil / candle_width) * candle_width;
            long hh = candle_start/1000/60/60;
            long mm = (candle_start - hh*60*60*1000)/1000/60;
            long ss = (candle_start - hh*60*60*1000 - mm*60*1000)/1000;
            long fff = (candle_start - hh*60*60*1000 - mm*60*1000 - ss*1000);
            long candle_moment = Long.parseLong(
                                     yyyymmdd +
                                     String.format("%02d", hh) +
                                     String.format("%02d", mm) +
                                     String.format("%02d", ss) +
                                     String.format("%03d", fff)
                                 );

            key_out.set(symbol, candle_start, candle_moment);
            
            long moment_id = Long.parseLong(line_split[3]);
            double price = Double.parseDouble(line_split[4]);

            value_out.set(price, price, price, price, moment_long, moment_long, moment_id, moment_id);
            context.write(key_out, value_out);
        }
    }
    
    public static class CandleCombiner extends Reducer<CandleKey, CandleValue, CandleKey, CandleValue> { 
        
        private CandleValue value_out = new CandleValue(); 
 
        public void reduce(CandleKey key, Iterable<CandleValue> values, Context context) throws IOException, InterruptedException { 
            
            double min_price = Double.POSITIVE_INFINITY;
            double max_price = Double.NEGATIVE_INFINITY; 
            double open_price = -1.0f;
            double close_price = -1.0f;
            long open_time = Long.MAX_VALUE;
            long close_time = Long.MIN_VALUE;
            long open_id = Long.MAX_VALUE;
            long close_id = Long.MIN_VALUE;

            for (CandleValue val : values) { 
                min_price = Math.min(min_price, val.min_price);
                max_price = Math.max(max_price, val.max_price);

                if (val.open_time < open_time || (val.open_time == open_time && val.open_id < open_id)){
                    open_time = val.open_time;
                    open_price = val.open_price;
                    open_id = val.open_id;
                }

              
                if (val.close_time > close_time || (val.close_time == close_time && val.close_id > close_id)){
                    close_time = val.close_time;
                    close_price = val.close_price;
                    close_id = val.close_id;
                }
            } 

            value_out.set(min_price, max_price, open_price, close_price, open_time, close_time, -1, -1); 
            context.write(key, value_out); 
        } 
    }
 
    public static class CandleReducer extends Reducer<CandleKey, CandleValue, CandleKey, CandleValue> { 
        
        private CandleValue value_out = new CandleValue(); 
        private MultipleOutputs mos;
 
        @Override
        public void setup(Context context) throws java.io.IOException,java.lang.InterruptedException {
            mos = new MultipleOutputs(context);
        }
        @Override()
        public void cleanup(Context context) throws java.io.IOException, InterruptedException {
            mos.close();
        }


        public void reduce(CandleKey key, Iterable<CandleValue> values, Context context) throws IOException, InterruptedException { 
            //log.info("REDUCE " + key.symbol + " " + key.candle_start + " " + key.candle_moment + " " + key.hashCode());
            
            double min_price = Double.POSITIVE_INFINITY;
            double max_price = Double.NEGATIVE_INFINITY; 
            double open_price = -1.0f;
            double close_price = -1.0f;
            long open_time = Long.MAX_VALUE;
            long close_time = Long.MIN_VALUE;
            long open_id = Long.MAX_VALUE;
            long close_id = Long.MIN_VALUE;

            for (CandleValue val : values) { 
                min_price = Math.min(min_price, val.min_price);
                max_price = Math.max(max_price, val.max_price);

                if (val.open_time < open_time || (val.open_time == open_time && val.open_id < open_id)){
                    open_time = val.open_time;
                    open_price = val.open_price;
                    open_id = val.open_id;
                }

              
                if (val.close_time > close_time || (val.close_time == close_time && val.close_id > close_id)){
                    close_time = val.close_time;
                    close_price = val.close_price;
                    close_id = val.close_id;
                }
            } 

            value_out.set(min_price, max_price, open_price, close_price, open_time, close_time, -1, -1); 

            mos.write("textlol", key, value_out, key.symbol);
            //context.write(key, value_out); 
        } 
    }


    public static class CandleOutputFormat extends FileOutputFormat<CandleKey, CandleValue> {

        // both work, first is cooler

        /*
        public RecordWriter<CandleKey, CandleValue> getRecordWriter(TaskAttemptContext tac) throws IOException, InterruptedException {
            Path path = FileOutputFormat.getOutputPath(tac);
            String name = FileOutputFormat.getOutputName(tac);
            Path fullPath = new Path(path, name);

            FileSystem fs = path.getFileSystem(tac.getConfiguration());
            FSDataOutputStream fileOut;
            fileOut = fs.create(fullPath);

            return new CandleRecordWriter(fileOut);
        }
        */

        //*
        public RecordWriter<CandleKey, CandleValue> getRecordWriter(TaskAttemptContext tac) throws IOException, InterruptedException {
            Path fullPath = getDefaultWorkFile(tac, "");

            FileSystem fs = fullPath.getFileSystem(tac.getConfiguration());
            FSDataOutputStream fileOut;
            fileOut = fs.create(fullPath);

            return new CandleRecordWriter(fileOut);
        }
        //*/

    }


    public static class CandleRecordWriter extends RecordWriter<CandleKey, CandleValue> {

        private DataOutputStream out;
        private String fmt = "%.1f";

        public CandleRecordWriter(DataOutputStream stream) {
            this.out = stream;
        }

        public String prec_str(double v){
            return String.format(fmt, v);
        }

        public synchronized void write(CandleKey key, CandleValue value) throws IOException {
            //write out our key
            out.writeBytes(key.symbol + "," + key.candle_moment + ",");
            out.writeBytes(prec_str(value.open_price) + "," + 
                           prec_str(value.max_price) + "," + 
                           prec_str(value.min_price) + "," + 
                           prec_str(value.close_price)
                           );
                
            //out.writeBytes("\r\n");  
            out.writeBytes("\n");  
        }

        public synchronized void close(TaskAttemptContext context) throws IOException {
            out.close();
        }
    }


    public static void main(String[] args) throws IOException {

        long startTime = System.currentTimeMillis();

        Configuration conf = new Configuration();
        new GenericOptionsParser(conf, args);
        /*
        for (Map.Entry<String,String> val: conf){
            log.info("VAL");
            log.info(val);

        }
        */
        
        log.info("START_CANDLE");
        int num_args = args.length;

        Job job = Job.getInstance(conf, "CANDLE");
        job.setJarByClass(candle.class);

        job.setMapperClass(TransMapper.class);
        //job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapOutputKeyClass(CandleKey.class);
        job.setMapOutputValueClass(CandleValue.class);

        if (conf.getBoolean("candle.use_combiners", true)) {
            log.info("USE COMBINERS");
            job.setCombinerClass(CandleCombiner.class);
        } else {
            log.info("DO NOT USE COMBINERS");
        }

        //TotalOrderPartitioner.setPartitionFile(job.getConfiguration(),new Path(args[num_args-1], "part"));
        //job.setPartitionerClass(TotalOrderPartitioner.class);


        job.setReducerClass(CandleReducer.class);
        job.setNumReduceTasks(conf.getInt("candle.num.reducers", 1));
        log.info("NUM REDUCE TASKS");
        log.info(job.getNumReduceTasks());

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[num_args-2]));
        FileOutputFormat.setOutputPath(job, new Path(args[num_args-1]));
        MultipleOutputs.addNamedOutput(job, "textlol", CandleOutputFormat.class, CandleKey.class, CandleValue.class);
        //job.setOutputFormatClass(CandleOutputFormat.class);
        LazyOutputFormat.setOutputFormatClass(job, CandleOutputFormat.class);
        //job.setOutputFormatClass(TextOutputFormat.class);


        try 
        {
            int res = job.waitForCompletion(true) ? 0 : 1;
            long estimatedTime = System.currentTimeMillis() - startTime;
            System.out.println("TIME:" + estimatedTime);
            log.info("TIME:");
            log.info(estimatedTime);
            System.exit(res);
        } 
        catch(Exception e)
        {
            e.printStackTrace();
        }

    }

}

