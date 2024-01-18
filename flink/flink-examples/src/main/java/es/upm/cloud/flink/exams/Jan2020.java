package es.upm.cloud.flink.exams;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/*

A supermarket is processing the purchases of its customers as they go through the cashier. Each time
an item is scanned, an event with the product information is produced with the following information:
o Timestamp(Long), ProductId(Int), CustomerId(Int)
being the Timestamp, the date and time of the purchase in milliseconds (remains the same
during all the purchase), ProductId an integer that defines de product uniquely, CustomerId a
unique identifier of the customer and quantity the amount of units that this user has bought of
this product.
The supermarket is interested in knowing the customers that buy more than 10 units of an item
in a single purchase. This information will be stored in a file with the format:
Timestamp(Long), ProductId(Int), CustomerId (Int), quantity (Int). Being Timestamp(Long)
the time of the purchase milliseconds, quantity (Int) the total number of products purchased. A
purchase is considered

 */
public class Jan2020 {

    public static class SumProduct implements WindowFunction<Tuple3<Long, Integer, Integer>, Tuple4<Long, Integer, Integer, Integer>, Tuple,  TimeWindow> {

        @Override
        public void apply(Tuple key, TimeWindow timeWindow, Iterable<Tuple3<Long, Integer, Integer>> in,
                          Collector<Tuple4<Long, Integer, Integer, Integer>> out) throws Exception {
            Iterator<Tuple3<Long, Integer, Integer>> iterator = in.iterator();
            Tuple3<Long, Integer, Integer> first = iterator.next();
            Long ts = 0L;
            Integer product = 0;
            Integer customer = 0;
            Integer quantity = 0;

            if(first!=null){
                ts = first.f0;
                product = first.f1;
                customer = first.f2;
                quantity = 1;
            }
            while(iterator.hasNext()){
                Tuple3<Long, Integer, Integer> next = iterator.next();
                quantity += 1;
            }

            out.collect(new Tuple4<Long, Integer, Integer, Integer> (ts, product, customer, quantity));
        }
    }

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);// set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> text = env.readTextFile(params.get("input"));

        env.getConfig().setGlobalJobParameters(params);
        SingleOutputStreamOperator<Tuple3<Long, Integer, Integer>> mapStream = text.
                map(new MapFunction<String,Tuple3<Long, Integer, Integer>>() {
                    public Tuple3<Long, Integer, Integer> map(String in) throws Exception{
                        String[] fieldArray = in.split(",");
                        Tuple3<Long, Integer, Integer> out = new Tuple3(Long.parseLong(fieldArray[0]),
                                Integer.parseInt(fieldArray[1]), Integer.parseInt(fieldArray[2]));
                        return out;
                    }
                });


        KeyedStream<Tuple3<Long, Integer, Integer>, Tuple> keyedStream = mapStream.keyBy(0,1,2);
        SingleOutputStreamOperator<Tuple4<Long, Integer, Integer, Integer>> sumProducts = keyedStream
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
                .apply(new SumProduct());

        SingleOutputStreamOperator<Tuple4<Long, Integer, Integer, Integer>> alarm =
                sumProducts.filter(new FilterFunction<Tuple4<Long, Integer, Integer, Integer>>() {
                    public boolean filter(Tuple4<Long, Integer, Integer, Integer> in) throws Exception {
                        return in.f3 >= 10;
                    }
                });


        if (params.has("output")) {
            alarm.writeAsCsv(params.get("output"));
        }

        env.execute("CustomerPurchase");
    }
}



