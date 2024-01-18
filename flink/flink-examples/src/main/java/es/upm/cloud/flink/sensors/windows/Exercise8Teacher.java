package es.upm.cloud.flink.sensors.windows;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author Ainhoa Azqueta Alzúaz (aazqueta@fi.upm.es)
 * @organization Universidad Politécnica de Madrid
 * @laboratory Laboratorio de Sistemas Distributidos (LSD)
 * @date 18/11/23
 **/
public class Exercise8Teacher {
    public static void main (String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> text;
        text = env.readTextFile(params.get("input"));

        SingleOutputStreamOperator<Tuple3<Long, String, Double>> mapStream = text.
                map(new MapFunction<String, Tuple3<Long, String, Double>>() {
                    public Tuple3<Long, String, Double> map(String in) throws Exception{
                        String[] fieldArray = in.split(",");
                        Tuple3<Long,String,Double> out = new Tuple3(Long.parseLong(fieldArray[0]),
                                fieldArray[1], Double.parseDouble(fieldArray[2]));
                        Thread.sleep(500); //To simulate events coming each 0.5 seconds
                        return out;
                    }
                });


        KeyedStream<Tuple3<Long, String, Double>, String> keyedStream = mapStream.keyBy(new KeySelector<Tuple3<Long, String, Double>, String>() {
            @Override
            public String getKey(Tuple3<Long, String, Double> longStringDoubleTuple3) throws Exception {
                return longStringDoubleTuple3.f1;
            }
        });

        SingleOutputStreamOperator<Tuple3<Long, String, Double>> output = keyedStream.
                window(SlidingProcessingTimeWindows.of(Time.seconds(3), Time.seconds(1))).aggregate(new MyAggregateFunction());

        if (params.has("output")) {
            output.writeAsCsv(params.get("output"), FileSystem.WriteMode.OVERWRITE);
        }
        else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            text.print();
        }

        env.execute("Exercise8");
    }

    private static class MyAggregateFunction implements AggregateFunction<Tuple3<Long, String, Double>, Tuple4<Long, String, Double, Integer>, Tuple3<Long, String, Double>> {


        @Override
        public Tuple4<Long, String, Double, Integer> createAccumulator() {
            return new Tuple4<>(0L,"", 0.0, 0);
        }

        @Override
        public Tuple4<Long, String, Double, Integer> add(Tuple3<Long, String, Double> value, Tuple4<Long, String, Double, Integer> acc) {
            return new Tuple4<>(value.f0, value.f1, acc.f2+value.f2, acc.f3+1);
        }

        @Override
        public Tuple3<Long, String, Double> getResult(Tuple4<Long, String, Double, Integer> acc) {
            return new Tuple3<>(acc.f0, acc.f1, acc.f2/acc.f3) ;
        }

        @Override
        public Tuple4<Long, String, Double, Integer> merge(Tuple4<Long, String, Double, Integer> a, Tuple4<Long, String, Double, Integer> b) {
            return new Tuple4<>(a.f0, a.f1, a.f2+b.f2, a.f3+b.f3);
        }
    }
}