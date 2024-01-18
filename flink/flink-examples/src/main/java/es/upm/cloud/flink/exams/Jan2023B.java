package es.upm.cloud.flink.exams;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class Jan2023B {

    public static class MyMap implements MapFunction<String, Tuple3<Long, Long, Double>> {

        @Override
        public Tuple3<Long, Long, Double> map(String in) throws Exception {
            String[] fieldArray = in.split(",");
            Tuple3<Long, Long, Double> out = new Tuple3(Long.parseLong(fieldArray[0]), Long.parseLong(fieldArray[1]), Double.parseDouble(fieldArray[2]));
            return out;
        }
    }

    public static class MyReduce implements ReduceFunction<Tuple3<Long, Long, Double>> {
        @Override
        public Tuple3<Long, Long, Double> reduce(Tuple3<Long, Long, Double> r1, Tuple3<Long, Long, Double> r2) throws Exception {
            return new Tuple3<>(r1.f0, r1.f1, r1.f2 + r2.f2);
        }
    }
    public static class CustomWatermarkStrategy implements WatermarkStrategy<Tuple3<Long, Long, Double>> {

        @Override
        public WatermarkGenerator<Tuple3<Long, Long, Double>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new BoundedOutOfOrdernessWatermarks<>(Duration.ofSeconds(1));
        }

        @Override
        public SerializableTimestampAssigner<Tuple3<Long, Long, Double>> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return (event, timestamp) -> event.f0 * 1000;
        }

    }
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args); // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(); // get input data
        DataStreamSource<String> text;

        text = env.readTextFile(params.get("input"));

        SingleOutputStreamOperator<Tuple3<Long, Long, Double>> mapOut = text.map(new MyMap());

        SingleOutputStreamOperator<Tuple3<Long, Long, Double>> output = mapOut
                .assignTimestampsAndWatermarks(new CustomWatermarkStrategy())
                .keyBy(1)
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .reduce(new MyReduce());


        //creates a global window for the entire stream, and it calculates the sum of temperatures for all sensor names combined every three events.
        // emit result
        if (params.has("output")) {
            System.out.println("Writing in " + params.get("output"));
            output.writeAsCsv(params.get("output"), FileSystem.WriteMode.OVERWRITE);

        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            output.print();
        }

        env.execute("SourceSink");


    }
}
