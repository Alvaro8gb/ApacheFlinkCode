package es.upm.cloud.flink.sensors.windows;

import es.upm.cloud.flink.sensors.MyMapFunction;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class Exercise7 {

    public static class CustomWatermarkStrategy implements WatermarkStrategy<Tuple3<Long, String, Double>> {

        @Override
        public WatermarkGenerator<Tuple3<Long, String, Double>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new BoundedOutOfOrdernessWatermarks<>(Duration.ofSeconds(0));
        }

        @Override
        public SerializableTimestampAssigner<Tuple3<Long, String, Double>> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return (event, timestamp) -> event.f0 * 1000;
        }

    }

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> text;

        text = env.readTextFile(params.get("input"));

        SingleOutputStreamOperator<Tuple3<Long, String, Double>> mapOut = text.map(new MyMapFunction());

        DataStream<Tuple3<Long, String, Double>> outStream = mapOut
                .assignTimestampsAndWatermarks(new CustomWatermarkStrategy())
                .keyBy(1) // Key by the sensor name
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .min(2);


        if (params.has("output")) {
            outStream.writeAsCsv(params.get("output"), FileSystem.WriteMode.OVERWRITE);

        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            text.print();
        }

        env.execute("Ex7");
    }
}