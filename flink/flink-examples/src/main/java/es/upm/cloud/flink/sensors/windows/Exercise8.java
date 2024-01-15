package es.upm.cloud.flink.sensors.windows;

import es.upm.cloud.flink.sensors.MyMapFunction;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

import java.time.Duration;

public class Exercise8 {

    // Custom AggregateFunction to calculate the average

    public static class AverageAggregateFunction implements AggregateFunction<Tuple3<Long, String, Double>,
            Tuple3<String, Long, Double>, Tuple2<String, Double>> {

        @Override
        public Tuple3<String, Long, Double> createAccumulator() {
            return new Tuple3<>("", 0L, 0.0);
        }

        @Override
        public Tuple3<String, Long, Double> add(Tuple3<Long, String, Double> elem, Tuple3<String, Long, Double> acc) {
            return new Tuple3<>(elem.f1, acc.f1 + 1, acc.f2 + elem.f2);
        }

        @Override
        public Tuple2<String, Double> getResult(Tuple3<String, Long, Double> acc) {
            return new Tuple2<>(acc.f0, acc.f2 / acc.f1);
        }

        @Override
        public Tuple3<String, Long, Double> merge(Tuple3<String, Long, Double> accA, Tuple3<String, Long, Double> accB) {
            return new Tuple3<>(accA.f0, accA.f1 + accB.f1, accA.f2 + accB.f2);
        }
    }

    // No purging
    public static class CustomEventTimeTrigger extends Trigger<Tuple3<Long, String, Double>, GlobalWindow> {
        @Override
        public TriggerResult onElement(Tuple3<Long, String, Double> element, long timestamp, GlobalWindow window, TriggerContext ctx) {

            long nextFireTimestamp = timestamp + 3000; // 3 seconds in milliseconds

            if (nextFireTimestamp > ctx.getCurrentWatermark()) {
                ctx.registerEventTimeTimer(nextFireTimestamp);
            }

            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, GlobalWindow window, TriggerContext ctx) {
            return TriggerResult.FIRE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext ctx) {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(GlobalWindow window, TriggerContext ctx) {
            // Cleanup logic if needed
        }

    }

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

        //map to transform 1 event to 1 event (a tuple from csv line)
        SingleOutputStreamOperator<Tuple3<Long, String, Double>> mapOut = text.map(new MyMapFunction());

        DataStream<Tuple2<String, Double>> outStream = mapOut
                .assignTimestampsAndWatermarks(new CustomWatermarkStrategy())
                .keyBy(1)
                .window(GlobalWindows.create())
                .trigger(new CustomEventTimeTrigger())
                .aggregate(new AverageAggregateFunction());


        // emit result
        if (params.has("output")) {
            outStream.writeAsCsv(params.get("output"), FileSystem.WriteMode.OVERWRITE);

        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            text.print();
        }
        // execute program
        env.execute("Ex8");
    }
}