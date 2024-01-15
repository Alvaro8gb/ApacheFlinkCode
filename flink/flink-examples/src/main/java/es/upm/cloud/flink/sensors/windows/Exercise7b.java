package es.upm.cloud.flink.sensors.windows;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class Exercise7b {

    public static class MyMapFunction implements MapFunction<String, Tuple3<Long, String, Double>> {

        @Override
        public Tuple3<Long, String, Double> map(String in) throws Exception {
            // TimeStamp (Event Time) - ID sensor - Temperature
            String[] fieldArray = in.split(",");
            Thread.sleep(500); // Sleep of processing time
            return new Tuple3<>(Long.parseLong(fieldArray[0]), fieldArray[1], Double.parseDouble(fieldArray[2]));
        }
    }

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> text;

        text = env.readTextFile(params.get("input"));

        SingleOutputStreamOperator<Tuple3<Long, String, Double>> mapOut = text.map(new MyMapFunction());

        DataStream<Tuple3<Long, String, Double>> outStream = mapOut
                .keyBy(1) // Key by the sensor name
                .window(TumblingProcessingTimeWindows.of(Time.seconds(3)))
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