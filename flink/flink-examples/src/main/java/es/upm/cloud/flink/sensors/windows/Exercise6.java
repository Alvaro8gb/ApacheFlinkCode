package es.upm.cloud.flink.sensors.windows;

import es.upm.cloud.flink.sensors.MyMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;

public class Exercise6 {

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args); // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(); // get input data
        DataStreamSource<String> text;

        // read the text file from given input path
        text = env.readTextFile(params.get("input"));

        //map to transform 1 event to 1 event (a tuple from csv line)
        SingleOutputStreamOperator<Tuple3<Long, String, Double>> mapOut = text.map(new MyMapFunction());

        DataStream<Tuple3<Long, String, Double>> outStream = mapOut
                .keyBy(1) // Key by the sensor name
                .window(GlobalWindows.create()) // Use global windows
                .trigger(CountTrigger.of(3)) // Trigger the window when 3 elements are received
                .sum(2); // Sum the temperatures within the window


        //creates a global window for the entire stream, and it calculates the sum of temperatures for all sensor names combined every three events.
        // emit result
        if (params.has("output")) {
            outStream.writeAsCsv(params.get("output"), FileSystem.WriteMode.OVERWRITE);

        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            text.print();
        }
// execute program
        env.execute("SourceSink");
    }
}