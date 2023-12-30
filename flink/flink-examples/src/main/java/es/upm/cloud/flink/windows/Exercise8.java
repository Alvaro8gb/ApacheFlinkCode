/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package es.upm.cloud.flink.windows;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */


public class Exercise8 {

    // Custom AggregateFunction to calculate the average

        public static class AverageAggregateFunction implements AggregateFunction< Tuple3<Long, String, Double>, Tuple3<String, Long, Double> , Tuple2<String, Double>>{

            @Override
            public Tuple3<String, Long, Double> createAccumulator() {
                return new Tuple3<>("", 0L, 0.0);
            }

            @Override
            public Tuple3<String, Long, Double> add(Tuple3<Long, String, Double> elem, Tuple3<String, Long, Double> acc) {
                return new Tuple3<>(elem.f1, acc.f1 + 1, acc.f2 + elem.f2);
            }

            @Override
            public Tuple2<String, Double>getResult(Tuple3<String, Long, Double> acc) {
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
                long currentTimestamp = ctx.getCurrentWatermark();
                long nextFireTimestamp = currentTimestamp + 3000; // 3 seconds

                if (timestamp >= nextFireTimestamp) {
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

        public static class CustomWatermarkExtractor extends AscendingTimestampExtractor<Tuple3<Long, String, Double>> {

            @Override
            public long extractAscendingTimestamp(Tuple3<Long, String, Double> elem) {
                return elem.f0*1000;
            }
        }
        public static void main(String[] args) throws Exception {
            final ParameterTool params = ParameterTool.fromArgs(args); // set up the execution environment
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(); // get input data
            DataStreamSource<String> text;

            // read the text file from given input path
            text = env.readTextFile(params.get("input"));

            //map to transform 1 event to 1 event (a tuple from csv line)
            SingleOutputStreamOperator<Tuple3<Long, String, Double>> mapOut = text.map(
                    new MapFunction<String, Tuple3<Long, String, Double>>() {
                        @Override
                        public Tuple3<Long, String, Double> map(String in) throws Exception {
                            String[] fieldArray = in.split(",");
                            Tuple3<Long, String, Double> out = new Tuple3(Long.parseLong(fieldArray[0]), fieldArray[1], Double.parseDouble(fieldArray[2]));
                            return out;
                        }
                    });

            DataStream<Tuple2<String, Double>> outStream = mapOut
                    .assignTimestampsAndWatermarks(new CustomWatermarkExtractor())
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
            env.execute("SourceSink");
        }
}