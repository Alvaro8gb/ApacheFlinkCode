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

package es.upm.cloud.flink.sensors.basics;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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
public class Exercise2 {

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

        SingleOutputStreamOperator<Tuple3<Long, String, Double>> filterOut = mapOut.filter(new FilterFunction<Tuple3<Long, String, Double>>() {
                    /*

                    In Apache Flink, the Tuple classes use a naming convention where the fields are named f0, f1, f2,
                    and so on. The f stands for "field," and the number represents the position or index of the field within the tuple.
                     */

            @Override
            public boolean filter(Tuple3<Long, String, Double> in) throws Exception {
                return in.f1.equals("sensor1");
            }
        });

        // emit result
        if (params.has("output")) {
            filterOut.writeAsCsv(params.get("output"), FileSystem.WriteMode.OVERWRITE);
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            filterOut.print();
        }
// execute program
        env.execute("Ex2");
    }
}