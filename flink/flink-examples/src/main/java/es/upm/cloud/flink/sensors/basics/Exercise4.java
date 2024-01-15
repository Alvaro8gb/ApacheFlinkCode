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

import es.upm.cloud.flink.sensors.MyMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
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
public class Exercise4 {

    public static class MyReduce implements ReduceFunction<Tuple3<Long, String, Double>> {

        @Override
        public Tuple3<Long, String, Double> reduce(Tuple3<Long, String, Double> t1, Tuple3<Long, String, Double> t2) throws Exception {
            return new Tuple3<Long, String, Double>(t2.f0, t1.f1, t1.f2 + t2.f2);
        }
    }

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args); // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(); // get input data
        DataStreamSource<String> text;

        text = env.readTextFile(params.get("input"));

        SingleOutputStreamOperator<Tuple3<Long, String, Double>> mapOut = text.map(new MyMapFunction());

        KeyedStream<Tuple3<Long, String, Double>, Tuple> keyedStream = mapOut.keyBy(1);

        SingleOutputStreamOperator<Tuple3<Long, String, Double>> outReduce = keyedStream.reduce(new MyReduce());

        if (params.has("output")) {
            outReduce.writeAsCsv(params.get("output"));
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            text.print();
        }

        env.execute("Ex4");
    }
}