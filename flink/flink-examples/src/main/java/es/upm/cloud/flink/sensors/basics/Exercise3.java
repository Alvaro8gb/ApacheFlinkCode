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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Exercise3 {

    public static class MyFlatMap implements FlatMapFunction<String, Tuple3<Long, String, Double>>{

        @Override
        public void flatMap(String in, Collector<Tuple3<Long, String, Double>> collector) throws Exception {
            String[] fieldArray = in.split(",");
            Tuple3<Long, String, Double> tuple = new Tuple3<>(Long.parseLong(fieldArray[0]), fieldArray[1], Double.parseDouble(fieldArray[2]));
            Double f_degrees = (tuple.f2 * 9 /12) + 32;
            collector.collect(tuple);
            collector.collect(new Tuple3<>(tuple.f0, tuple.f1, f_degrees));
        }
    }
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args); // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(); // get input data
        DataStreamSource<String> text;

        text = env.readTextFile(params.get("input"));

        //map to transform 1 event to 2 event
        SingleOutputStreamOperator<Tuple3<Long, String, Double>> flatMapOut = text.flatMap(new MyFlatMap());

        if (params.has("output")) {
            flatMapOut.writeAsCsv(params.get("output"));
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            text.print();
        }
        env.execute("Ex3");
    }
}