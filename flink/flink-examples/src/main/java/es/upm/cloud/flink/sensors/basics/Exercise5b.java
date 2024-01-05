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
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class Exercise5b {

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args); // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(); // get input data
        DataStreamSource<String> text;

        text = env.readTextFile(params.get("input"));
        
        SingleOutputStreamOperator<Tuple3<Long, String, Double>> mapOut = text.map(new MyMapFunction());

        KeyedStream<Tuple3<Long, String, Double>, Tuple> keyedStream = mapOut.keyBy(1); // Group by sensor name

        SingleOutputStreamOperator<Tuple3<Long, String, Double>> outMin = keyedStream.min(2);
        SingleOutputStreamOperator<Tuple3<Long, String, Double>> outMax = keyedStream.max(2);
        SingleOutputStreamOperator<Tuple3<Long, String, Double>> outSum = keyedStream.sum(2);

        // emit result
        if (params.has("output")) {
            outSum.writeAsCsv(params.get("output").replace(".csv", "-sum.csv"));
            outMax.writeAsCsv(params.get("output").replace(".csv", "-max.csv"));
            outMin.writeAsCsv(params.get("output").replace(".csv", "-min.csv"));


        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            text.print();
        }

        env.execute("SourceSink");
    }
}