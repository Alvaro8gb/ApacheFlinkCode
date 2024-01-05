package es.upm.cloud.flink.sensors;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class MyMapFunction implements MapFunction<String, Tuple3<Long, String, Double>>{

    @Override
    public Tuple3<Long, String, Double> map(String in) throws Exception {
        // TimeStamp (Event Time) - ID sensor - Temperature
        String[] fieldArray = in.split(",");
        return new Tuple3<>(Long.parseLong(fieldArray[0]), fieldArray[1], Double.parseDouble(fieldArray[2]));
    }
}

