package master2017.flink;

import com.sun.org.apache.xpath.internal.operations.Bool;
import master2017.flink.detectors.SpeedLimitDetector;
import master2017.flink.events.CarEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


import java.io.File;

public class VehicleTelematics {
    static String inputFilePath;
    static String outputDirectoryPath;
    static Logger logger = LoggerFactory.getLogger(VehicleTelematics.class);

    static public void main(String  [] args)
    {
        if (!checkArgs(args)) {
            return;
        }

        //TODO: come back to store only string not files
        inputFilePath = args[0];
        outputDirectoryPath = args[1];

        final StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        streamEnv.setParallelism(1);


        DataStreamSource<String> fileStreamSource = streamEnv.readTextFile(inputFilePath);

        SingleOutputStreamOperator<CarEvent> carEventStream = fileStreamSource.flatMap(new FlatMapFunction<String, CarEvent>() {
            @Override
            public void flatMap(String s, Collector<CarEvent> collector) throws Exception {
                try {
                    collector.collect(CarEvent.fromString(s));
                } catch (Exception ex) {
                    //in case of malformed lines
                    //TODO: make an exception custom class
                    ex.printStackTrace();
                }
            }
        }).assignTimestampsAndWatermarks(
                new AscendingTimestampExtractor<CarEvent>() {
                    @Override
                    public long extractAscendingTimestamp(CarEvent carEvent) {
                        return carEvent.getTimestamp() * 1000;
                    }
                }
        );

        //check if it is working
//        carEventStream.map(new MapFunction<CarEvent, CarEvent>() {
//            @Override
//            public CarEvent map(CarEvent carEvent) throws Exception {
//                System.out.println(carEvent.toString());
//                return carEvent;
//            }
//        });

        /*
         * We have decided to parallelise with the finest grain possible.
         * So we are basically following a single car on an highway on a single direction
         */
        KeyedStream<CarEvent, Tuple3<String, String, Boolean>> carEventKeyedStream = carEventStream.keyBy(new KeySelector<CarEvent, Tuple3<String, String, Boolean>>() {
            @Override
            public Tuple3<String, String, Boolean> getKey(CarEvent carEvent) throws Exception {
                return new Tuple3<>(
                        carEvent.getVehicleID(),
                        carEvent.getHighwayID(),
                        carEvent.getWestbound()
                );
            }
        });

        SpeedLimitDetector speedLimitDetector = new SpeedLimitDetector(
            outputDirectoryPath,
            carEventKeyedStream,
            90
        );

        speedLimitDetector.processCarEventKeyedStream();


        try {
            streamEnv.execute();
        } catch (Exception e) {
            logger.error("Execption: " , e);
        }


    }

    static boolean checkArgs(String [] args)
    {
        if (args.length != 2)
        {
            System.err.println("Not enough arguments provided");
            return false;
        }
        if (!(new File(args[0]).exists()))
        {
            System.err.println("Input file does not exists");
            return false;
        }
        if (!(new File(args[1])).isDirectory())
        {
            System.err.println("Second argument should be a directory");
            return false;
        }
        return true;
    }

}
