package master2017.flink;

import master2017.flink.detectors.AccidentDetector;
import master2017.flink.detectors.AverageSpeedLimitDetector;
import master2017.flink.detectors.SpeedLimitDetector;
import master2017.flink.events.CarEvent;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class VehicleTelematics {
    static String inputFilePath;
    static String outputDirectoryPath;
    static Logger logger = LoggerFactory.getLogger(VehicleTelematics.class);

    static public void main(String[] args) {
        if (!checkArgs(args)) {
            return;
        }

        inputFilePath = args[0];
        outputDirectoryPath = args[1];

        final StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        streamEnv.setParallelism(1); //for debugging
        streamEnv.setParallelism(10);

        DataStreamSource<String> fileStreamSource = streamEnv.readTextFile(inputFilePath).setParallelism(1);

        SingleOutputStreamOperator<CarEvent> carEventStream = fileStreamSource.flatMap(new FlatMapFunction<String, CarEvent>() {
            @Override
            public void flatMap(String s, Collector<CarEvent> collector) throws Exception {
                try {
                    collector.collect(CarEvent.fromString(s));
                } catch (Exception ex) {
                    //in case of malformed lines
                    ex.printStackTrace();
                }
            }
        }).setParallelism(1) //parallelism needed for the count windows, otherwise they are not in order
        ;

        SpeedLimitDetector speedLimitDetector = new SpeedLimitDetector(
                outputDirectoryPath,
                carEventStream,
                90
        );

        AverageSpeedLimitDetector averageSpeedLimitDetector = new AverageSpeedLimitDetector(
                outputDirectoryPath,
                carEventStream,
                60,
                52,
                56
        );

        AccidentDetector accidentDetector = new AccidentDetector(
                outputDirectoryPath,
                carEventStream
        );

        speedLimitDetector.processCarEventStream();
        averageSpeedLimitDetector.processCarEventStream();
        accidentDetector.processCarEventStream();


        try {
            streamEnv.execute();
        } catch (Exception e) {
            logger.error("Execption: ", e);
        }


    }

    static boolean checkArgs(String[] args) {
        if (args.length != 2) {
            System.err.println("Not enough arguments provided");
            return false;
        }
        if (!(new File(args[0]).exists())) {
            System.err.println("Input file does not exists");
            return false;
        }
        if (!(new File(args[1])).isDirectory()) {
            System.err.println("Second argument should be a directory");
            return false;
        }
        return true;
    }

}
