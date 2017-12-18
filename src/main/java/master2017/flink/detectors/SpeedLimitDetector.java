package master2017.flink.detectors;

import master2017.flink.events.CarEvent;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.datastream.KeyedStream;

import java.nio.file.Path;
import java.nio.file.Paths;

public class SpeedLimitDetector extends Detector {
    Integer speedLimitThreshold;

    static  FilterFunction<CarEvent> higherSpeedFilterFunction = new FilterFunction<CarEvent>() {
        @Override
        public boolean filter(CarEvent carEvent) throws Exception {
            return carEvent.getSpeedModule() > 90;// SpeedLimitDetector.this.getSpeedLimitThreshold();
        }
    };

    public SpeedLimitDetector(
            String outputFolder,
            KeyedStream<CarEvent, Tuple3<String, String, Boolean>> carEventKeyedStream,
            Integer speedLimitThreshold
    )
    {
        super(outputFolder, carEventKeyedStream,"speedfines.csv");
        this.speedLimitThreshold = speedLimitThreshold;
    }

    public SpeedLimitDetector(String outputFolder, KeyedStream<CarEvent, Tuple3<String, String, Boolean>> carEventKeyedStream, String outputFileName, Integer speedLimitThreshold, FilterFunction<CarEvent> higherSpeedFilterFunction) {
        super(outputFolder, carEventKeyedStream, outputFileName);
        this.speedLimitThreshold = speedLimitThreshold;
        this.higherSpeedFilterFunction = higherSpeedFilterFunction;
    }

    public Integer getSpeedLimitThreshold() {
        return speedLimitThreshold;
    }

    public void setSpeedLimitThreshold(Integer speedLimitThreshold) {
        this.speedLimitThreshold = speedLimitThreshold;
    }


    @Override
    public void processCarEventKeyedStream() {
        this.getCarEventKeyedStream().filter(
                higherSpeedFilterFunction
        ).map(new MapFunction<CarEvent, Tuple6<Long, String, String, Integer, Boolean, Integer> >() {
            @Override
            public Tuple6<Long, String, String, Integer, Boolean, Integer> map(CarEvent carEvent) throws Exception {
                return new Tuple6<>(
                        carEvent.getTimestamp(),
                        carEvent.getVehicleID(),
                        carEvent.getHighwayID(),
                        carEvent.getSegment(),
                        carEvent.getWestbound(),
                        carEvent.getSpeedModule()
                );
            }
        }).writeAsCsv("/tmp/asd.csv");
    }



}
