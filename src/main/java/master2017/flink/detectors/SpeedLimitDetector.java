package master2017.flink.detectors;

import master2017.flink.events.CarEvent;
import master2017.flink.functions.filters.HigherSpeedFilterFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.datastream.KeyedStream;


public class SpeedLimitDetector extends Detector {
    Integer speedLimitThreshold;


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
                new HigherSpeedFilterFunction(speedLimitThreshold)
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
        }).writeAsCsv(getOutputCSVFilePath().toString());
    }



}
