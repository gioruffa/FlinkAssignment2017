package master2017.flink.detectors;

import master2017.flink.events.CarEvent;
import master2017.flink.functions.filters.HigherSpeedFilterFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;


public class SpeedLimitDetector extends Detector {
    Integer speedLimitThreshold;


    public SpeedLimitDetector(
            String outputFolder,
            SingleOutputStreamOperator<CarEvent> carEventStream,
            Integer speedLimitThreshold
    )
    {
        super(outputFolder, carEventStream,"speedfines.csv");
        this.speedLimitThreshold = speedLimitThreshold;
    }


    public Integer getSpeedLimitThreshold() {
        return speedLimitThreshold;
    }

    public void setSpeedLimitThreshold(Integer speedLimitThreshold) {
        this.speedLimitThreshold = speedLimitThreshold;
    }


    @Override
    public void processCarEventStream() {
        this.getCarEventStream().filter(
                new HigherSpeedFilterFunction(speedLimitThreshold)
        ).map(new MapFunction<CarEvent, Tuple6<Long, String, String, Integer, Integer, Integer> >() {
            @Override
            public Tuple6<Long, String, String, Integer, Integer, Integer> map(CarEvent carEvent) throws Exception {
                return new Tuple6<>(
                        carEvent.getTimestamp(),
                        carEvent.getVehicleID(),
                        carEvent.getHighwayID(),
                        carEvent.getSegment(),
                        carEvent.getWestbound() ? 1 : 0,
                        carEvent.getSpeedModule()
                );
            }
        }).writeAsCsv(getOutputCSVFilePath().toString());
    }



}
