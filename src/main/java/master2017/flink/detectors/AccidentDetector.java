package master2017.flink.detectors;

import master2017.flink.KeySelectors.VidHighwayWestboundKeySelector;
import master2017.flink.events.CarEvent;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class AccidentDetector extends Detector {

    public AccidentDetector(
            String outputFolder,
            SingleOutputStreamOperator<CarEvent> carEventStream
    ) {
        super(outputFolder, carEventStream, "accidents.csv");
    }

    @Override
    public void processCarEventStream() {
        this.getCarEventStream()
                .filter(
                        new FilterFunction<CarEvent>() {
                            @Override
                            public boolean filter(CarEvent value) throws Exception {
                                return value.getSpeedModule().equals(0);
                            }
                        }
                ).setParallelism(1)
                .keyBy(new VidHighwayWestboundKeySelector())
                .countWindow(4, 1)
                .apply(new WindowFunction<CarEvent, Tuple7<Long, Long, String, String, Integer, Integer, Integer>, Tuple3<String, String, Boolean>, GlobalWindow>() {

                    @Override
                    public void apply(Tuple3<String, String, Boolean> stringStringBooleanTuple3, GlobalWindow globalWindow, Iterable<CarEvent> iterable, Collector<Tuple7<Long, Long, String, String, Integer, Integer, Integer>> collector) throws Exception {

                        Iterator<CarEvent> carEventIterator = iterable.iterator();
                        Long startTimestamp = null;
                        Long stopTimestamp;
                        Integer lastPosition = null;
                        Integer i = 1;

                        while (carEventIterator.hasNext()) {

                            CarEvent carEvent = carEventIterator.next();

                            if (i == 1) {
                                startTimestamp = carEvent.getTimestamp();
                                lastPosition = carEvent.getPosition();
                            } else {

                                if (lastPosition != carEvent.getPosition().intValue()) {
                                    break;
                                } else {

                                    if (i == 4) {
                                        stopTimestamp = carEvent.getTimestamp();
                                        Tuple7<Long, Long, String, String, Integer, Integer, Integer> finalTuple = new Tuple7<>(
                                                startTimestamp,
                                                stopTimestamp,
                                                carEvent.getVehicleID(),
                                                carEvent.getHighwayID(),
                                                carEvent.getSegment(),
                                                carEvent.getWestbound() ? 1 : 0,
                                                lastPosition
                                        );
                                        collector.collect(finalTuple);
                                    }

                                }

                            }

                            i++;

                        }

                    }
                }).writeAsCsv(getOutputCSVFilePath().toString());
    }

}
