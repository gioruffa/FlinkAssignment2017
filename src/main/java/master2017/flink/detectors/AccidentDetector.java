package master2017.flink.detectors;

import master2017.flink.events.CarEvent;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

//import master2017.flink.functions.windowFunctions.AccidentAlarmTriggerFunction;

public class AccidentDetector extends Detector {

    public AccidentDetector(
            String outputFolder,
            KeyedStream<CarEvent, Tuple3<String, String, Boolean>> carEventKeyedStream
    ){
        super(outputFolder, carEventKeyedStream, "accidents.csv");
    }

    @Override
    public void processCarEventKeyedStream() {
                this.getCarEventKeyedStream()
                        .countWindow(4,1)
                        //.apply(new AccidentAlarmTriggerFunction())
                        .apply(new WindowFunction<CarEvent, Tuple7<Long,Long,String,String,Integer,Boolean,Integer>, Tuple3<String,String,Boolean>, GlobalWindow>() {

                            @Override
                            public void apply(Tuple3<String, String, Boolean> stringStringBooleanTuple3, GlobalWindow globalWindow, Iterable<CarEvent> iterable, Collector<Tuple7<Long,Long,String,String,Integer,Boolean,Integer>> collector) throws Exception {

                                Iterator<CarEvent> carEventIterator = iterable.iterator();
                                Long startTimestamp = null;
                                Long stopTimestamp;
                                Integer lastPosition = null;
                                Integer i = 1;

                                while (carEventIterator.hasNext())
                                {

                                    //System.out.println(i);
                                    CarEvent carEvent = carEventIterator.next();

                                    if (i == 1) {
                                        startTimestamp = carEvent.getTimestamp();
                                        lastPosition = carEvent.getPosition();
                                    }

                                    else {

                                        if (lastPosition != carEvent.getPosition().intValue()) {
                                            break;
                                        }

                                        else {

                                            if (i == 4) {
                                                stopTimestamp = carEvent.getTimestamp();
                                                Tuple7<Long,Long,String,String,Integer,Boolean,Integer> finalTuple = new Tuple7<>(
                                                        startTimestamp,
                                                        stopTimestamp,
                                                        carEvent.getVehicleID(),
                                                        carEvent.getHighwayID(),
                                                        carEvent.getSegment(),
                                                        carEvent.getWestbound(),
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
