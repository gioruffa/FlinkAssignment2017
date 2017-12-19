package master2017.flink.KeySelectors;

import master2017.flink.events.CarEvent;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;

public class VidHighwayWestboundKeySelector implements KeySelector<CarEvent, Tuple3<String, String, Boolean>> {
    @Override
    public Tuple3<String, String, Boolean> getKey(CarEvent carEvent) throws Exception {

        return new Tuple3<>(
                carEvent.getVehicleID(),
                carEvent.getHighwayID(),
                carEvent.getWestbound()
        );

    }
}
