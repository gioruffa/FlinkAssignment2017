package master2017.flink.functions.filters;

import master2017.flink.events.CarEvent;
import org.apache.flink.api.common.functions.FilterFunction;

public class HigherSpeedFilterFunction implements FilterFunction<CarEvent> {
    Integer speedLimitThreshold;

    public HigherSpeedFilterFunction(Integer speedLimitThreshold) {
        this.speedLimitThreshold = speedLimitThreshold;
    }

    public Integer getSpeedLimitThreshold() {
        return speedLimitThreshold;
    }

    public void setSpeedLimitThreshold(Integer speedLimitThreshold) {
        this.speedLimitThreshold = speedLimitThreshold;
    }

    @Override
    public boolean filter(CarEvent carEvent) throws Exception {
        return carEvent.getSpeedModule() > getSpeedLimitThreshold();
    }
}
