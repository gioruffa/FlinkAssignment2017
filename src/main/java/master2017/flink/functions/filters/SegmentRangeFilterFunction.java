package master2017.flink.functions.filters;

import master2017.flink.events.CarEvent;
import org.apache.flink.api.common.functions.FilterFunction;

public class SegmentRangeFilterFunction implements FilterFunction<CarEvent> {
    Integer startingSegment;
    Integer endingSegment;

    public SegmentRangeFilterFunction(Integer startingSegment, Integer endingSegment) {
        this.startingSegment = startingSegment;
        this.endingSegment = endingSegment;
    }

    @Override
    public boolean filter(CarEvent carEvent) throws Exception {
        return carEvent.getSegment() >= startingSegment && carEvent.getSegment() <= endingSegment;
    }
}
