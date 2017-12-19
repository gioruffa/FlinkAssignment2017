package master2017.flink.functions.filters;

import master2017.flink.events.CarEvent;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.shaded.com.google.common.collect.ImmutableSet;

public class SegmentExtractorFilterFunction implements FilterFunction<CarEvent>{
    ImmutableSet<Integer> segments; //Flink uses Guava

    public SegmentExtractorFilterFunction(ImmutableSet<Integer> segments) {
        this.segments = segments;
    }

    @Override
    public boolean filter(CarEvent carEvent) throws Exception {
        return segments.contains(carEvent.getSegment());
    }
}
