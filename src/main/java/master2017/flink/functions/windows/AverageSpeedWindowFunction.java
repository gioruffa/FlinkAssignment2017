package master2017.flink.functions.windows;

import com.sun.org.apache.xpath.internal.operations.Bool;
import master2017.flink.events.AverageSpeedViolationEvent;
import master2017.flink.events.CarEvent;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class AverageSpeedWindowFunction implements WindowFunction<CarEvent, AverageSpeedViolationEvent, Tuple3<String, String, Boolean>, TimeWindow> {
    Integer speedLimit;
    Integer startingSegment;
    Integer endingSegment;

    public AverageSpeedWindowFunction(Integer speedLimit, Integer startingSegment, Integer endingSegment) {
        this.speedLimit = speedLimit;
        this.startingSegment = startingSegment;
        this.endingSegment = endingSegment;
    }

    /**
     * min segmnent number is 0
     * @return
     */
    Integer getBeforeStargingSegment()
    {
        return startingSegment == 0 ? startingSegment : startingSegment -1;
    }

    /**
     * max segment number is 1
     * @return
     */
    Integer getAfterEndingSegnmet()
    {
        return endingSegment == 99 ? endingSegment : endingSegment + 1;
    }

    @Override
    public void apply(
            Tuple3<String, String, Boolean> key,
            TimeWindow timeWindow,
            Iterable<CarEvent> input,
            Collector<AverageSpeedViolationEvent> collector
    ) throws Exception {
        System.out.println("Applying window");
        Iterator<CarEvent> debugIterator = input.iterator();
        Iterator<CarEvent> inputIterator = input.iterator();
        Iterator<CarEvent> otherIterator = input.iterator();

        boolean hasStartingMinus1 = false;
        boolean hasEndingPlus1 = false;

        //first of all we need to detect if the var has completed the track.
        //we need at least one element in startingSegment-1 and endingSegment+1 (regardless of the direction)

        while (inputIterator.hasNext())
        {
            CarEvent carEvent = inputIterator.next();
            hasStartingMinus1 |= carEvent.getSegment().equals(getBeforeStargingSegment());
            hasEndingPlus1 |= carEvent.getSegment().equals(getAfterEndingSegnmet());
        }
        if (!(hasStartingMinus1 && hasEndingPlus1))
        {
            return;
        }
        while (debugIterator.hasNext()) {
            CarEvent event = debugIterator.next();
            System.out.println(event);
        }
        System.out.println("Car has completed the segments!");



        //if the car is going westbound -> first event in time is endingSegment, last is startingSegment
        //if the car is going eastbound -> first event in time is startingSegnment, last is endingSegment

        Integer enteringSegment = getEnteringSegment(key.f2);
        Integer exitingSegment = getExitignSegment(key.f2);

        Long enteringTimestamp = Long.MAX_VALUE;
        Long exitingTimestamp = Long.MIN_VALUE;

        CarEvent enteringEvent = null;
        CarEvent exitingEvent = null;
        //TODO: just take the min time and the maximum time of sector included!

        while (otherIterator.hasNext())
        {
            CarEvent carEvent = otherIterator.next();
            //we want the earliest enter and the latest exit
            if(carEvent.getSegment().equals(enteringSegment))
            {
                if (carEvent.getTimestamp().compareTo(enteringTimestamp) < 0 )
                {
                    enteringTimestamp = carEvent.getTimestamp();
                    enteringEvent = carEvent;
                }
//                enteringTimestamp = carEvent.getTimestamp().compareTo(enteringTimestamp) < 0 ? carEvent.getTimestamp() : enteringTimestamp;
            }
            else if (carEvent.getSegment().equals(exitingSegment))
            {
                if(carEvent.getTimestamp().compareTo(exitingTimestamp) > 0)
                {
                    exitingTimestamp = carEvent.getTimestamp();
                    exitingEvent = carEvent;
                }
//                exitingTimestamp = carEvent.getTimestamp().compareTo(exitingTimestamp) > 0  ? carEvent.getTimestamp() : exitingTimestamp;
            }
        }
        AverageSpeedViolationEvent candidateViolationEvent = new AverageSpeedViolationEvent(
                enteringEvent,
                exitingEvent
        );

        System.out.println(candidateViolationEvent);
        if(candidateViolationEvent.getAvgSpeedInMilesPerHour().compareTo(speedLimit.doubleValue()) > 0)
        {
            System.out.println("Average speed violation detected");
            collector.collect(candidateViolationEvent);
        }


        //Once we know if the car has completed the pass we need the first element in

        System.out.println("Closing window");
    }

    Integer getEnteringSegment(Boolean westbound)
    {
        return westbound ? endingSegment : startingSegment;
    }

    Integer getExitignSegment(Boolean westbound)
    {
        return westbound ? startingSegment : endingSegment;
    }

}
