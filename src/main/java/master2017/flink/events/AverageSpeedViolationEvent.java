package master2017.flink.events;

import org.apache.flink.api.java.tuple.Tuple6;

public class AverageSpeedViolationEvent {
    CarEvent enteringEvent;
    CarEvent exitingEvent;

    public AverageSpeedViolationEvent(CarEvent enteringEvent, CarEvent exitingEvent) {
        this.enteringEvent = enteringEvent;
        this.exitingEvent = exitingEvent;
    }

    @Override
    public String toString() {
        return "AverageSpeedViolationEvent{\n" +
                "\tenteringEvent=" + enteringEvent + ",\n" +
                "\texitingEvent=" + exitingEvent + ",\n" +
                "\tavg speed:=" + String.format("%f", getAvgSpeedInKilometersPerHour()) + "Km/h\n" +
                '}';
    }

    public Double getAvgSpeedInMetersPerSeconds()
    {
        return Math.abs(enteringEvent.getPosition() - exitingEvent.getPosition()) /
                new Long(exitingEvent.getTimestamp() - enteringEvent.getTimestamp()).doubleValue();
    }

    public Double getAvgSpeedInKilometersPerHour()
    {
        return getAvgSpeedInMetersPerSeconds() * new Double(3.6);
    }

    public Double getAvgSpeedInMilesPerHour()
    {
        return getAvgSpeedInMetersPerSeconds() * new Double(2.23694);
    }

    public Tuple6<Long,Long,String,String,Integer,Double> toTuple()
    {
        return  new Tuple6<>(
                enteringEvent.getTimestamp(),
                exitingEvent.getTimestamp(),
                enteringEvent.getVehicleID(),
                enteringEvent.getHighwayID(),
                enteringEvent.getWestbound() ? 1 : 0,
                getAvgSpeedInMilesPerHour()
        );
    }


}
