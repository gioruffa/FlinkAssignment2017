package master2017.flink.events;

import java.io.Serializable;

/**
 * Commodity POJO for the input line
 */
public class CarEvent implements Serializable{
    Long timestamp;
    String vehicleID;
    Integer speedModule; //speed in physics is an array, we are just considering the module. It has only pos. values
    String highwayID; //string because it is not clear the value it can have
    Short lane; //just from 0 to 4
    Boolean westbound; //TODO: consider to use an enum, how does flink take it for partitioning?
    Integer segment;
    Integer position;

    public CarEvent(
            Long timestamp,
            String vehicleID,
            Integer speedModule,
            String highwayID,
            Short lane,
            Boolean westbound,
            Integer segment,
            Integer position) {
        this.timestamp = timestamp;
        this.vehicleID = vehicleID;
        this.speedModule = speedModule;
        this.highwayID = highwayID;
        this.lane = lane;
        this.westbound = westbound;
        this.segment = segment;
        this.position = position;
    }

    public Integer getSegment() {
        return segment;
    }

    public void setSegment(Integer segment) {
        this.segment = segment;
    }

    public Integer getPosition() {
        return position;
    }

    public void setPosition(Integer position) {
        this.position = position;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public String getVehicleID() {
        return vehicleID;
    }

    public void setVehicleID(String vehicleID) {
        this.vehicleID = vehicleID;
    }

    public Integer getSpeedModule() {
        return speedModule;
    }

    public void setSpeedModule(Integer speedModule) {
        this.speedModule = speedModule;
    }

    public String getHighwayID() {
        return highwayID;
    }

    public void setHighwayID(String highwayID) {
        this.highwayID = highwayID;
    }

    public Short getLane() {
        return lane;
    }

    public void setLane(Short lane) {
        this.lane = lane;
    }

    public Boolean getWestbound() {
        return westbound;
    }

    public void setWestbound(Boolean westbound) {
        this.westbound = westbound;
    }

    @Override
    public String toString() {
        return "CarEvent{" +
                "timestamp=" + timestamp +
                ", vehicleID='" + vehicleID + '\'' +
                ", speedModule=" + speedModule +
                ", highwayID='" + highwayID + '\'' +
                ", lane=" + lane +
                ", westbound=" + westbound +
                ", segment=" + segment +
                ", position=" + position +
                '}';
    }

    /**
     * Create a CarEvent from line of csv
     * Since costructure never should throw exceptions, we use a stitic method to validate and create the class.
     * Handle exceptions accordingly
     * @param inputOrigin
     * @return
     * @throws Exception
     */
    public static CarEvent fromString(String inputOrigin) throws Exception {
        String[] tokens = inputOrigin.split(",");
        if (tokens.length != 8)
        {
            throw new Exception("Malformed input line");
        }

        return new CarEvent(
                Long.valueOf(tokens[0]),
                tokens[1],
                Integer.valueOf(tokens[2]),
                tokens[3],
                Short.valueOf(tokens[4]),
                Integer.valueOf(tokens[5]) == 0 ? Boolean.FALSE : Boolean.TRUE,
                Integer.valueOf(tokens[6]),
                Integer.valueOf(tokens[7])
        );


    }

}
