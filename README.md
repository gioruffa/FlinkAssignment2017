<h1 align="center">
  <div style="margin:10px;">
    <img src="https://github.com/fdiazgon/fdiazgon.github.io/blob/master/art/flink-vehicle-telematics-logo.png?raw=true" alt="project-logo" width="200px">
  </div>
  flink-vehicle-telematics
</h1>

<h4 align="center">
Stream processing of simulated on-vehicle sensors data using <a href="https://flink.apache.org/">Apache Flink</a>
</h4>


<p align="center">
  <a href="#problem-statement">Problem statement</a> •
  <a href="#running-the-program">Running the program</a> • 
  <a href="#authors">Authors</a> •
  <a href="#license">License</a>
</p>

## Problem statement

In this project we consider that each vehicle reports a position event every 30 seconds with the
following format: Time, VID, Spd, XWay, Lane, Dir, Seg, Pos.

![problem-intuition](https://github.com/fdiazgon/fdiazgon.github.io/blob/master/art/flink-vehicle-telematics-road.png?raw=true)

| Metric | Description                                                                                                         | Domain                              |
|--------|---------------------------------------------------------------------------------------------------------------------|-------------------------------------|
| Time   | timestamp when the event was emitted                                                                                | integer                             |
| VID    | identifies the vehicle                                                                                              | integer                             |
| Spd    | speed in mph                                                                                                        | integer [0, 100]                    |
| XWay   | identifies the highway where the event was emitted                                                                  | integer                             |
| Lane   | identifies the lane of the highway from which the position report is emitted                                        | integer [0, 4]                      |
| Dir    | indicates the direction                                                                                             | 0 for Eastbound and 1 for Westbound |
| Seg    | identifies the segment from which the position report is emitted                                                    | integer [0, 99]                     |
| Pos    | identifies the horizontal position of the vehicle as the number of meters from the westernmost point on the highway | integer [0, 527999]                 |

The goal of this project is to develop a Java program using Flink implementing the following functionality:

* **Speed Radar**: detect cars that overcome the speed limit of 90 mph
* **Average Speed Control**: detects cars with an average speed higher than 60 mph between
segments 52 and 56 (both included) in both directions.
* **Accident Reporter**: detects stopped vehicles on any segment. A vehicle is stopped when it
reports at least 4 consecutive events from the same position.

Notes:

* All metrics must take into account the direction field.
* A given vehicle could report more than 1 event for the same segment.
* Event time must be used for timestamping.
* Cars that do not complete the segment (52-56) are not taken into account by the average speed control.
For example 52->54 or 55->56.
* A car can be stopped on the same position for more than 4 consecutive events.
* An accident report must be sent for each group of 4 events. For example, the next figure shows 5 events for the car with 
identifier `VID`=3:

<pre>
900,3,0,0,0,1,51,306000
930,3,0,0,0,1,51,306000
960,3,0,0,0,1,51,306000
990,3,0,0,0,1,51,306000
1020,3,0,0,0,1,51,306000
</pre>

## Running the program

The main program receives two arguments: the path to the csv with the events and the destination folder to write the 
results. In this last folder, you will find three csv files, each one storing the output of three types of events 
detected as explained above. There is a sample file in the [data/](data/traffic-3xways_head1M.tar.gz) folder that needs to be extracted.

If you don't have a flink cluster running you can launch it locally. 

```bash
$FLINK_HOME/bin/start-local.sh
```

To run the app with the sample csv, run the following commands:

```bash
$ mvn clean package -Pbuild-jar
$ mkdir -p output
$ flink run -p 10 -c master2017.flink.VehicleTelematics target/flink-assignment-1.0-SNAPSHOT.jar $PATH_TO_INPUT_FILE $PATH_TO_OUTPUT_FOLDER
```

Check the `output` folder for the results 
(`accidents.csv`, `avgspeedfines.csv` and `speedfines.csv`).

## Acknowledgements
This readme was adapted from the one provided by [Fernando Díaz González](https://github.com/fdiazgon/flink-vehicle-telematics), with his permission. 

## Authors

* **Giorgio Ruffa**
* **Jaime Elguero Tejera**

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details