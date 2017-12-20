package master2017.flink.detectors;

import master2017.flink.events.CarEvent;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;


import java.nio.file.Path;
import java.nio.file.Paths;

public abstract class Detector {
    String outputFolder;
    SingleOutputStreamOperator<CarEvent> carEventStream;
    String outputFileName;


    public Detector(
            String outputFolder,
            SingleOutputStreamOperator<CarEvent> carEventStream,
            String outputFileName
    ) {
        this.outputFolder = outputFolder;
        this.carEventStream = carEventStream;
        this.outputFileName = outputFileName;
    }

    public String getOutputFolder() {
        return outputFolder;
    }

    public void setOutputFolder(String outputFolder) {
        this.outputFolder = outputFolder;
    }

    public SingleOutputStreamOperator<CarEvent> getCarEventStream() {
        return carEventStream;
    }

    public void setCarEventStream(SingleOutputStreamOperator<CarEvent> carEventStream) {
        this.carEventStream = carEventStream;
    }

    public String getOutputFileName() {
        return outputFileName;
    }

    public void setOutputFileName(String outputFileName) {
        this.outputFileName = outputFileName;
    }

    abstract public void processCarEventKeyedStream();
    public Path getOutputCSVFilePath()
    {
        return Paths.get(this.getOutputFolder(), this.getOutputFileName());
    }
}
