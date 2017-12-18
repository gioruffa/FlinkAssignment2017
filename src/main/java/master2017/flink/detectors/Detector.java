package master2017.flink.detectors;

import master2017.flink.events.CarEvent;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.KeyedStream;


import java.nio.file.Path;
import java.nio.file.Paths;

public abstract class Detector {
    String outputFolder;
    KeyedStream<CarEvent, Tuple3<String, String, Boolean>> carEventKeyedStream;
    String outputFileName;


    public Detector(
            String outputFolder,
            KeyedStream<CarEvent, Tuple3<String, String, Boolean>> carEventKeyedStream,
            String outputFileName
    ) {
        this.outputFolder = outputFolder;
        this.carEventKeyedStream = carEventKeyedStream;
        this.outputFileName = outputFileName;
    }

    public String getOutputFolder() {
        return outputFolder;
    }

    public void setOutputFolder(String outputFolder) {
        this.outputFolder = outputFolder;
    }

    public KeyedStream<CarEvent, Tuple3<String, String, Boolean>> getCarEventKeyedStream() {
        return carEventKeyedStream;
    }

    public void setCarEventKeyedStream(KeyedStream<CarEvent, Tuple3<String, String, Boolean>> carEventKeyedStream) {
        this.carEventKeyedStream = carEventKeyedStream;
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
