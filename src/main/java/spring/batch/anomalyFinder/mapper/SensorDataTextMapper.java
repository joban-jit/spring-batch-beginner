package spring.batch.anomalyFinder.mapper;

import org.springframework.batch.item.file.LineMapper;
import spring.batch.anomalyFinder.pojo.DailySensorData;

import java.util.Arrays;
import java.util.stream.Collectors;

public class SensorDataTextMapper implements LineMapper<DailySensorData> {
    @Override
    public DailySensorData mapLine(String line, int lineNumber) throws Exception {
        String[] dateAndMeasurements = line.split(":");
        return new DailySensorData(dateAndMeasurements[0],
                Arrays.stream(dateAndMeasurements[1].split(","))
                        .map(Double::parseDouble)
                        .collect(Collectors.toList())
        );
    }
}
