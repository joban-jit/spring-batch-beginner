package spring.batch.anomalyFinder.pojo;

import java.util.List;

public record DailySensorData(
        String date,
        List<Double> measurements
) {
}
