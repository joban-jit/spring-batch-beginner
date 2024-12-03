package spring.batch.anomalyFinder.processor;

import org.springframework.batch.item.ItemProcessor;
import spring.batch.anomalyFinder.pojo.AnomalyType;
import spring.batch.anomalyFinder.pojo.DailyAggregatedSensorData;
import spring.batch.anomalyFinder.pojo.DataAnomaly;

public class SensorDataAnomalyProcessor implements ItemProcessor<DailyAggregatedSensorData, DataAnomaly> {
    private static final double THRESHOLD = 0.9;
    @Override
    public DataAnomaly process(DailyAggregatedSensorData item) throws Exception {
        if ((item.getMin() / item.getAvg()) < THRESHOLD) {
            return new DataAnomaly(item.getDate(), AnomalyType.MINIMUM, item.getMin());
        } else if ((item.getAvg() / item.getMax()) < THRESHOLD) {
            return new DataAnomaly(item.getDate(), AnomalyType.MAXIMUM, item.getMax());
        } else {
            // Convention is to return null to filter item out and not pass it to the writer
            return null;
        }
    }
}
