package spring.batch.anomalyFinder.processor;

import org.springframework.batch.item.ItemProcessor;
import spring.batch.anomalyFinder.pojo.DailyAggregatedSensorData;
import spring.batch.anomalyFinder.pojo.DailySensorData;

import java.util.DoubleSummaryStatistics;

public class RawToAggregateSensorDataProcessor implements ItemProcessor<DailySensorData, DailyAggregatedSensorData> {
    @Override
    public DailyAggregatedSensorData process(DailySensorData item) throws Exception {
        DoubleSummaryStatistics doubleSummaryStatistics = item.measurements()
                .stream()
                .mapToDouble(Double::doubleValue)
                .summaryStatistics();

        return new DailyAggregatedSensorData(
                item.date(),
                convertToCelsius(doubleSummaryStatistics.getMin()),
                        convertToCelsius(doubleSummaryStatistics.getAverage()),
                                convertToCelsius(doubleSummaryStatistics.getMax())
        );
    }

    public static double convertToCelsius(double fahT){
        return (5*(fahT-32)/9);
    }
}
