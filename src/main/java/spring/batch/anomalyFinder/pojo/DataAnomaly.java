package spring.batch.anomalyFinder.pojo;

public record DataAnomaly(
        String date,
        AnomalyType type,
        double value
) {
}
