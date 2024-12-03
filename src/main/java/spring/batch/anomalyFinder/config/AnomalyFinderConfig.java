package spring.batch.anomalyFinder.config;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.support.DefaultBatchConfiguration;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.xml.StaxEventItemWriter;
import org.springframework.batch.item.xml.builder.StaxEventItemReaderBuilder;
import org.springframework.batch.item.xml.builder.StaxEventItemWriterBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.batch.BatchDataSourceScriptDatabaseInitializer;
import org.springframework.boot.autoconfigure.batch.BatchProperties;
import org.springframework.boot.sql.init.DatabaseInitializationMode;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.WritableResource;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import spring.batch.anomalyFinder.mapper.SensorDataTextMapper;
import spring.batch.anomalyFinder.pojo.DataAnomaly;
import spring.batch.anomalyFinder.processor.RawToAggregateSensorDataProcessor;
import spring.batch.anomalyFinder.pojo.DailyAggregatedSensorData;
import spring.batch.anomalyFinder.pojo.DailySensorData;
import spring.batch.anomalyFinder.processor.SensorDataAnomalyProcessor;

import javax.sql.DataSource;

@Configuration
@PropertySource("classpath:db.properties")
public class AnomalyFinderConfig extends DefaultBatchConfiguration {


    @Value("file:input/HTE2NP.txt")
    private Resource rawInputResource;

//    @Value("file:input/HTE2NP.xml")
//    private WritableResource aggregatedDailyOutputXmlResource;

    @Value("input/HTE2NP.xml")
    private FileSystemResource aggregatedDailyOutputXmlResource;

    @Value("file:HTE2NP-anomalies.csv")
    private WritableResource anomalyDataResource;


    @Bean
    public Job temperatureSensor(JobRepository jobRepository,
                                    @Qualifier("aggregateStep") Step aggregateStep,
                                    @Qualifier("reportAnomaliesStep") Step reportAnomaliesStep
                                 ){
        return new JobBuilder("temperatureSensor", jobRepository)
                .start(aggregateStep)
                .next(reportAnomaliesStep)
                .build();

    }

    @Bean
    public Step aggregateStep(JobRepository jobRepository,
                              PlatformTransactionManager transactionManager,
                              @Qualifier("dailyAggregatedSensorDataItemWriter") ItemWriter<DailyAggregatedSensorData> itemWriter
    ){
        return new StepBuilder("aggregate-sensor", jobRepository)
                .<DailySensorData, DailyAggregatedSensorData>chunk(1, transactionManager)
                .reader(new FlatFileItemReaderBuilder<DailySensorData>()
                        .name("dailySensorDataReader")
                        .resource(rawInputResource)
                        .lineMapper(new SensorDataTextMapper())
                        .build())
                .processor(new RawToAggregateSensorDataProcessor())
                .writer(itemWriter)
                .build();


    }



    @Bean
    public StaxEventItemWriter<DailyAggregatedSensorData> dailyAggregatedSensorDataItemWriter(){
        return new StaxEventItemWriterBuilder<DailyAggregatedSensorData>()
                .name("dailyAggregatedSensorDataWriter")
                .marshaller(DailyAggregatedSensorData.getMarshaller())
                .resource(aggregatedDailyOutputXmlResource)
                .rootTagName("data")
                .overwriteOutput(true)
                .build();
    }


    @Bean
    public Step reportAnomaliesStep(JobRepository jobRepository,
                              PlatformTransactionManager transactionManager,
                              @Qualifier("dailyAggregatedSensorDataItemWriter") ItemWriter<DailyAggregatedSensorData> itemWriter
    ){
        return new StepBuilder("aggregate-sensor", jobRepository)
                .<DailyAggregatedSensorData, DataAnomaly>chunk(1, transactionManager)
                .reader(new StaxEventItemReaderBuilder<DailyAggregatedSensorData>()
                        .name("dailySensorDataReader")
                        .resource(aggregatedDailyOutputXmlResource)
                        .unmarshaller(DailyAggregatedSensorData.getMarshaller())
                        .addFragmentRootElements("daily-data")
                        .build())
                .processor(new SensorDataAnomalyProcessor())
                .writer(new FlatFileItemWriterBuilder<DataAnomaly>()
                        .name("dataAnomalyWriter")
                        .resource(anomalyDataResource)
                        .delimited()
                        .delimiter(",")
                        .names("date", "type", "value")
                        .build()
                )
                .build();


    }



    /* ******************************** Spring Batch Utilities are defined below ********************************** */

    @Bean
    public PlatformTransactionManager transactionManager(DataSource dataSource){
        JdbcTransactionManager jdbcTransactionManager = new JdbcTransactionManager();
        jdbcTransactionManager.setDataSource(dataSource);
        return jdbcTransactionManager;
    }

    @Bean
    public DataSource dataSource(
            @Value("${db.driverClassName}") String driverClassName,
            @Value("${db.url}") String url,
            @Value("${db.username}") String username,
            @Value("${db.password}") String password
    ){
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource
                .setDriverClassName(driverClassName);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        dataSource.setUrl(url);
        return dataSource;
    }

    /**
     * Due to usage of {@link DefaultBatchConfiguration}, we need to explicitly (programmatically) set initializeSchema
     * mode, and we are taking this parameter from the configuration wile, defined at {@link PropertySource} on class level;
     * In case we'd use {@link org.springframework.batch.core.configuration.annotation.EnableBatchProcessing}, having
     * 'spring.batch.initialize-schema' property would be enough
     */

    @Bean
    public BatchProperties batchProperties(
            @Value("${batch.db.initialize-schema}")DatabaseInitializationMode initializationMode
            ){
        BatchProperties batchProperties = new BatchProperties();
        batchProperties.getJdbc().setInitializeSchema(initializationMode);
        return batchProperties;
    }
    /**
     * Due to usage of {@link DefaultBatchConfiguration}, db initializer need to defined in order for Spring Batch
     * to consider initializing the schema on the first usage. In case of
     * {@link org.springframework.batch.core.configuration.annotation.EnableBatchProcessing} usage, it would have
     * been resolved with 'spring.batch.initialize-schema' property
     */
    @Bean
    public BatchDataSourceScriptDatabaseInitializer batchDataSourceScriptDatabaseInitializer(
            DataSource dataSource,
            BatchProperties batchProperties
    )
    {
        return new BatchDataSourceScriptDatabaseInitializer(dataSource, batchProperties.getJdbc());
    }


}
