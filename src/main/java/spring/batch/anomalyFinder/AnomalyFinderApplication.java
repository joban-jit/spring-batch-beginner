package spring.batch.anomalyFinder;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.UUID;

@SpringBootApplication
public class AnomalyFinderApplication implements CommandLineRunner {


	@Autowired
	private JobLauncher jobLauncher;

	@Autowired
	private Job temperatureSensorJob;
	public static void main(String[] args) {
		SpringApplication.run(AnomalyFinderApplication.class, args);
	}


	@Override
	public void run(String... args) throws Exception {
		String uniqueId = UUID.randomUUID().toString();
		JobParameters jobParameters = new JobParametersBuilder()
				.addString("id", uniqueId)
				.toJobParameters();

		jobLauncher.run(temperatureSensorJob, jobParameters);

	}
}
