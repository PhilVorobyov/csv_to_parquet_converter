package by.epam.configuration;

import static org.slf4j.LoggerFactory.getLogger;

import by.epam.csv.CsvConverter;
import by.epam.hdfs.HDFSClient;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;


@SpringBootApplication
@ComponentScan("by.epam")
public class Application implements CommandLineRunner {
  public static final Logger LOG = getLogger(Application.class);

  @Autowired
  HDFSClient hdfsClient;

  @Autowired
  CsvConverter csvConverter;

  public static void main(String[] args) {
    SpringApplication.run(Application.class, args);
  }

  public void run(String... args) {
    hdfsClient.downloadFileFromHDFS();
    csvConverter.convertCsvToParquet();
  }
}
