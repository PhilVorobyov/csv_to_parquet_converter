package by.epam.csv;

import static org.slf4j.LoggerFactory.getLogger;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import parquet.hadoop.api.WriteSupport;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

@Component
public class CsvConverter {

  public static final Logger LOG = getLogger(CsvConverter.class);

  @Value("${csv.file.path}")
  private String csvFilePath;

  @Value("${csv.schema.file.path}")
  private String csvSchemaFilePath;

  @Value("${parquet.file.path}")
  private String parquetFilePath;

  public void convertCsvToParquet() {
    if (!new File(parquetFilePath).exists()) {
      String schema = getSchema();
      MessageType messageType = MessageTypeParser.parseMessageType(schema);
      WriteSupport<List<String>> writeSupport = new CsvWriteSupport(messageType);
      try (BufferedReader br = new BufferedReader(new FileReader(new File(csvFilePath)))) {
        LOG.debug("start converting csv + " + csvFilePath + "to parquet file " + parquetFilePath);
        String line = br.readLine();
        CsvParquetWriter writer = new CsvParquetWriter(new Path(parquetFilePath), writeSupport);

        while ((line = br.readLine()) != null) {
          String[] fields = line.split(Pattern.quote(","));
          writer.write(Arrays.asList(fields));
        }
        writer.close();
      } catch (IOException e) {
        LOG.error("Failed to write parquet file: " + e.getMessage(), e);
      }
    } else {
      LOG.info("parquet file is already converted from csv");
    }
  }

  private String getSchema() {
    StringBuilder stringBuilder = new StringBuilder();
    String lineSeparator = System.getProperty("line.separator");
    String line;
    try (BufferedReader reader = new BufferedReader(new FileReader(csvSchemaFilePath))) {
      LOG.debug("start reading schema file from " + csvSchemaFilePath);
      while ((line = reader.readLine()) != null) {
        stringBuilder.append(line);
        stringBuilder.append(lineSeparator);
      }
    } catch (IOException e) {
      LOG.error("Failed to read schema file: " + e.getMessage(), e);
    }
    return stringBuilder.toString();
  }
}
