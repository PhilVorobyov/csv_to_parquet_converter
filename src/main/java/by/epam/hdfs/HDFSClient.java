package by.epam.hdfs;

import static org.slf4j.LoggerFactory.getLogger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URI;

@Component
public class HDFSClient {

  public static final Logger LOG = getLogger(HDFSClient.class);

  @Value("${webhdfs.uri}")
  private String webHdfs;


  @Value("${remote.csv.file.path}")
  private String remoteCsvPath;

  @Value("${csv.file.path}")
  private String localCsvPath;


  public void downloadFileFromHDFS() {
    FileSystem fileSystem = getFileSystem();
    downloadRequiredFile(fileSystem);
  }

  private FileSystem getFileSystem() {
    FileSystem fileSystem = null;
    try {
      Configuration conf = new Configuration();
      conf.set("dfs.client.use.datanode.hostname", "true");
      fileSystem = FileSystem.get(URI.create(webHdfs), conf);
      LOG.debug("fileSystem is succesfully created");
    } catch (IOException e) {
      LOG.error("couldn't establish connection with hdfs server");
    }
    return fileSystem;
  }

  private void downloadRequiredFile(final FileSystem fileSystem)  {
    try {
      LOG.debug("Start downloading file from hdfs");
      fileSystem.copyToLocalFile(new Path(remoteCsvPath), new Path(localCsvPath));
    } catch (IOException e) {
      LOG.error("can't download " + remoteCsvPath + " from hdfs");
    }
  }
}
