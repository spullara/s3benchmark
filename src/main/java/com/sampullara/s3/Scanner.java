package com.sampullara.s3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.sampullara.cli.Args;
import com.sampullara.cli.Argument;
import com.wavefront.integrations.metrics.WavefrontReporter;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.MappingJsonFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;

public class Scanner {
  @Argument(alias = "m", description = "Multiplier on processors")
  private Integer multiplier = 60;
  @Argument(alias = "b", description = "Bucket name", required = true)
  private String bucket;
  @Argument(alias = "p", description = "Bucket path", required = true)
  private String path;
  @Argument(alias = "r", description = "Bucket region", required = true)
  private String region = "us-west-2";

  public static void main(String[] args) throws UnknownHostException, InterruptedException {
    Scanner scanner = new Scanner();
    Args.parse(scanner, args);
    System.out.println(scanner.count());
  }

  private long count() throws UnknownHostException, InterruptedException {
    MetricRegistry mr = new MetricRegistry();

    System.out.println("Starting metrics reporter");
    WavefrontReporter.forRegistry(mr)
            .withJvmMetrics()
            .withSource(InetAddress.getLocalHost().getHostName())
            .withPointTag("service", "s3benchmark")
            .build("wavefront.sampullara.com", 2878)
            .start(5, TimeUnit.SECONDS);

    int concurrency = Runtime.getRuntime().availableProcessors() * multiplier;
    ClientConfiguration cc = new ClientConfiguration();
    cc.setConnectionTimeout(1000);
    cc.setMaxConnections(concurrency);
    cc.setSocketTimeout(10000);
    cc.setUseGzip(false);
    cc.setUseReaper(true);
    cc.setMaxErrorRetry(5);
//      cc.setPreemptiveBasicProxyAuth(true);
    cc.setConnectionTTL(60000);
    AmazonS3 s3c = AmazonS3ClientBuilder.standard()
            .withClientConfiguration(cc)
            .withRegion(Regions.fromName(region))
            .build();

    Counter counter = mr.counter("s3scanner.objects");
    Timer getTimer = mr.timer("s3scanner.get");
    Timer listTimer = mr.timer("s3scanner.list");
    Timer parseTimer = mr.timer("s3scanner.parsing");

    ExecutorService es = Executors.newFixedThreadPool(concurrency);
    Queue<S3ObjectSummary> s3ObjectSummaries = new ConcurrentLinkedQueue<>();

    System.out.println("Reading S3 objects...");
    String continuationToken = null;
    do {
      Timer.Context time = listTimer.time();
      try {
        ListObjectsV2Result objectListing;
        if (continuationToken == null) {
          objectListing = s3c.listObjectsV2(bucket, path);
        } else {
          objectListing = s3c.listObjectsV2(new ListObjectsV2Request()
                  .withBucketName(bucket)
                  .withPrefix(path)
                  .withContinuationToken(continuationToken));
        }
        List<S3ObjectSummary> objectSummaries = objectListing.getObjectSummaries();
        s3ObjectSummaries.addAll(objectSummaries);
        counter.inc(objectSummaries.size());
        continuationToken = objectListing.getNextContinuationToken();
      } finally {
        time.stop();
      }
    } while (continuationToken != null);
    System.out.println("Found " + s3ObjectSummaries.size() + " objects.");

    MappingJsonFactory mf = new MappingJsonFactory();
    AtomicLong count = new AtomicLong();
    Semaphore semaphore = new Semaphore(concurrency);
    for (S3ObjectSummary s3ObjectSummary : s3ObjectSummaries) {
      semaphore.acquire();
      es.submit(() -> {
        Timer.Context time = getTimer.time();
        try {
          S3Object object = s3c.getObject(new GetObjectRequest(s3ObjectSummary.getBucketName(), s3ObjectSummary.getKey()));
          InputStream gis = new GZIPInputStream(object.getObjectContent());
          BufferedReader br = new BufferedReader(new InputStreamReader(gis, "UTF-8"));
          String line;
          while ((line = br.readLine()) != null) {
            Timer.Context parseTime = parseTimer.time();
            JsonParser jsonParser = mf.createJsonParser(line);
            jsonParser.readValueAsTree();
            count.incrementAndGet();
            parseTime.stop();
          }
        } catch (Exception e) {
          System.out.println("Error reading: " + s3ObjectSummary.getKey());
          e.printStackTrace();
        } finally {
          time.stop();
          semaphore.release();
        }
      });
    }
    semaphore.acquire(concurrency);

    return count.get();
  }

}
