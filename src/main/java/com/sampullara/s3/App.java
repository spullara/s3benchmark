package com.sampullara.s3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.sampullara.cli.Args;
import com.sampullara.cli.Argument;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

public class App {

  @Argument(alias = "m", description = "Base multiplier on processors")
  Integer multiplier = 10;

  @Argument(alias = "r", description = "Range in multiples of 5 added to based concurrency")
  Integer range = 10;

  @Argument(alias = "w", description = "Number of writes to attempt per iteration")
  Integer writes = 100;

  @Argument(alias = "b", description = "Bucket name")
  String bucket;

  @Argument(alias = "a", description = "Amazon access key for the bucket", required = true)
  String accessKey;

  @Argument(alias = "s", description = "Amazon secret key for the bucket", required = true)
  String secretKey;

  private final ExecutorService es = Executors.newCachedThreadPool();

  public void testS3Client() {
    AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
    Random random = new Random();

    for (int j = 0; j < range; j++) {
      Set<String> keys = new HashSet<>();
      int concurrency = Runtime.getRuntime().availableProcessors() * multiplier + j * 5;

      ClientConfiguration cc = new ClientConfiguration();
      cc.setConnectionTimeout(1000);
      cc.setMaxConnections(concurrency);
      cc.setSocketTimeout(10000);
      cc.setUseGzip(false);
      cc.setUseReaper(true);
      cc.setMaxErrorRetry(5);
      cc.setPreemptiveBasicProxyAuth(true);
      cc.setConnectionTTL(60000);
      final AmazonS3Client s3c = new AmazonS3Client(credentials, cc);
      S3ClientOptions options = new S3ClientOptions();
      s3c.setS3ClientOptions(options);

      final Semaphore semaphore = new Semaphore(concurrency);
      for (int n = 0; n < 100; n += 10) {
        final long start = System.currentTimeMillis();
        int totalBytes = n * 1024;
        final byte[] bytes = new byte[totalBytes];
        random.nextBytes(bytes);
        final ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(bytes.length);
        for (int i = 0; i < writes; i++) {
          final String s = reverse(UUID.randomUUID().toString());
          keys.add(s);
          semaphore.acquireUninterruptibly();
          es.submit(() -> {
            PutObjectRequest por = new PutObjectRequest(bucket, s, new ByteArrayInputStream(bytes), metadata);
            s3c.putObject(por);
            semaphore.release();
          });
        }
        semaphore.acquireUninterruptibly(concurrency);
        semaphore.release(concurrency);
        long diff = System.currentTimeMillis() - start;
        System.out.println("concurrency: " + concurrency + " doing " + writes + " writes of " + totalBytes + " bytes in " + diff + " ms: " + (1000 * writes / diff) + " w/s");
      }

      long start = System.currentTimeMillis();
      final AtomicLong totalBytes = new AtomicLong();
      for (String key : keys) {
        semaphore.acquireUninterruptibly();
        es.submit(() -> {
          GetObjectRequest gor = new GetObjectRequest(bucket, key);
          S3Object object = s3c.getObject(gor);
          DataInputStream dis = new DataInputStream(object.getObjectContent());
          int length = (int) object.getObjectMetadata().getContentLength();
          byte[] bytes = new byte[length];
          try {
            dis.readFully(bytes);
            totalBytes.addAndGet(length);
            dis.close();
          } catch (IOException e) {
            e.printStackTrace();
          }
          semaphore.release();
        });
      }
      semaphore.acquireUninterruptibly(concurrency);
      semaphore.release(concurrency);
      long diff = System.currentTimeMillis() - start;
      System.out.println("concurrency: " + concurrency + " doing " + keys.size() + " of " + (totalBytes.get() / keys.size()) + " bytes in " + diff + " ms: " + ((1000 * keys.size()) / diff) + " r/s");
    }

    System.exit(0);
  }

  public static void main(String[] args) {
    App s3PerfTest = new App();
    Args.parseOrExit(s3PerfTest, args);
    s3PerfTest.testS3Client();
  }

  public String reverse(String s) {
    char[] chars = s.toCharArray();
    for (int i = 0; i < chars.length / 2; i++) {
      char tmp = chars[i];
      chars[i] = chars[chars.length - i - 1];
      chars[chars.length - i - 1] = tmp;
    }
    return new String(chars);
  }
}

