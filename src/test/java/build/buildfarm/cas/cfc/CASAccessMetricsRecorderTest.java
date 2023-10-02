package build.buildfarm.cas.cfc;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import build.bazel.remote.execution.v2.Digest;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class CASAccessMetricsRecorderTest {

  private CASAccessMetricsRecorder casAccessMetricsRecorder;
  private long delay;
  private long window;

  @Before
  public void setup() throws IOException {
    this.delay = 10;
    this.window = 100;
    casAccessMetricsRecorder =
        new CASAccessMetricsRecorder(Duration.ofMillis(window), Duration.ofMillis(delay));
  }

  @After
  public void tearDown() throws InterruptedException {
    casAccessMetricsRecorder.stop();
  }

  @Test
  public void testRecordRead() {
    casAccessMetricsRecorder.start();
    List<Digest> testDigests = new ArrayList<>(1000);
    Map<Digest, AtomicInteger> expectedReadCounts = new HashMap<>(1000);
    for (int i = 0; i < 1000; i++) {
      Digest randomDigest = Digest.newBuilder().setHash(UUID.randomUUID().toString()).build();
      testDigests.add(randomDigest);
      expectedReadCounts.put(randomDigest, new AtomicInteger(0));
    }

    long windowStart = Instant.now().toEpochMilli() / delay * delay + delay * 5;
    long windowEnd = windowStart + window;

    Thread[] threads = new Thread[5];
    for (int i = 0; i < threads.length; i++) {
      threads[i] =
          new Thread(new RecordAndCountReads(testDigests, expectedReadCounts, windowStart));
      threads[i].start();
    }

    ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
    ScheduledFuture<Map<Digest, Integer>> actualReadCountsFuture =
        scheduledExecutor.schedule(
            this::getActualReadCounts,
            windowEnd - Instant.now().toEpochMilli(),
            TimeUnit.MILLISECONDS);

    try {
      Map<Digest, Integer> actualReadCounts = actualReadCountsFuture.get();
      actualReadCounts.forEach(
          (digest, readCount) -> {
            // assert with +/-5% error, due to
            assertThat(readCount).isLessThan((int) (expectedReadCounts.get(digest).get() * 1.05));
            assertThat(readCount)
                .isGreaterThan((int) (expectedReadCounts.get(digest).get() * 0.95));
          });
    } catch (InterruptedException | ExecutionException e) {
      System.err.println(e.getMessage());
    }

    for (Thread thread : threads) {
      thread.interrupt();
    }
    scheduledExecutor.shutdownNow();
  }

  @Test
  public void testRecordRead_BeforeStart() {
    Digest randomDigest = Digest.newBuilder().setHash(UUID.randomUUID().toString()).build();
    assertThrows(
        IllegalStateException.class, () -> casAccessMetricsRecorder.recordRead(randomDigest));
  }

  @Test
  public void testRecordWrite_BeforeStart() {
    Digest randomDigest = Digest.newBuilder().setHash(UUID.randomUUID().toString()).build();
    assertThrows(
        IllegalStateException.class, () -> casAccessMetricsRecorder.recordWrite(randomDigest));
  }

  private Map<Digest, Integer> getActualReadCounts() {
    Map<Digest, Integer> actualReadCounts = new HashMap<>();
    Iterator<Map<Digest, AtomicInteger>> iterator =
        casAccessMetricsRecorder.getReadIntervalCountQueue().iterator();

    while (iterator.hasNext()) {
      Map<Digest, AtomicInteger> intervalReadCount = iterator.next();
      if (!iterator.hasNext()) { // ignore last item.
        break;
      }
      intervalReadCount.forEach(
          (digest, count) ->
              actualReadCounts.compute(
                  digest, (d, v) -> v == null ? count.get() : v + count.get()));
    }
    return actualReadCounts;
  }

  class RecordAndCountReads implements Runnable {
    List<Digest> testDigests;
    Random rand = new Random();
    Map<Digest, AtomicInteger> expectedDigestAndReadCount;
    long windowStart;

    RecordAndCountReads(
        List<Digest> testDigests,
        Map<Digest, AtomicInteger> expectedDigestAndReadCount,
        long windowStart) {
      this.windowStart = windowStart;
      this.testDigests = testDigests;
      this.expectedDigestAndReadCount = expectedDigestAndReadCount;
    }

    @Override
    public void run() {
      while (!Thread.currentThread().isInterrupted()) {
        Digest digest = testDigests.get(rand.nextInt(testDigests.size()));
        casAccessMetricsRecorder.recordRead(digest);

        // record read counts for 1 full window.
        if (Instant.now().toEpochMilli() >= windowStart
            && Instant.now().toEpochMilli() < windowStart + window) {
          expectedDigestAndReadCount.get(digest).incrementAndGet();
        }
      }
    }
  }
}
