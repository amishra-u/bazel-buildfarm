package build.buildfarm.cas.cfc;

import static java.lang.String.format;

import build.bazel.remote.execution.v2.Digest;
import build.buildfarm.backplane.Backplane;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import java.time.Duration;
import java.time.Instant;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.extern.java.Log;

/**
 * This class records read and write metrics, updating the information to central storage at regular
 * intervals. The collected data can be utilized to determine the read count of any CAS entry, query
 * the top "n%" of frequently accessed CAS entries, and rank CAS entries based on their read
 * frequency, among other uses.
 */
@Log
public final class CASAccessMetricsRecorder {
  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

  private final Deque<Map<Digest, AtomicInteger>> readIntervalCountQueue;

  private volatile Map<Digest, AtomicInteger> currentIntervalReadCount;

  private final Duration casEntryReadCountWindow;

  private final Duration casEntryReadCountUpdateInterval;

  private final int queueLength;

  private boolean running = false;

  @SuppressWarnings("unused")
  private final Backplane backplane;

  final ReadWriteLock lock = new ReentrantReadWriteLock();

  int intervalNumber = 0; // Assists in unit test.

  public CASAccessMetricsRecorder(
      Backplane backplane,
      Duration casEntryReadCountWindow,
      Duration casEntryReadCountUpdateInterval) {
    Preconditions.checkArgument(
        casEntryReadCountUpdateInterval.toMillis() > 0,
        "CASEntryReadCountUpdateInterval must be greater than 0");
    Preconditions.checkArgument(
        casEntryReadCountWindow.toMillis() % casEntryReadCountUpdateInterval.toMillis() == 0,
        "CASEntryReadCountWindow must be divisible by CASEntryReadCountUpdateInterval");
    this.backplane = backplane;
    this.casEntryReadCountWindow = casEntryReadCountWindow;
    this.casEntryReadCountUpdateInterval = casEntryReadCountUpdateInterval;
    this.readIntervalCountQueue = new LinkedList<>();
    this.queueLength = getQueueLength();
  }

  /**
   * Increments the read count by 1 for the given digest.
   *
   * @param digest Digest to which the read count is recorded.
   */
  public void recordRead(Digest digest) {
    if (!running) {
      throw new IllegalStateException("Metrics Recorder is not running");
    }
    lock.readLock().lock();
    try {
      currentIntervalReadCount.computeIfAbsent(digest, d -> new AtomicInteger(0)).incrementAndGet();
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Records the write operation for a digest if it is new. If a read is recorded before for a newly
   * written digest, this method will have no effect.
   *
   * @param digest Digest for which the write operation is recorded.
   */
  public void recordWrite(Digest digest) {
    if (!running) {
      throw new IllegalStateException("Metrics Recorder is not running");
    }
    lock.readLock().lock();
    try {
      currentIntervalReadCount.putIfAbsent(digest, new AtomicInteger(0));
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Transitions the metrics recorder to the running state and sets up periodic read updates. Once
   * the metrics recorder is in the running state, calling this method will have no effect.
   */
  public synchronized void start() {
    if (running) {
      return;
    }
    currentIntervalReadCount = new ConcurrentHashMap<>();
    long initialDelay = getInitialDelayInMillis();
    scheduler.scheduleAtFixedRate(
        this::updateReadCount,
        initialDelay,
        casEntryReadCountUpdateInterval.toMillis(),
        TimeUnit.MILLISECONDS);
    running = true;
  }

  /** Stops the scheduler responsible to update read counts periodically. */
  public void stop() throws InterruptedException {
    running = false;
    scheduler.shutdown();
    if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
      log.severe("Could not shut down cas access metrics recorder service");
    }
    scheduler.shutdownNow();
  }

  private int getQueueLength() {
    return (int) (casEntryReadCountWindow.toMillis() / casEntryReadCountUpdateInterval.toMillis());
  }

  private long getInitialDelayInMillis() {
    // Ensure synchronization among all workers, regardless of their start time.
    return casEntryReadCountUpdateInterval.toMillis()
        - (Instant.now().toEpochMilli() % casEntryReadCountUpdateInterval.toMillis());
  }

  private void updateReadCount() {
    Stopwatch stopwatch = Stopwatch.createStarted();
    Map<Digest, AtomicInteger> lastIntervalReadCount;
    lock.writeLock().lock(); // Ensure all threads writes to new interval.
    try {
      lastIntervalReadCount = currentIntervalReadCount;
      currentIntervalReadCount = new ConcurrentHashMap<>();
      intervalNumber++;
    } finally {
      lock.writeLock().unlock();
    }
    readIntervalCountQueue.offer(lastIntervalReadCount);

    Map<Digest, Integer> effectiveReadCount = new HashMap<>();
    lastIntervalReadCount.forEach((k, v) -> effectiveReadCount.put(k, v.get()));

    Map<Digest, AtomicInteger> firstIntervalReadCount = new HashMap<>();
    if (queueLength > 0 && readIntervalCountQueue.size() > queueLength) {
      firstIntervalReadCount = readIntervalCountQueue.poll();
    }
    firstIntervalReadCount.forEach(
        (k, v) ->
            effectiveReadCount.compute(
                k, (digest, readCount) -> readCount == null ? -v.get() : readCount - v.get()));

    // TODO : Implement logic to update read counts in redis.

    long timeToUpdate = stopwatch.stop().elapsed().toMillis();
    log.fine(format("Took %d ms to update read count.", timeToUpdate));

    if (timeToUpdate >= casEntryReadCountUpdateInterval.toMillis()) {
      log.warning(
          "Consider increasing the update interval: read count update time exceeds the update interval.");
    }
  }

  @VisibleForTesting
  Deque<Map<Digest, AtomicInteger>> getReadIntervalCountQueue() {
    return readIntervalCountQueue;
  }
}
