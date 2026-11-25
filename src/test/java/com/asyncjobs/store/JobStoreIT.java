package com.asyncjobs.store;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.asyncjobs.domain.DeadLetter;
import com.asyncjobs.domain.Job;
import com.asyncjobs.domain.JobState;
import com.asyncjobs.domain.NewJob;
import com.asyncjobs.domain.QueueDepth;

import org.junit.jupiter.api.Test;

class JobStoreIT extends AbstractStoreIT {

    // ------------------------------------------------------------------ submission

    @Test
    void enqueuedJobIsQueuedWithItsParameters() {
        Job job = store.enqueue(new NewJob("echo", json.createObjectNode().put("k", "v"), 7, 4, 45, null, 0, null)).job();

        Job found = reload(job);
        assertThat(found.state()).isEqualTo(JobState.QUEUED);
        assertThat(found.type()).isEqualTo("echo");
        assertThat(found.payload().path("k").asText()).isEqualTo("v");
        assertThat(found.priority()).isEqualTo(7);
        assertThat(found.maxRetries()).isEqualTo(4);
        assertThat(found.timeoutSeconds()).isEqualTo(45);
        assertThat(found.attempts()).isZero();
        assertThat(found.result()).isNull();
        assertThat(found.lastError()).isNull();
        assertThat(store.find(UUID.randomUUID())).isEmpty();
    }

    @Test
    void sameIdempotencyKeyReturnsTheOriginalJobInsteadOfADuplicate() {
        NewJob request = new NewJob("echo", null, 0, 3, 30, null, 0, "order-42");

        JobStore.Enqueued first = store.enqueue(request);
        JobStore.Enqueued second = store.enqueue(request);

        assertThat(first.created()).isTrue();
        assertThat(second.created()).isFalse();
        assertThat(second.job().id()).isEqualTo(first.job().id());
        assertThat(jdbc.queryForObject("SELECT count(*) FROM jobs", Long.class)).isEqualTo(1);
    }

    // ------------------------------------------------------------------ claiming

    @Test
    void claimTakesHighestPriorityFirstThenOldest() {
        Job lowOld = enqueue("echo", 1, 3);
        Job high = enqueue("echo", 9, 3);
        Job lowNew = enqueue("echo", 1, 3);

        List<Job> claimed = store.claim("w1", 2, GRACE_SECONDS);

        assertThat(claimed).extracting(Job::id).containsExactly(high.id(), lowOld.id());
        assertThat(store.claim("w1", 10, GRACE_SECONDS)).extracting(Job::id).containsExactly(lowNew.id());
        assertThat(store.claim("w1", 10, GRACE_SECONDS)).isEmpty();
    }

    @Test
    void claimStartsAnAttemptUnderALease() {
        Job job = enqueue("echo", 0, 3);

        Job claimed = store.claim("w1", 1, GRACE_SECONDS).get(0);

        assertThat(claimed.id()).isEqualTo(job.id());
        assertThat(claimed.state()).isEqualTo(JobState.RUNNING);
        assertThat(claimed.attempts()).isEqualTo(1);
        assertThat(claimed.lockedBy()).isEqualTo("w1");
        assertThat(claimed.startedAt()).isNotNull();
        // lease = per-attempt timeout (30s) + grace (15s)
        assertThat(claimed.leaseExpiresAt()).isBetween(claimed.startedAt().plusSeconds(44), claimed.startedAt().plusSeconds(46));
    }

    @Test
    void delayedJobIsNotClaimableUntilItsRunAt() {
        Job delayed = store.enqueue(new NewJob("echo", null, 0, 3, 30, null, 60_000, null)).job();
        Job scheduled = store.enqueue(new NewJob("echo", null, 0, 3, 30, Instant.now().plusSeconds(3600), 0, null)).job();

        assertThat(store.claim("w1", 10, GRACE_SECONDS)).isEmpty();
        assertThat(store.depth().delayed()).isEqualTo(2);

        makeDue(delayed);
        assertThat(store.claim("w1", 10, GRACE_SECONDS)).extracting(Job::id).containsExactly(delayed.id());
        assertThat(reload(scheduled).state()).isEqualTo(JobState.QUEUED);
    }

    @Test
    void pausedQueueHandsOutNothingUntilResumed() {
        enqueue("echo", 0, 3);

        store.setPaused(true);
        assertThat(store.claim("w1", 10, GRACE_SECONDS)).isEmpty();
        assertThat(store.depth().paused()).isTrue();
        assertThat(store.depth().ready()).isEqualTo(1);

        store.setPaused(false);
        assertThat(store.claim("w1", 10, GRACE_SECONDS)).hasSize(1);
    }

    @Test
    void concurrentWorkersNeverReceiveTheSameJob() throws Exception {
        int jobs = 300;
        int workers = 8;
        for (int i = 0; i < jobs; i++) {
            enqueue("echo", i % 3, 3);
        }

        ExecutorService pool = Executors.newFixedThreadPool(workers);
        CountDownLatch go = new CountDownLatch(1);
        List<Future<List<UUID>>> futures = new ArrayList<>();
        for (int w = 0; w < workers; w++) {
            String workerId = "w" + w;
            Callable<List<UUID>> task = () -> {
                List<UUID> mine = new ArrayList<>();
                go.await();
                List<Job> batch;
                while (!(batch = store.claim(workerId, 7, GRACE_SECONDS)).isEmpty()) {
                    batch.forEach(j -> mine.add(j.id()));
                }
                return mine;
            };
            futures.add(pool.submit(task));
        }
        go.countDown();

        List<UUID> all = new ArrayList<>();
        for (Future<List<UUID>> f : futures) {
            all.addAll(f.get());
        }
        pool.shutdown();

        assertThat(all).hasSize(jobs);
        Set<UUID> unique = new HashSet<>(all);
        assertThat(unique).hasSize(jobs);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM jobs WHERE state = 'running' AND attempts = 1", Long.class))
                .isEqualTo(jobs);
    }

    // ------------------------------------------------------------------ outcomes

    @Test
    void successStoresTheResultAndClearsTheLease() {
        Job claimed = claimOne(enqueue("echo", 0, 3), "w1");

        boolean recorded = store.succeed(claimed.id(), "w1", 1, json.createObjectNode().put("answer", 42));

        Job done = reload(claimed);
        assertThat(recorded).isTrue();
        assertThat(done.state()).isEqualTo(JobState.SUCCEEDED);
        assertThat(done.result().path("answer").asInt()).isEqualTo(42);
        assertThat(done.lockedBy()).isNull();
        assertThat(done.leaseExpiresAt()).isNull();
        assertThat(done.finishedAt()).isNotNull();
    }

    @Test
    void failedAttemptWithRetriesLeftWaitsOutItsBackoff() {
        Job claimed = claimOne(enqueue("echo", 0, 2), "w1");

        Job failed = store.fail(claimed.id(), "w1", 1, "boom", true, 60_000).orElseThrow();

        assertThat(failed.state()).isEqualTo(JobState.FAILED);
        assertThat(failed.attempts()).isEqualTo(1);
        assertThat(failed.lastError()).isEqualTo("boom");
        assertThat(failed.finishedAt()).isNull();
        assertThat(failed.runAt()).isAfter(failed.updatedAt().plusSeconds(55));
        assertThat(store.claim("w1", 10, GRACE_SECONDS)).as("still backing off").isEmpty();
        assertThat(store.depth().retrying()).isEqualTo(1);
        assertThat(store.deadLetters(10)).isEmpty();

        makeDue(failed);
        Job retry = store.claim("w2", 10, GRACE_SECONDS).get(0);
        assertThat(retry.attempts()).isEqualTo(2);
        assertThat(retry.lastError()).as("last error stays visible while retrying").isEqualTo("boom");
    }

    @Test
    void jobMovesToDeadLetterStoreOnceRetriesAreExhausted() {
        Job job = enqueue("echo", 0, 2); // 2 retries = 3 attempts in total

        for (int attempt = 1; attempt <= 3; attempt++) {
            Job claimed = claimOne(job, "w1");
            assertThat(claimed.attempts()).isEqualTo(attempt);
            Job after = store.fail(job.id(), "w1", attempt, "boom " + attempt, true, 0).orElseThrow();
            assertThat(after.state()).isEqualTo(attempt < 3 ? JobState.FAILED : JobState.DEAD_LETTERED);
        }

        Job dead = reload(job);
        assertThat(dead.state()).isEqualTo(JobState.DEAD_LETTERED);
        assertThat(dead.attempts()).isEqualTo(3);
        assertThat(dead.lastError()).isEqualTo("boom 3");
        assertThat(dead.finishedAt()).isNotNull();
        assertThat(store.claim("w1", 10, GRACE_SECONDS)).as("dead letters are never redelivered").isEmpty();

        List<DeadLetter> letters = store.deadLetters(10);
        assertThat(letters).hasSize(1);
        assertThat(letters.get(0).jobId()).isEqualTo(job.id());
        assertThat(letters.get(0).reason()).isEqualTo("retries_exhausted");
        assertThat(letters.get(0).attempts()).isEqualTo(3);
        assertThat(letters.get(0).lastError()).isEqualTo("boom 3");
        assertThat(letters.get(0).payload().path("n").asInt()).isEqualTo(1);
        assertThat(store.depth().deadLettered()).isEqualTo(1);
    }

    @Test
    void nonRetryableFailureIsDeadLetteredImmediately() {
        Job claimed = claimOne(enqueue("echo", 0, 5), "w1");

        Job after = store.fail(claimed.id(), "w1", 1, "bad payload", false, 1000).orElseThrow();

        assertThat(after.state()).isEqualTo(JobState.DEAD_LETTERED);
        assertThat(after.attempts()).isEqualTo(1);
        assertThat(store.deadLetters(10)).extracting(DeadLetter::reason).containsExactly("non_retryable");
    }

    @Test
    void zeroRetriesMeansExactlyOneAttempt() {
        Job claimed = claimOne(enqueue("echo", 0, 0), "w1");

        assertThat(store.fail(claimed.id(), "w1", 1, "boom", true, 0).orElseThrow().state())
                .isEqualTo(JobState.DEAD_LETTERED);
    }

    // ------------------------------------------------------------------ at-least-once

    @Test
    void jobOfACrashedWorkerIsRedeliveredAfterItsLeaseExpires() {
        Job job = enqueue("echo", 0, 3);
        claimOne(job, "crashed-worker");

        assertThat(store.reapExpiredLeases(100, 1000, 100)).as("lease still valid").isEmpty();

        expireLease(job);
        List<Job> reaped = store.reapExpiredLeases(100, 1000, 100);

        assertThat(reaped).extracting(Job::id).containsExactly(job.id());
        Job requeued = reload(job);
        assertThat(requeued.state()).isEqualTo(JobState.FAILED);
        assertThat(requeued.lockedBy()).isNull();
        assertThat(requeued.lastError()).contains("lease expired on attempt 1").contains("crashed-worker");
        assertThat(store.deadLetters(10)).isEmpty();

        makeDue(job);
        Job redelivered = store.claim("healthy-worker", 1, GRACE_SECONDS).get(0);
        assertThat(redelivered.id()).isEqualTo(job.id());
        assertThat(redelivered.attempts()).as("the lost attempt is counted").isEqualTo(2);
    }

    @Test
    void workerThatLostItsLeaseCannotOverwriteTheNewOwnersOutcome() {
        Job job = enqueue("echo", 0, 3);
        claimOne(job, "slow-worker");      // attempt 1, then stalls past its lease
        expireLease(job);
        store.reapExpiredLeases(0, 0, 100);
        makeDue(job);
        claimOne(job, "new-worker");       // attempt 2: the job is now executing twice

        // The stalled worker wakes up and tries to report. Both outcomes are rejected.
        assertThat(store.succeed(job.id(), "slow-worker", 1, json.createObjectNode().put("from", "slow"))).isFalse();
        assertThat(store.fail(job.id(), "slow-worker", 1, "late failure", true, 0)).isEmpty();
        assertThat(store.release(job.id(), "slow-worker", 1)).isFalse();
        assertThat(reload(job).state()).isEqualTo(JobState.RUNNING);
        assertThat(reload(job).lockedBy()).isEqualTo("new-worker");

        assertThat(store.succeed(job.id(), "new-worker", 2, json.createObjectNode().put("from", "new"))).isTrue();
        assertThat(reload(job).result().path("from").asText()).isEqualTo("new");

        // And nothing can change a finished job.
        assertThat(store.succeed(job.id(), "new-worker", 2, json.createObjectNode().put("from", "again"))).isFalse();
        assertThat(reload(job).result().path("from").asText()).isEqualTo("new");
    }

    @Test
    void jobThatKeepsKillingItsWorkerIsEventuallyDeadLettered() {
        Job job = enqueue("echo", 0, 1); // 2 attempts

        claimOne(job, "w1");
        expireLease(job);
        assertThat(store.reapExpiredLeases(0, 0, 100).get(0).state()).isEqualTo(JobState.FAILED);

        claimOne(job, "w2");
        expireLease(job);
        assertThat(store.reapExpiredLeases(0, 0, 100).get(0).state()).isEqualTo(JobState.DEAD_LETTERED);

        assertThat(store.deadLetters(10)).extracting(DeadLetter::reason).containsExactly("lease_expired");
        assertThat(store.claim("w3", 10, GRACE_SECONDS)).isEmpty();
    }

    @Test
    void releasedJobIsImmediatelyClaimableAndTheAttemptIsNotCounted() {
        Job job = enqueue("echo", 0, 3);
        claimOne(job, "w1");

        assertThat(store.release(job.id(), "w1", 1)).isTrue();

        Job released = reload(job);
        assertThat(released.state()).isEqualTo(JobState.QUEUED);
        assertThat(released.attempts()).isZero();
        assertThat(claimOne(job, "w2").attempts()).isEqualTo(1);
    }

    // ------------------------------------------------------------------ operational control

    @Test
    void cancelOnlyAffectsJobsThatHaveNotStarted() {
        Job pending = enqueue("echo", 0, 3);
        Job running = enqueue("echo", 5, 3);
        claimOne(running, "w1");

        assertThat(store.cancel(pending.id())).isEqualTo(JobStore.CancelResult.CANCELLED);
        assertThat(store.cancel(pending.id())).as("already cancelled").isEqualTo(JobStore.CancelResult.NOT_CANCELLABLE);
        assertThat(store.cancel(running.id())).isEqualTo(JobStore.CancelResult.NOT_CANCELLABLE);
        assertThat(store.cancel(UUID.randomUUID())).isEqualTo(JobStore.CancelResult.NOT_FOUND);

        assertThat(reload(pending).state()).isEqualTo(JobState.CANCELLED);
        assertThat(reload(pending).finishedAt()).isNotNull();
        assertThat(reload(running).state()).isEqualTo(JobState.RUNNING);
        assertThat(store.claim("w1", 10, GRACE_SECONDS)).isEmpty();
    }

    @Test
    void drainCancelsEveryPendingJobButLeavesRunningAndFinishedOnesAlone() {
        Job running = enqueue("echo", 9, 3);
        claimOne(running, "w1");
        Job ready = enqueue("echo", 0, 3);
        Job delayed = store.enqueue(new NewJob("sleep", null, 0, 3, 30, null, 60_000, null)).job();
        Job retrying = enqueue("flaky", 8, 3);
        claimOne(retrying, "w1");
        store.fail(retrying.id(), "w1", 1, "boom", true, 60_000);

        assertThat(store.drain("sleep")).as("drain one type").isEqualTo(1);
        assertThat(reload(delayed).state()).isEqualTo(JobState.CANCELLED);
        assertThat(reload(ready).state()).isEqualTo(JobState.QUEUED);

        assertThat(store.drain(null)).as("drain everything else").isEqualTo(2);
        assertThat(reload(ready).state()).isEqualTo(JobState.CANCELLED);
        assertThat(reload(retrying).state()).isEqualTo(JobState.CANCELLED);
        assertThat(reload(running).state()).isEqualTo(JobState.RUNNING);
        assertThat(store.depth().pending()).isZero();
    }

    @Test
    void depthBreaksTheQueueDownByState() {
        enqueue("echo", 0, 3);
        enqueue("echo", 0, 3);
        store.enqueue(new NewJob("echo", null, 0, 3, 30, null, 60_000, null));
        Job running = enqueue("echo", 9, 3);
        claimOne(running, "w1");
        Job retrying = enqueue("echo", 8, 3);
        claimOne(retrying, "w1");
        store.fail(retrying.id(), "w1", 1, "boom", true, 60_000);
        Job dead = enqueue("echo", 7, 0);
        claimOne(dead, "w1");
        store.fail(dead.id(), "w1", 1, "boom", true, 0);
        Job done = enqueue("echo", 6, 0);
        claimOne(done, "w1");
        store.succeed(done.id(), "w1", 1, null);

        QueueDepth depth = store.depth();

        assertThat(depth.ready()).isEqualTo(2);
        assertThat(depth.delayed()).isEqualTo(1);
        assertThat(depth.retrying()).isEqualTo(1);
        assertThat(depth.running()).isEqualTo(1);
        assertThat(depth.deadLettered()).isEqualTo(1);
        assertThat(depth.pending()).isEqualTo(4);
        assertThat(depth.oldestReadyAgeSeconds()).isGreaterThanOrEqualTo(0);
        assertThat(depth.paused()).isFalse();
    }

    @Test
    void emptyQueueHasZeroDepth() {
        QueueDepth depth = store.depth();

        assertThat(depth.pending()).isZero();
        assertThat(depth.running()).isZero();
        assertThat(depth.oldestReadyAgeSeconds()).isZero();
    }

    @Test
    void deadLetterCanBeRequeuedWithAFreshRetryBudget() {
        Job job = enqueue("echo", 0, 0);
        claimOne(job, "w1");
        store.fail(job.id(), "w1", 1, "boom", true, 0);

        Optional<Job> requeued = store.requeueDeadLetter(job.id());

        assertThat(requeued).isPresent();
        assertThat(requeued.get().state()).isEqualTo(JobState.QUEUED);
        assertThat(requeued.get().attempts()).isZero();
        assertThat(store.deadLetters(10)).isEmpty();
        assertThat(claimOne(job, "w2").attempts()).isEqualTo(1);
        assertThat(store.requeueDeadLetter(job.id())).as("not dead-lettered any more").isEmpty();
    }

    @Test
    void listFiltersByState() {
        Job a = enqueue("echo", 0, 3);
        Job b = enqueue("echo", 5, 3);
        claimOne(b, "w1");

        assertThat(store.list(JobState.QUEUED, 10)).extracting(Job::id).containsExactly(a.id());
        assertThat(store.list(JobState.RUNNING, 10)).extracting(Job::id).containsExactly(b.id());
        assertThat(store.list(null, 10)).hasSize(2);
        assertThat(store.list(null, 1)).hasSize(1);
    }

    /** Claims until the given job is the one handed out (it must be the highest-priority due job). */
    private Job claimOne(Job job, String workerId) {
        makeDue(job);
        List<Job> claimed = store.claim(workerId, 1, GRACE_SECONDS);
        assertThat(claimed).extracting(Job::id).containsExactly(job.id());
        return claimed.get(0);
    }
}
