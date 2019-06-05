package borunovv;

import org.junit.Test;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class TaskSchedulerTest {

    @Test
    public void completes_several_tasks_in_correct_order() throws InterruptedException, ExecutionException {
        // ARRANGE
        TaskScheduler scheduler = new TaskScheduler();
        scheduler.start(4);

        final List<String> resultList = new ArrayList<>();
        LocalDateTime now = LocalDateTime.now();

        // ACT
        // Put in back order (with time diff = 1 second between tasks)
        Future<String> f5 = scheduler.schedule(now.plus(5, ChronoUnit.SECONDS), makeCallable("Fifth", resultList));
        Future<String> f4 = scheduler.schedule(now.plus(4, ChronoUnit.SECONDS), makeCallable("Fourth", resultList));
        Future<String> f3 = scheduler.schedule(now.plus(3, ChronoUnit.SECONDS), makeCallable("Third", resultList));
        Future<String> f2 = scheduler.schedule(now.plus(2, ChronoUnit.SECONDS), makeCallable("Second", resultList));
        Future<String> f1 = scheduler.schedule(now.plus(1, ChronoUnit.SECONDS), makeCallable("First", resultList));

        List<Future<String>> futures = Arrays.asList(f1, f2, f3, f4, f5);

        // Wait all tasks executed
        for (Future<String> f : futures) {
            f.get();
        }

        // Stop the scheduler and wait all worker threads termination.
        scheduler.stop(true);

        // VERIFY
        assertTrue(f1.isDone());
        assertEquals("First", f1.get());

        assertTrue(f2.isDone());
        assertEquals("Second", f2.get());

        assertTrue(f3.isDone());
        assertEquals("Third", f3.get());

        assertTrue(f4.isDone());
        assertEquals("Fourth", f4.get());

        assertTrue(f5.isDone());
        assertEquals("Fifth", f5.get());

        assertEquals(5, resultList.size());
        assertEquals("First", resultList.get(0));
        assertEquals("Second", resultList.get(1));
        assertEquals("Third", resultList.get(2));
        assertEquals("Fourth", resultList.get(3));
        assertEquals("Fifth", resultList.get(4));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void completes_several_tasks_in_correct_order_even_with_same_time() throws InterruptedException, ExecutionException {
        // ARRANGE
        TaskScheduler scheduler = new TaskScheduler();
        // Note: we use single thread here, because only in this case
        // the order of tasks with same time can be properly achieved.
        scheduler.start(1);

        final List<String> resultList = new ArrayList<>();
        LocalDateTime now = LocalDateTime.now();

        // ACT
        // Put in ascending order with _same_ time.
        Future<String> f1 = scheduler.schedule(now, makeCallable("First", resultList));
        Future<String> f2 = scheduler.schedule(now, makeCallable("Second", resultList));
        Future<String> f3 = scheduler.schedule(now, makeCallable("Third", resultList));
        Future<String> f4 = scheduler.schedule(now, makeCallable("Fourth", resultList));
        Future<String> f5 = scheduler.schedule(now, makeCallable("Fifth", resultList));

        List<Future<String>> futures = Arrays.asList(f1, f2, f3, f4, f5);

        // Wait all tasks executed
        for (Future<String> f : futures) {
            f.get();
        }

        // Stop the scheduler and wait all worker threads termination.
        scheduler.stop(true);

        // VERIFY
        assertEquals(5, resultList.size());
        assertEquals("First", resultList.get(0));
        assertEquals("Second", resultList.get(1));
        assertEquals("Third", resultList.get(2));
        assertEquals("Fourth", resultList.get(3));
        assertEquals("Fifth", resultList.get(4));
    }

    private Callable<String> makeCallable(final String name, List<String> resultList) {
        return () -> {
            synchronized (resultList) {
                resultList.add(name);
            }
            return name;
        };
    }

    @Test
    public void cancels_the_task_before_execution() throws InterruptedException, ExecutionException {
        // ARRANGE
        TaskScheduler scheduler = new TaskScheduler();
        scheduler.start(4);
        LocalDateTime inOneMinuteFromNow = LocalDateTime.now().plus(1, ChronoUnit.MINUTES);

        // ACT
        Future<String> future = scheduler.schedule(inOneMinuteFromNow, () -> "fake");
        boolean cancelled = future.cancel(true);

        // VERIFY
        assertTrue(cancelled);
        assertTrue(future.isDone());
        assertTrue(future.isCancelled());

        try {
            future.get();
            fail("Expected CancellationException");
        } catch (CancellationException e) {
        }

        // Stop the scheduler and wait all worker threads termination.
        scheduler.stop(true);
    }

    @Test
    public void method_get_rethrows_actual_exception_if_task_execution_failed() throws InterruptedException {
        // ARRANGE
        TaskScheduler scheduler = new TaskScheduler();
        scheduler.start(4);
        LocalDateTime now = LocalDateTime.now();

        // ACT
        Future<String> future = scheduler.schedule(now, () -> {
            throw new RuntimeException("Test exception");
        });

        // VERIFY
        try {
            future.get();
            fail("Expected ExecutionException");
        } catch (ExecutionException e) {
            assertEquals("Test exception", e.getCause().getMessage());
        }

        // Stop the scheduler and wait all worker threads termination.
        scheduler.stop(true);
    }

    @Test
    public void method_get_throws_timeout_exception_while_waiting_not_enough_time() throws InterruptedException, ExecutionException {
        // ARRANGE
        TaskScheduler scheduler = new TaskScheduler();
        scheduler.start(4);
        LocalDateTime inOneMinuteFromNow = LocalDateTime.now().plus(1, ChronoUnit.MINUTES);

        // ACT
        Future<String> future = scheduler.schedule(inOneMinuteFromNow, () -> "Fake");

        // VERIFY
        try {
            future.get(1, TimeUnit.SECONDS);
            fail("Expected TimeoutException");
        } catch (TimeoutException e) {
        }

        // Stop the scheduler and wait all worker threads termination.
        scheduler.stop(true);
    }

    @Test
    public void stress_test_with_concurrent_clients() throws InterruptedException {
        // ARRANGE
        TaskScheduler scheduler = new TaskScheduler();
        scheduler.start(4);

        final int CLIENTS = 10;
        final int TASKS_PER_CLIENT = 100;
        final CountDownLatch startSignal = new CountDownLatch(1);
        final CountDownLatch doneSignal = new CountDownLatch(CLIENTS * TASKS_PER_CLIENT);

        ConcurrentHashMap<Integer, AtomicInteger> counters = new ConcurrentHashMap<>();
        List<Future<Integer>> futures = new ArrayList<>();

        // ACT

        // Run concurrent clients
        for (int i = 0; i < CLIENTS; ++i) {
            final int clientIndex = i;
            counters.put(i, new AtomicInteger());
            new Thread(() -> {
                try {
                    // Wait single signal to start.
                    // Need to more fair simultaneous multiple threads start.
                    startSignal.await();

                    // Per thread random
                    final Random THREAD_RAND = new Random();

                    // Schedule tasks to increment the counter by +1.
                    for (int task = 0; task < TASKS_PER_CLIENT; ++task) {
                        final int taskIndex = task;
                        Thread.sleep(THREAD_RAND.nextInt(100)); // Wait a little for more random thread scheduling
                        Future<Integer> future = scheduler.schedule(
                                LocalDateTime.now().plusNanos(1000000 * THREAD_RAND.nextInt(2000)), // [0..2] seconds in future
                                () -> {
                                    // Do increment the client's counter.
                                    counters.get(clientIndex).incrementAndGet();
                                    doneSignal.countDown();
                                    return clientIndex;
                                });
                        synchronized (futures) {
                            futures.add(future);
                        }
                    }
                } catch (InterruptedException ignore) {
                }
            }).start();
        }

        // Start all threads
        startSignal.countDown();
        // Wait completion
        doneSignal.await();


        // VERIFY

        // All counters must contain value == TASKS_PER_CLIENT (TASKS_PER_CLIENT times of +1 = TASKS_PER_CLIENT)
        assertEquals(CLIENTS, counters.size());
        for (int i = 0; i < CLIENTS; ++i) {
            assertEquals(TASKS_PER_CLIENT, counters.get(i).get());
        }

        // Futures count must be equals to total task count: (CLIENTS * TASKS_PER_CLIENT)
        assertEquals(CLIENTS * TASKS_PER_CLIENT, futures.size());

        // All futures must be in completed state
        for (Future<Integer> future : futures) {
            assertTrue(future.isDone());
        }

        // Stop the scheduler and wait all worker threads termination.
        scheduler.stop(true);
    }
}