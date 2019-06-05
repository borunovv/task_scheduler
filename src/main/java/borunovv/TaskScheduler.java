package borunovv;

import java.time.LocalDateTime;
import java.util.concurrent.Callable;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * </p>
 * Task scheduler. Allows delayed execution of tasks according given time.
 * Supports several worker executor threads.
 * </p>
 * <p>
 * Note1: tasks with same time will be executed in same order as they come to scheduler<br/>
 * Note2: with more than one worker threads there are no guarantee of task order with same execution start time.
 * Due to concurrent nature of execution.
 * </p>
 * Usage:<br/>
 * <pre>
 * {@code
 *
 * TaskScheduler scheduler = new TaskScheduler();
 * scheduler.start(4);
 * Future<String> future = scheduler.schedule(LocalDateTime.now(), ()->"Result");
 * ...
 * scheduler.stop(true);
 * }
 * </pre>
 */
public class TaskScheduler {

    /**
     * Task queue. Provide correct execution time.
     */
    private final DelayQueue<Task> queue = new DelayQueue<>();

    /**
     * Flag for running state.
     */
    private volatile boolean running;

    /**
     * Current running threads counter.
     */
    private AtomicInteger runningThreads = new AtomicInteger(0);

    /**
     * Lock object for thread safety.
     */
    private final Object lock = new Object();

    /**
     * Used to achieve correct task order with same LocalDateTime.
     */
    private static AtomicLong totalTasksScheduled = new AtomicLong();

    /**
     * Total worker threads count used during last start.
     */
    private int threadsCount = 0;

    /**
     * Start the scheduler.
     *
     * @param threadsCount count of worker threads
     */
    public void start(int threadsCount) {
        if (threadsCount <= 0) {
            throw new IllegalArgumentException("Threads count must be positive");
        }

        synchronized (lock) {
            if (running) {
                throw new RuntimeException("Scheduler is already running");
            }
            this.threadsCount = threadsCount;
            for (int i = 0; i < threadsCount; ++i) {
                new Thread(this::doInThread, "task-scheduler-worker-thread-" + i).start();
                runningThreads.incrementAndGet();
            }
            running = true;
        }
    }

    /**
     * Stop the scheduler
     *
     * @param waitTermination if {@code true}, then do wait termination of all worker threads.
     * @throws InterruptedException
     */
    public void stop(boolean waitTermination) throws InterruptedException {
        synchronized (lock) {
            if (running) {
                try {
                    // Add special marks to queue to stop the worker threads.
                    for (int i = 0; i < threadsCount; ++i) {
                        queue.add(new StopRequestTask());
                    }
                    if (waitTermination) {
                        while (runningThreads.get() > 0) {
                            Thread.sleep(1);
                        }
                    }
                } finally {
                    running = false;
                }
            }
        }
    }

    /**
     * Add new task to schedule
     *
     * @param time     execution time
     * @param callable what to execute
     * @param <T>      type of execution result
     * @return {@link Future} wrapper for delayed execution result
     */
    public <T> Future<T> schedule(LocalDateTime time, Callable<T> callable) {
        Task<T> task = new Task<>(time, callable, totalTasksScheduled.incrementAndGet());
        queue.add(task);
        return new TaskFuture<>(task);
    }

    /**
     * Called by worker threads.
     * Executes next task from queue (according task start time).
     */
    private void doInThread() {
        try {
            while (true) {
                try {
                    Task task = queue.take();
                    if (task instanceof StopRequestTask) {
                        // We've got a special 'stop working' mark. Exiting processing loop now.
                        break;
                    }
                    task.execute();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt(); // Set interrupt flag
                    break;
                }
            }
        } finally {
            runningThreads.decrementAndGet();
        }
    }
}
