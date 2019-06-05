package borunovv;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Task - the unit of work which should be executed at given time.
 *
 * @param <T> generic task result type
 */
class Task<T> implements Delayed {

    private static final int TIMEOUT_INFINITE = -1;

    /**
     * Time at which callable should be executed.
     */
    private final LocalDateTime time;

    /**
     * Will be executed at given time.
     */
    private final Callable<T> callable;

    /**
     * Used to achieve correct task order with same LocalDateTime.
     */
    private final long serialIndex;

    /**
     * Monitor to track final state achieved.
     */
    private final Object doneMonitor = new Object();

    /**
     * Current task state
     */
    private AtomicReference<State> state = new AtomicReference<>(State.PENDING);

    /**
     * Task execution result.
     */
    private AtomicReference<T> result = new AtomicReference<>();

    /**
     * Task execution error.
     */
    private AtomicReference<Throwable> error = new AtomicReference<>();

    /**
     * C-tor.
     *
     * @param time        task execution time.
     * @param callable    what to execute
     * @param serialIndex internal serial task index, assigned by scheduler
     *                    in case to keep task order in case of same time.
     */
    Task(LocalDateTime time, Callable<T> callable, long serialIndex) {
        if (time == null) {
            throw new IllegalArgumentException("Param 'time' must not be null");
        }
        if (callable == null) {
            throw new IllegalArgumentException("Param 'callable' must not be null");
        }
        this.time = time;
        this.callable = callable;
        this.serialIndex = serialIndex;
    }

    /**
     * {@link Delayed} interface implementation
     *
     * @param unit time unit
     * @return how much time is elapsed to task execution
     */
    @Override
    public long getDelay(TimeUnit unit) {
        LocalDateTime nowTime = LocalDateTime.now();
        long millisToStart = ChronoUnit.MILLIS.between(nowTime, time);
        return unit.convert(millisToStart, TimeUnit.MILLISECONDS);
    }

    /**
     * {@link Delayed} interface implementation
     *
     * @param o second object to compare with
     * @return comparison result as usual.
     */
    @Override
    public int compareTo(Delayed o) {
        if (o instanceof Task) {
            int result = time.compareTo(((Task) o).time);
            // If time's are equal we do compare serial numbers to keep task order correct.
            // Note: it is actual only in case of single scheduler's worker thread.
            return result == 0 ?
                    Long.compare(this.serialIndex, ((Task) o).serialIndex) :
                    result;
        } else {
            throw new IllegalArgumentException("Expected object of type 'Task'");
        }
    }

    /**
     * Execute the task.
     */
    void execute() {
        try {
            if (tryChangeState(State.PENDING, State.EXECUTING)) {
                result.set(callable.call());
                tryChangeState(State.EXECUTING, State.SUCCESS);
            }
        } catch (Throwable e) {
            if (tryChangeState(State.EXECUTING, State.ERROR)) {
                error.set(e);
            }
        }
    }

    /**
     * Atomic state change.
     *
     * @param expectedState expected current state
     * @param newState      new state
     * @return {@code true} on success
     */
    private boolean tryChangeState(State expectedState, State newState) {
        if (state.compareAndSet(expectedState, newState)) {
            if (newState == State.ERROR || newState == State.SUCCESS || newState == State.CANCELLED) {
                synchronized (doneMonitor) {
                    doneMonitor.notifyAll();
                }
            }
            return true;
        }
        return false;
    }

    /**
     * Cancel the task.
     *
     * @return {@code false} if the task could not be cancelled,
     * typically because it has already completed normally;
     * {@code true} otherwise
     */
    boolean cancel() {
        return tryChangeState(State.PENDING, State.CANCELLED);
    }

    /**
     * @return current state.
     */
    State getState() {
        return state.get();
    }

    /**
     * Wait for any of next states: CANCELLED, SUCCESS, ERROR.
     * Wait infinitely.
     *
     * @return task state (one of CANCELLED, SUCCESS, ERROR)
     * @throws InterruptedException if waiting was interrupted
     */
    public State waitFinalState() throws InterruptedException {
        return waitFinalStateLocal(TIMEOUT_INFINITE, null);
    }

    /**
     * Wait for any of next states: CANCELLED, SUCCESS, ERROR.
     * Wait with timeout.
     *
     * @return current task state (could be any)
     * @throws InterruptedException if waiting was interrupted
     */
    public State waitFinalState(long timeout, TimeUnit unit) throws InterruptedException {
        if (timeout < 0) {
            throw new IllegalArgumentException("Expected timeout >= 0, actual: " + timeout);
        }
        return waitFinalStateLocal(timeout, unit);
    }

    /**
     * @return the task execution error (if so), or {@code null} if no error occurred.
     */
    public Throwable getError() {
        return error.get();
    }

    /**
     * @return the task execution result (if task already done), or {@code null} otherwise.
     */
    public T getResult() {
        return result.get();
    }

    /**
     * Do wait final task state: CANCELLED, SUCCESS or ERROR.
     *
     * @param timeout timeout to wait
     * @param unit    time unit for timeout
     * @return current task state (could be any)
     * @throws InterruptedException if waiting was interrupted
     */
    private State waitFinalStateLocal(long timeout, TimeUnit unit) throws InterruptedException {
        switch (state.get()) {
            case SUCCESS:
            case CANCELLED:
            case ERROR:
                // Task in one of 'final' state.
                return state.get();

            default: {
                // Task is not in 'final' state. Do wait.
                synchronized (doneMonitor) {
                    if (timeout >= 0) {
                        doneMonitor.wait(unit.toMillis(timeout));
                    } else {
                        doneMonitor.wait();
                    }
                }
                return state.get();
            }
        }
    }
}