package borunovv;

import java.time.LocalDateTime;
import java.util.concurrent.Delayed;

/**
 * Special task to notify worker threads to stop.
 */
class StopRequestTask extends Task<Void> {

    StopRequestTask() {
        super(LocalDateTime.now(), () -> null, 0);
    }

    @Override
    public int compareTo(Delayed o) {
        return -1; // Highest priority
    }
}
