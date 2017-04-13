package com.revjet.interview;

import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by Alexey on 4/12/2017.
 */
public class CombinerImpl<T> implements Combiner<T> {

    /**
     * It's used for handling synchronization.
     */
    private final ReentrantLock globalLock = new ReentrantLock();

    /**
     * Holds CombinerInput by priority.
     */
    private final NavigableMap<Double, CombinerInputImpl> priorityMap = new ConcurrentSkipListMap<>();

    /**
     * List of all CombinerInputs.
     */
    private Set<CombinerInputImpl> inputs = new HashSet<>();
    /**
     * Sum of all priorities, it's needed for priority processing.
     */
    private double totalPriority = 0;

    @Override
    public CombinerInput<T> addInput(double priority, long isEmptyTimeout, TimeUnit timeUnit) {
        CombinerInputImpl input = new CombinerInputImpl(priority, isEmptyTimeout, timeUnit);
        globalLock.lock();
        try {
            inputs.add(input);
            doResetPriorities();
        } finally {
            globalLock.unlock();
        }
        return input;
    }

    @Override
    public T poll() {
        CombinerInputImpl input = next();
        return input == null ? null : input.poll();
    }

    @Override
    public T poll(long timeout, TimeUnit timeUnit) throws InterruptedException {
        CombinerInputImpl input = next();
        return input == null ? null : input.poll(timeout, timeUnit);
    }

    /**
     * Fills 'priorityMap' and provides synchronization.
     */
    private void resetPriorities() {
        globalLock.lock();
        try {
            doResetPriorities();
        } finally {
            globalLock.unlock();
        }
    }

    /**
     * Calculates weight of each CombinerInput and fills 'priorityMap'.
     * CombinerInput is skipped if it is empty.
     */
    private void doResetPriorities() {
        totalPriority = 0;
        priorityMap.clear();
        inputs.forEach(this::add);
    }

    /**
     * Adds CombinerInput to the 'priorityMap' with weights, summarizes total priority.
     * CombinerInput is skipped if it is empty.
     * @param result
     */
    private void add(CombinerInputImpl result) {
        if (result.isEmpty())
            return;
        totalPriority += result.priority;
        priorityMap.put(totalPriority, result);
    }

    /**
     * Gets next CombinerInput by weight.
     * @return CombinerInput
     */
    private CombinerInputImpl next() {
        double key = ThreadLocalRandom.current().nextDouble() * totalPriority;
        Map.Entry<Double, CombinerInputImpl> entry = priorityMap.ceilingEntry(key);
        return entry == null ? null : entry.getValue();
    }

    /**
     * Removes CombinerInput from list of CombinerInputs.
     * @param input - CombinerInput to remove.
     */
    private void remove(CombinerInputImpl input) {
        globalLock.lock();
        try {
            inputs.remove(input);
            resetPriorities();
        } finally {
            globalLock.unlock();
        }
    }

    private class CombinerInputImpl implements Combiner.CombinerInput<T> {
        /**
         * Internal lock for synchronization.
         */
        private final ReentrantLock localLock = new ReentrantLock();
        /**
         * Condition for timeout.
         */
        private final Condition notEmpty = localLock.newCondition();
        /**
         * Service for timeout.
         */
        private final ExecutorService timeoutWaitingService = Executors.newSingleThreadExecutor();
        /**
         * Queue with messages.
         */
        private BlockingQueue<T> queue = new LinkedBlockingDeque<>();
        /**
         * Input priority.
         */
        private double priority;
        /**
         * Timeout for waiting incoming messages.
         */
        private long emptyTimeout;
        /**
         * Time unit.
         */
        private TimeUnit timeUnit;
        /**
         * Flag that holds detached state.
         */
        private AtomicBoolean removed = new AtomicBoolean(false);

        private CombinerInputImpl(double priority, long emptyTimeout, TimeUnit timeUnit) {
            if (priority <= 0d) {
                throw new IllegalArgumentException("'priority' must be greater than 0.");
            }
            if (emptyTimeout <= 0 || timeUnit == null) {
                throw new IllegalArgumentException("Timeout and time unit must be specified. 'emptyTimeout' must be greater than 0.");
            }
            this.priority = priority;
            this.emptyTimeout = emptyTimeout;
            this.timeUnit = timeUnit;
            waitTimeout();
        }

        @Override
        public void put(T value) {
            if (removed.get()) {
                throw new IllegalStateException("Input has already been removed.");
            }
            boolean wasEmpty = false;
            localLock.lock();
            try {
                wasEmpty = isEmpty();
                queue.add(value);
                notEmpty.signal();
            } finally {
                localLock.unlock();
            }
            if (wasEmpty) {
                resetPriorities();
            }
        }

        @Override
        public void remove() {
            localLock.lock();
            try {
                doRemove();
            } finally {
                localLock.unlock();
            }
        }

        /**
         * Marks input as detached, removes itself from Combiner.
         */
        private void doRemove() {
            removed.set(true);
            CombinerImpl.this.remove(this);
            timeoutWaitingService.shutdownNow();
        }

        @Override
        public boolean isRemoved() {
            return removed.get();
        }

        /**
         * Retrieves and removes the head of the inner queue.
         * @return value
         */
        private T poll() {
            T result = queue.poll();
            if (isEmpty()) {
                waitTimeout();
                resetPriorities();
            }
            return result;
        }



        /**
         * Retrieves and removes the head of the inner queue, waiting up to the specified wait time if necessary for an element to become available.
         * @param timeout
         * @param timeUnit
         * @return value
         * @throws InterruptedException
         */
        private T poll(long timeout, TimeUnit timeUnit) throws InterruptedException {
            T result = queue.poll(timeout, timeUnit);
            if (isEmpty()) {
                waitTimeout();
                resetPriorities();
            }
            return result;
        }

        /**
         * Waits for specified time, if internal queue is empty after timeout marks itself as detached.
         */
        private void waitTimeout() {
            timeoutWaitingService.execute(() -> {
                try {
                    localLock.lockInterruptibly();
                    try {
                        notEmpty.await(emptyTimeout, timeUnit);
                        if (isEmpty()) {
                            doRemove();
                        }
                    } finally {
                        localLock.unlock();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });
        }

        /**
         * Returns true is no more messages in internal queue, false otherwise.
         * @return
         */
        private boolean isEmpty() {
            return queue.isEmpty();
        }
    }
}
