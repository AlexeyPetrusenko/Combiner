package com.revjet.interview;

import java.util.concurrent.TimeUnit;

/**
 * com.revjet.interview.Combiner takes items from multiple input producers (think "streams" or "queues") and
 * merges them into single output. Each input has a priority (weight), which influences the frequency at which
 * elements from this particular input appear in the output.
 * <p/>
 * Example:<br>
 * if input A has a priority of 9.5, and input B has a priority of 0.5, then on average, provided both inputs don't
 * have delays in supply, for every 100 items added to the output, 95 should come from input A, and 5 should come from
 * queue B.
 * <p/>
 * Priorities make sense only if producers are faster than consumer. If producers are slower than consumer, then
 * throughput becomes more important than prioritization. I.e. if high priority input doesn't produce any elements
 * for the time being, while some of the lower-priority inputs do, output should not wait for higher-priority input.
 * Instead output should be supplied with elements from the lower-priority inputs.
 * <p/>
 * Implementation notes:
 * <ul>
 * <li>Combiner and CombinerInput must be thread-safe</li>
 * <li>Combiner may or may not buffer input elements internally</li>
 * <li>Combiner may or may not start internal threads</li>
 *
 * </ul>
 *
 * @param <T> the type of elements in this combiner
 */
public interface Combiner<T> {

    /**
     * Adds input channel to the combiner
     *
     * @param priority       priority of the queue. Determines ratio of the output elements
     * @param isEmptyTimeout if input stayed empty (didn't produce any element) for given timeout,
     *                       it's automatically removed
     *                       (see {@link CombinerInput#remove()})
     * @param timeUnit       time unit of the isEmptyTimeout
     * @return {@link CombinerInput} interface that allows to add elements to the added input
     */
    CombinerInput<T> addInput(double priority, long isEmptyTimeout, TimeUnit timeUnit);

    /**
     * Fetches next element from the combiner if available (retrieves and removes).
     * If all inputs of the combiner are empty (no new elements were produced), returns null.
     * This methods should not block.
     *
     * @return element or null if empty
     */
    T poll();

    /**
     * Fetches the next element from the combiner (retrieves and removes), waiting up to the
     * specified wait time if necessary for an element to become available.
     *
     * @param timeout  time to wait
     * @param timeUnit time unit of the timeout
     * @return element or null if specified waiting time elapsed before element became available
     * @throws InterruptedException if interrupted while waiting
     */
    T poll(long timeout, TimeUnit timeUnit) throws InterruptedException;

    /**
     * Interface of the Combiner's input. Provides methods to add new element through this input and
     * to forcefully detach (remove) this input.
     *
     * @param <T> type of the elements this input consumes. Same as the type of Combiner.
     */
    interface CombinerInput<T> {

        /**
         * Add element to this input waiting if necessary for internal capacity to become available.
         * <p>
         * May or may not block. Actual behavior is implementation specific.
         *
         * @param value element to add
         * @throws IllegalStateException if this input has been already detached (removed)
         */
        void put(T value);

        /**
         * Removes (detaches) this input. Does nothing if input was already detached.
         */
        void remove();

        /**
         * Checks whether this input was removed (detached)
         *
         * @return true if input was detached, false otherwise
         */
        boolean isRemoved();
    }
}
