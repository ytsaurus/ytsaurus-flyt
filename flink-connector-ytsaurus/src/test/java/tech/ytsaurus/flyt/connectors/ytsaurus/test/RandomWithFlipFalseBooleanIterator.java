package tech.ytsaurus.flyt.connectors.ytsaurus.test;

import java.util.Iterator;
import java.util.Random;

/**
 * Generates random booleans but if it encounters 'false' n consecutive times,
 * the next value gets 'flipped' (false -> true).
 * Thus, it cannot generate more than n falses in a row.
 */
public class RandomWithFlipFalseBooleanIterator implements Iterator<Boolean> {
    private final int maxConsecutive;
    private final Random random;
    private int streak;

    public RandomWithFlipFalseBooleanIterator(int maxConsecutive, Random random) {
        this.maxConsecutive = maxConsecutive;
        this.random = random;
    }

    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public synchronized Boolean next() {
        boolean current = random.nextBoolean();
        if (!current && ++streak > maxConsecutive) {
            streak = 0;
            return true;
        }
        return current;
    }
}
