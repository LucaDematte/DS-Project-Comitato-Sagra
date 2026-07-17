package it.unitn.ds.cs;

import java.io.Serializable;

/**
 * Represents the pair {@code <e, i>} used by the coordinator to define the order of updates.
 * The values of the pairs store the following information:
 * <ul>
 *     <li>{@code e}: the epoch in which the update was processed. {@code e} is incremented when the coordinator changes.</li>
 *     <li>{@code i}: the sequence number within an epoch. {@code i} restarts from 0 at the beginning of a new epoch.</li>
 * </ul>
 */
public class CSUpdateKey implements Serializable, Comparable<CSUpdateKey> {
    /** The epoch in which the update was processed. */
    public final int epoch;
    /** The sequence number within an epoch. */
    public final int seq_no;
    
    public CSUpdateKey(int epoch, int seq_no) {
        this.epoch = epoch;
        this.seq_no = seq_no;
    }
    
    /**
     * Copy constructor for update keys.
     *
     * @param k The key of which a deep copy is needed.
     */
    public CSUpdateKey(CSUpdateKey k) {
        this(k.epoch, k.seq_no);
    }
    
    @Override
    public String toString() {
        return "[" + epoch + ", " + seq_no + "]";
    }
    
    @Override
    public int compareTo(CSUpdateKey other) {
        int cmp = Integer.compare(this.epoch, other.epoch);
        if (cmp != 0) {
            return cmp;
        }
        return Integer.compare(this.seq_no, other.seq_no);
    }
}