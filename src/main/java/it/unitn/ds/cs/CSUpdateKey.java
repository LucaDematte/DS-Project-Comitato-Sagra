package it.unitn.ds.cs;

import java.io.Serializable;
import java.util.Objects;

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
    public final int seqNo;
    
    public CSUpdateKey(int epoch, int seqNo) {
        this.epoch = epoch;
        this.seqNo = seqNo;
    }
    
    /**
     * Copy constructor for update keys.
     *
     * @param k The key of which a deep copy is needed.
     */
    public CSUpdateKey(CSUpdateKey k) {
        this(k.epoch, k.seqNo);
    }
    
    @Override
    public String toString() {
        return "[" + epoch + ", " + seqNo + "]";
    }
    
    @Override
    public int compareTo(CSUpdateKey other) {
        int cmp = Integer.compare(this.epoch, other.epoch);
        if (cmp != 0) {
            return cmp;
        }
        return Integer.compare(this.seqNo, other.seqNo);
    }
    
    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass())
            return false;
        CSUpdateKey that = (CSUpdateKey) o;
        return epoch == that.epoch && seqNo == that.seqNo;
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(epoch, seqNo);
    }
}