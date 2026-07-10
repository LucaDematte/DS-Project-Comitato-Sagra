package it.unitn.ds.cs;

/**
 * @param epoch  e
 * @param seq_no i
 */
public record CSUpdateKey(int epoch, int seq_no) {
    public CSUpdateKey(CSUpdateKey k) {
        this(k.epoch, k.seq_no);
    }
}