package it.unitn.ds.cs;

/**
 * epoch  <- e
 * seq_no <- i
 */
public class CSUpdateKey {
    public final int epoch;
    public final int seq_no;
    
    public CSUpdateKey(int epoch, int seq_no) {
        this.epoch = epoch;
        this.seq_no = seq_no;
    }
    
    public CSUpdateKey(CSUpdateKey k) {
        this(k.epoch, k.seq_no);
    }
    
    @Override
    public String toString() {
        return "[" + epoch + ", " + seq_no + "]";
    }
}