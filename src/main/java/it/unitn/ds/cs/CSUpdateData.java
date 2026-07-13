package it.unitn.ds.cs;

public class CSUpdateData {
    public final int index;
    public final int value;
    public final boolean completed;
    
    public CSUpdateData(int index, int value, boolean completed) {
        this.index = index;
        this.value = value;
        this.completed = completed;
    }
}
