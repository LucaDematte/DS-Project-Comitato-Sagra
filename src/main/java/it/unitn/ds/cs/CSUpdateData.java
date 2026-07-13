package it.unitn.ds.cs;

public class UpdateData {
    public final int index;
    public final int value;
    public boolean completed;
    
    public UpdateData(int index, int value, boolean completed) {
        this.index = index;
        this.value = value;
        this.completed = completed;
    }
    
    public UpdateData(int index, int value) {
        this.index = index;
        this.value = value;
        this.completed = false;
    }
}
