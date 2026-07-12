package it.unitn.ds.cs.messages.client;

import java.io.Serializable;

public class CSWriteRequest implements Serializable {
    public final int index;
    public final int value;
    
    public CSWriteRequest(int index, int value) {
        this.index = index;
        this.value = value;
    }
}
