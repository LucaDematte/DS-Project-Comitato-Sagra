package it.unitn.ds.cs.messages;

import java.io.Serializable;

public class CSReadRequest implements Serializable {
    public final int index;

    public CSReadRequest(int index) {
        this.index = index;
    }
}
