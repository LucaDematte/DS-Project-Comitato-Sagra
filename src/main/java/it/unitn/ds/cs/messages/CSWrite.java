package it.unitn.ds.cs.messages;

import java.io.Serializable;

public class CSWrite implements Serializable {
    int index;
    int value;

    public CSWrite(int index, int value) {
        this.index = index;
        this.value = value;
    }
}
