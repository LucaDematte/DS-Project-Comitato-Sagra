package it.unitn.ds.cs.messages;

import java.io.Serializable;

public class CSRead implements Serializable {
    public final int index;

    public CSRead(int index) {
        this.index = index;
    }
}
