package it.unitn.ds.cs.messages;

import it.unitn.ds.cs.CSUpdateKey;

import java.io.Serializable;

public class CSWriteOk implements Serializable {
    public final CSUpdateKey key;

    public CSWriteOk(CSUpdateKey key) {
        this.key = key;
    }
}
