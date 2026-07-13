package it.unitn.ds.cs.messages.client;

import it.unitn.ds.cs.CSAsk;

import java.io.Serializable;

public class CSWriteRequest extends CSAsk implements Serializable {
    public final int index;
    public final int value;
    
    public CSWriteRequest(int index, int value) {
        super();
        this.index = index;
        this.value = value;
    }
}
