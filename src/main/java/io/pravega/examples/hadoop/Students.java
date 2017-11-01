package io.pravega.examples.hadoop;

import java.io.Serializable;

public class Students implements Serializable {
    public String First;
    public String Second;

    public Students(String s1, String s2) {
        First = s1;
        Second = s2;
    }
}
