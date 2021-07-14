package org.example;

public class TimeUtil {
    private long curTime;

    public TimeUtil() {
        this.curTime = System.currentTimeMillis();
    }

    public long getTimeAndReset() {
        return System.currentTimeMillis() - this.curTime;
    }
}

