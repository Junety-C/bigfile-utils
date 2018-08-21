package cn.junety.tools.bigfile.utils;

import java.util.concurrent.TimeUnit;

/**
 * Created by caijt on 2018/8/20
 */
public class TimeMeter {

    private long startTime;
    private long used;
    private boolean running;

    public TimeMeter() {
        this.startTime = System.currentTimeMillis();
        this.used = 0;
        this.running = true;
    }

    public TimeMeter reset() {
        this.startTime = System.currentTimeMillis();
        this.used = 0;
        return this;
    }

    public TimeMeter start() {
        startTime = System.currentTimeMillis();
        running = true;
        return this;
    }

    public TimeMeter stop() {
        used += getCost();
        running = false;
        return this;
    }

    public long getUsed() {
        return getUsed(TimeUnit.MILLISECONDS);
    }

    public long getUsed(TimeUnit unit) {
        return getCost() / unit.toMillis(1);
    }

    private long getCost() {
        return used + (running ? System.currentTimeMillis() - startTime : 0);
    }
}
