package com.neotech.worker;

import com.neotech.domain.TimeLog;

import java.sql.Timestamp;
import java.util.Queue;

public class CollectTimestampWorker extends Thread{
    private Queue<TimeLog> timeLogs;
    private final long collectTsPauseSec = 1000L;


    public CollectTimestampWorker(Queue<TimeLog> timeLogs) {
        this.timeLogs = timeLogs;
    }

    @Override
    public void run() {
        while (true){
            try {
                TimeLog timeLog = new TimeLog();
                timeLog.setTimestamp(new Timestamp(System.currentTimeMillis()));
                timeLogs.add(timeLog);
                Thread.sleep(collectTsPauseSec);
            } catch (InterruptedException e) {
                System.err.println(e.getMessage());
            }
        }
    }
}
