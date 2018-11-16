package com.neotech.worker;

import com.neotech.domain.TimeLog;
import com.neotech.util.DBHelper;
import com.neotech.util.PersistenceManager;
import org.apache.log4j.Logger;

import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;
import javax.persistence.Query;
import java.sql.*;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;


public class DBWorker extends Thread {
    private final String param;
    private EntityManager em;
    private static final Logger logger = Logger.getLogger(DBWorker.class);
    private final long dbErrorPauseSec = 5000L;


    
    public DBWorker(String[] params) {
        param = params != null && params.length > 0? params[0].equals(DBHelper.INSERT_PARAM_NAME)? params[0] : null : null;
        em = PersistenceManager.INSTANCE.getEntityManager();
    }


    @Override
    public void run() {
        if(param == null){
            insertTimestamps();
        } else {
            printAllTimestamps();
        }
    }

    private void insertTimestamps(){
        //start separate thread to collect timestamps
        Queue<TimeLog> timeLogs = new ConcurrentLinkedQueue<>();
        CollectTimestampWorker tsWorker = new CollectTimestampWorker(timeLogs);
        tsWorker.start();

        //wait until there will be atleast one timestamp in queue
        while (timeLogs.isEmpty()) {
            try {
                Thread.sleep(100L);
            } catch (InterruptedException e) {
                System.err.println(e.getMessage());
            }
        }

        //start to insert timestamps that separate thread collects
        while (true) {
            EntityTransaction transaction = em.getTransaction();
            try{
                LinkedList<TimeLog> tmp = new LinkedList<>(timeLogs);
                if(tmp.size() > 0) {
                    transaction.begin();
                    
                    for (TimeLog tl : tmp) {
                        em.persist(tl);
                    }
                    transaction.commit();
                    em.clear();
                    
                    logger.info("Successfully inserted rows: " + tmp.size());
                    for(int i=0; i<tmp.size(); i++)
                        timeLogs.remove();
                }
            } catch (Exception e){
                logger.error("Insert time log failed. Error: ", e);
                if (transaction.isActive()) {
                    transaction.rollback();
                    em.clear();
                }
                try {
                    em = PersistenceManager.INSTANCE.getEntityManager();
                } catch (Exception ex){
                    logger.error("Can't init new EntityManager. Error: ", e);
                }
            }
        }
    }

    private void printAllTimestamps(){
        while (true) {
            try{
                Query q = em.createQuery("select t from TimeLog t");
                List<TimeLog> timeLogs = q.getResultList();
                for (TimeLog timeLog : timeLogs) {
                    logger.info(timeLog.getTimestamp());
                }
                em.close();
                PersistenceManager.INSTANCE.close();
                break;
            } catch (Exception e){
                logger.error("Print time log failed. Error: ", e);
            }
        }
    }
}
