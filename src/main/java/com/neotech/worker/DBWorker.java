package com.neotech.worker;

import com.neotech.util.DBHelper;

import java.sql.*;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;


public class DBWorker extends Thread {
    private final String param;
    private final String url;
    private final String user;
    private final String password;
    private final long dbErrorPauseSec = 5000L;
    private String insertSql = "INSERT INTO %s(timestamp) VALUES(?)";
    private String selectSql = "SELECT timestamp FROM %s order by id asc";

    
    public DBWorker(String param) {
        param = param != null? param.equals(DBHelper.INSERT_PARAM_NAME)? param : null : null;
        this.param = param;
        DBHelper dbHelper = new DBHelper();

        Properties props = dbHelper.getProperties();
        url = props.getProperty(DBHelper.URL_PROP_NAME);
        user = props.getProperty(DBHelper.USER_PROP_NAME);
        password = props.getProperty(DBHelper.PASSWORD_PROP_NAME);
        insertSql = String.format(insertSql, props.getProperty(DBHelper.TABLE_PROP_NAME));
        selectSql = String.format(selectSql, props.getProperty(DBHelper.TABLE_PROP_NAME));
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
        Queue<Timestamp> timestamps = new ConcurrentLinkedQueue<>();
        CollectTimestampWorker tsWorker = new CollectTimestampWorker(timestamps);
        tsWorker.start();

        //wait until there will be atleast one timestamp in queue
        while (timestamps.isEmpty()) {
            try {
                Thread.sleep(100L);
            } catch (InterruptedException e) {
                System.err.println(e.getMessage());
            }
        }

        //start to insert timestamps that separate thread collects
        while (true) {
            try (Connection con = DriverManager.getConnection(url, user, password)){
                try (Statement st = con.createStatement()) {
                    con.setAutoCommit(false);

                    for (Timestamp ts; (ts = timestamps.poll()) != null;){
                        st.addBatch(insertSql.replace("?", "'" + ts.toString() + "'"));
                    }

                    st.executeBatch();
                    con.commit();
                    System.out.println("Records inserted successfully");

                } catch (SQLException ex) {
                    System.err.println(ex.getMessage());
                    try {
                        con.rollback();
                    } catch (SQLException ex2) {
                        System.err.println(ex2.getMessage());
                    }
                    Thread.sleep(dbErrorPauseSec);
                }
            } catch (Exception ex) {
                System.err.println(ex.getMessage());
                try {
                    Thread.sleep(dbErrorPauseSec);
                } catch (InterruptedException e) {
                    System.err.println(ex.getMessage());
                }
            }
        }
    }

    private void printAllTimestamps(){
        while (true) {
            try (Connection con = DriverManager.getConnection(url, user, password);
                 Statement st = con.createStatement();
                 ResultSet rs = st.executeQuery(selectSql)) {

                while (rs.next()) {
                    System.out.println(rs.getString(1));
                }
                break;
            } catch (SQLException ex) {
                System.err.println(ex.getMessage());
                try {
                    Thread.sleep(dbErrorPauseSec);
                } catch (InterruptedException e) {
                    System.err.println(ex.getMessage());
                }
            }
        }
    }

    private class CollectTimestampWorker extends Thread{
        private Queue<Timestamp> timestamps;
        private final long collectTsPauseSec = 1000L;
        

        public CollectTimestampWorker(Queue<Timestamp> timestamps) {
            this.timestamps = timestamps;
        }

        @Override
        public void run() {
            while (true){
                try {
                    Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                    timestamps.add(timestamp);
                    Thread.sleep(collectTsPauseSec);
                } catch (InterruptedException e) {
                    System.err.println(e.getMessage());
                }
            }
        }
    }
}
