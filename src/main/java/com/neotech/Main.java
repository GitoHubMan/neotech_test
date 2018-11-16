package com.neotech;


import com.neotech.worker.DBWorker;

public class Main {

    public static void main(String[] args){
        DBWorker dbWorker = new DBWorker(args);
        dbWorker.start();
    }


}
