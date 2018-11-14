package com.neotech;

import com.neotech.worker.DBWorker;



public class Main {

    public static void main(String[] args){
        DBWorker dbWorker = new DBWorker(args != null && args.length > 0? args[0] : null);
        dbWorker.start();
    }


}
