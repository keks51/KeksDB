package com.keks.kv_storage.bplus.utils;

//import org.apache.log4j.FileAppender;


//public class CustomFileAppender extends  FileAppender{
//
//    @Override
//    public void setFile(String fileName)
//    {
//        if (fileName.contains("%timestamp")) {
//            Date d = new Date();
//            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SS");
//            fileName = fileName.replaceAll("%timestamp", format.format(d));
//        }
//        super.setFile(fileName);
//    }
//
//}