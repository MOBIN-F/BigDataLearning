//package com.mobin.Example;
//
//import org.apache.hadoop.hive.ql.metadata.Hive;
//
//import java.sql.Connection;
//import java.sql.DriverManager;
//import java.sql.SQLException;
//
///**
// * Created by MOBIN on 2016/9/21.
// */
//public class HiveDataBaseConnection {
//    private final static String DriverName = "org.apache.hive.jdbc.HiveDriver";
//    private final static String URL = "jdbc:hive2://132.122.70.2:10000/default";
//    private final static String UserName = "";
//    private final static String Password = "";
//    private Connection con;
//
//    public HiveDataBaseConnection(){
//        try {
//            Class.forName(DriverName);
//            con = DriverManager.getConnection(URL,UserName, Password);
//            System.out.println(con);
//        } catch (ClassNotFoundException e) {
//            e.printStackTrace();
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//    }
//
//    public Connection getConnection(){
//        return con;
//    }
//
//    public void Close(){
//            try {
//                if(con != null)
//                  con.close();
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//    }
//
//    public static void main(String[] args) {
//      HiveDataBaseConnection connection = new HiveDataBaseConnection();
//    }
//}
