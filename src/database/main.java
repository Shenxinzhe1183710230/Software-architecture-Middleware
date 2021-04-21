package database;

import java.sql.ResultSet;
import java.sql.SQLException;

public class main {
    public static void main(String[] args) throws SQLException {
        DBBean dbBean=new DBBean();
//        dbBean.executeQuery("customermanager(Name,PhoneNum)","'wkr','123456'");
//        dbBean.executeQuery("customermanager(Name,PhoneNum)","'sxz','222222'");
//        dbBean.executeQuery("customermanager(Name,PhoneNum)","'yqq','333333'");
//        dbBean.executeQuery("customermanager(Name,PhoneNum)","'yzj','444444'");
//        dbBean.executeQuery("customermanager(Name,PhoneNum)","'dzh','555555'");
//        dbBean.executeQuery("customermanager(Name,PhoneNum)","'yzj','444444'");
//        dbBean.executeQuery("itemmanager(Name,OutPrice,Num,InPrice)","'car',100000,1,50000");
//        dbBean.executeQuery("itemmanager(Name,OutPrice,Num,InPrice)","'banana',15.1,10,5.5");
//        dbBean.executeQuery("itemmanager(Name,OutPrice,Num,InPrice)","'apple',3.2,50,1.2");
//        dbBean.executeQuery("itemmanager(Name,OutPrice,Num,InPrice)","'bowl',8.8,15,4.4");
        //dbBean.executeDelete("wkr","customermanager","Name");
        //dbBean.executeUpdate("'wkr'","nameandpassword",
        //       "user_name","654321","user_password");
        //dbBean.executeDelete("yzj","nameandpassword","user_name");
        //dbBean.executeQuery("nameandpassword(user_name,user_password,age)","'yzj','654321',10");
        //ResultSet resultSet=dbBean.executeQuery("wkr","nameandpassword","user_name");
//        String password="1234567";
//        dbBean.executeCreateNewTable("repository1","repository");
//        dbBean.executeCreateNewTable("repository1_order","order");
//        dbBean.executeCreateNewTable("repository1_item_order","item_order");
//        dbBean.executeDeleteTable("item1");
        ResultSet resultSet=null;
//        Vector<Vector<Object>> res = new Vector<Vector<Object>>();returnVector.FromDBRead(dbBean,"itemmanager","sxz","name");
//        resultSet=dbBean.executeTablehead("itemmanager");
        //System.out.println(returnVector.getHeadName(dbBean,"ordermanager"));
//        System.out.println(op.TotalProfit(returnVector.FromDBReadAll(dbBean,"ordermanager",returnVector.getHeadName(dbBean,"ordermanager")),dbBean));
        int x=dbBean.executeQuery("用户","'4444','sxz3','sxz3','1999-05-05','harbin','123456'");
        System.out.println(x);
//       resultSet=dbBean.executeFind("sxz","用户","昵称");
//        while (resultSet.next())
//        {
//            System.out.println(resultSet.getString(1));
//
//        }
    }
}