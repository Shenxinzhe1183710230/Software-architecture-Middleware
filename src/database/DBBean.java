package database;
import java.sql.*;


public class DBBean {
    private String driverStr = "com.mysql.jdbc.Driver";
//    private String connStr = "jdbc:mysql://localhost:3306/homework?serverTimezone=UTC";
    private String connStr = "jdbc:mysql://cdb-fugoxito.cd.tencentcdb.com:10200/sxz_database_lab3?serverTimezone=UTC";
    private String dbusername = "root";
    private String dbpassword = "zhege00++";
    private Connection conn = null;
    private Statement stmt = null;
    private String database_name="sxz_database_lab3";

    public DBBean()
    {
        try
        {
            Class.forName(driverStr);
            conn = DriverManager.getConnection(connStr, dbusername, dbpassword);
            stmt = conn.createStatement();
            System.out.println("数据连接成功！");
        }
        catch (Exception ex) {
            System.out.println(ex);
            System.out.println("数据连接失败！");
        }

    }

    /**
     *删除这个表
     * @param tableName  表的名字
     * @return
     */
    public int executeDeleteTable(String tableName) {
        int result = 0;
        String sql = null;
        sql="drop table "+tableName;
        System.out.println("--删除表:"+sql+"\n");
        try {
            result = stmt.executeUpdate(sql);
        } catch (Exception ex) {
            System.out.println(ex);
            System.out.println("执行创建/删除表错误！");
        }
        return result;
    }


    /**
     *
     * @param tableName   要创建的表的名字
     * @param type   创建仓库表，还是订单表  创建仓库输入repository   创建订单输入order
     * @return
     */
    public int executeCreateNewTable(String tableName, String type) {
        int result = 0;
        String sql = null;
        if(type.equals("order")){
            sql = "create table " + tableName +
                    "(id int NOT NULL AUTO_INCREMENT PRIMARY KEY, " +
                    "name varchar(100) NOT NULL UNIQUE KEY" +
                    ",state varchar(100) NOT NULL" +
                    ",orders varchar(100) NOT NULL"+
                    ",price_all float,num int NOT NULL" +
                    ",profit float NOT NULL)";
        }else if(type.equals("repository")) {
            sql = "create table " + tableName +
                    "(id int NOT NULL AUTO_INCREMENT PRIMARY KEY, " +
                    "name varchar(100) NOT NULL UNIQUE KEY" +
                    ",outprice float NOT NULL" +
                    ",inprice float NOT NULL," +
                    "num int NOT NULL," +
                    "outprice_wholesale float NOT NULL)";
        }
        else if(type.equals("item_order")){
            sql = "create table " + tableName +
                    "(order_id int NOT NULL" +
                    ",product_name varchar(100) NOT NULL" +
                    ",num int NOT NULL)";
        }
        System.out.println("--创建新的表:"+sql+"\n");
        try {
            result = stmt.executeUpdate(sql);
        } catch (Exception ex) {
            System.out.println(ex);
            System.out.println("执行创建表错误！");
        }
        return result;
    }

    /**
     *删除数据库中数据
     * @param value      想要删除的那一列的哪个值
     * @param tableName     想要查询的数据库的表名
     * @param index      想要查询数据库的表的哪一列的名字
     * @return   返回查询到的所有行
     *  eg:   dbBean.executeDelete("wkr","nameandpassword","user_name");
     */
    public int executeDelete(String value,String tableName,String index){
        int rs = 0;
        String sql="delete from "+tableName+" where "+index+"="+"'"+value+"'";//定义一个删除语句
        System.out.print("--删除语句:"+sql+"\n");
        try {
            rs = stmt.executeUpdate(sql);
        } catch (Exception ex) {
            System.out.println(ex);
            System.out.println("ִ执行删除错误！");
        }
        return rs;
    }

    /**
     * 更新数据库中的数据（如果值为字符串类型，需要加单引号‘’）
     * @param index_value   查询那一列的值
     * @param tableName     想要更新的表名
     * @param index         查询哪一列
     * @param target_value  想要更改后的值。如果值为字符串类型，需要加单引号‘’
     * @param target_index  想要修改表单的哪一列值
     * @return
     * dbBean.executeUpdate(" ' wkr ' ", " nameandpassword ",
     *                 "user_name","654321","user_password");
     */
    public int executeUpdate(String index_value,String tableName,String index,String target_value,String target_index) {
        int result = 0;
        String sql="update "+tableName+" set "+target_index+"="+target_value+" where "+index+"="+index_value;
        System.out.println("--更新语句:"+sql+"\n");
        try {
            result = stmt.executeUpdate(sql);
        } catch (Exception ex) {
            System.out.println(ex);
            System.out.println("执行更新错误！");
        }
        return result;
    }


    /**
     *查询数据库中数据
     * @param value      想要查询的值
     * @param tableName     想要查询的数据库的表名
     * @param index      想要查询数据库的表的哪一列的名字
     * @return   返回查询到的所有行
     *  eg:   dbBean.executeQuery("wkr","nameandpassword","user_name");
     */
    public ResultSet executeFind(String value, String tableName, String index) {
        ResultSet rs = null;
        String sql="select * from "+tableName+" where "+index+"="+"'"+value+"'";//定义一个查询语句
        System.out.print("--查询语句:"+sql+"\n");
        try {
            rs = stmt.executeQuery(sql);
            System.out.println(rs);
        } catch (Exception ex) {
            System.out.println(ex);
            System.out.println("ִ执行查询错误！");
        }
        return rs;
    }


    /**
     *查询数据库中全部数据
     * @param tableName     想要查询的数据库的表名
     * @return   返回查询到的所有行
     *  eg:   dbBean.executeQuery("wkr","nameandpassword","user_name");
     */
    public ResultSet executeFindAll(String tableName) {
        ResultSet rs = null;
        String sql="select * from "+tableName;
        System.out.print("--全部查询语句:"+sql+"\n");
        try {
            rs = stmt.executeQuery(sql);
            System.out.println(rs);
        } catch (Exception ex) {
            System.out.println(ex);
            System.out.println("ִ执行查询错误！");
        }
        return rs;
    }


    /**
     *
     * @param table_name 数据库的表名  eg:itemmanager
     * @return 返回所有表头字段。ResultSet
     */
    public ResultSet executeTablehead(String table_name){
        ResultSet resultSet=null;
        String sql="SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA " +
                "= 'homework' AND TABLE_NAME = '"+table_name+"'";
        System.out.println(sql);
        try {
            resultSet=stmt.executeQuery(sql);
        } catch (SQLException e) {
            System.out.println(e);
            System.out.println("查询表头错误！");
        }
        return resultSet;
    }

    /**
     *向数据库中插入一个数据
     * @param table_name    数据库的表名及参数名   eg:table(id,name,age)
     * @param value         要传入的值   字符串需要打单引号          eg:1,'sxz',20
     * dbBean.execQuery("nameandpassword(user_name,user_password,age)","'yzj','654321',10");
     */
    public int executeQuery(String table_name, String value){
        String sql="insert into "+database_name+"."+table_name +" values"+"("+value+")";//定义一个插入语句
        int x=0;
        System.out.println(sql);
        try {
            x=stmt.executeUpdate(sql);
            System.out.println(x);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            System.out.println(e);
            System.out.println("执行插入错误！");
        }
        return x;
    }

    public void close() {
        try {
            stmt.close();
            conn.close();
        } catch (Exception e) {
        }
    }

    /**
     *    返回tablename表的最大ID  也就是最新添加的一行数据
     * @param tableName   想要查询的数据库的表名
     * @param type   输入”id“表示ID列的字段为小写id,输入”ID”表示ID列的字段为大写ID
     * @return  返回查询到的行
     * eg:       resultSet=dbBean.executeFindMAXID("customermanager","id");
     */
    public ResultSet executeFindMAXID(String tableName, String type) {
        ResultSet rs = null;
        String id;
        if(type.equals("id")){
            id="id";
        }else {
            id="ID";
        }
        String sql="select * from "+tableName+" where "+id+" = (SELECT max("+id+") FROM "+tableName+")";//定义一个查询语句
        System.out.print("--查询最大ID语句:"+sql+"\n");
        try {
            rs = stmt.executeQuery(sql);
            System.out.println(rs);
        } catch (Exception ex) {
            System.out.println(ex);
            System.out.println("ִ执行查询最大ID错误！");
        }
        return rs;
    }
}