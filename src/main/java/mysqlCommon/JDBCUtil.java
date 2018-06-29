package mysqlCommon;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.Properties;

/**
 * Created by probdViewSonic on 2017/5/16.
 */
public class JDBCUtil {

    static String username;
    static String password;
    static String url;
    static String driver;
    static Properties properties=new Properties();
    public static Connection getConnection(){
            try {
                properties.load(new FileReader("/data/db.properties"));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }

            username = properties.getProperty("username");
            password = properties.getProperty("password");
            url = properties.getProperty("url");
            driver = properties.getProperty("driver");
            Connection  connection=null;
        try {
            Class.forName(driver);

            connection = DriverManager.getConnection(url, username, password);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
            return connection;
        }

      /**
       *  删除
       */
    public static void delete(Connection conn,String table,String type,String name) {
        PreparedStatement pst;
        try {
            String sql = "delete from "+table+" where type=? and name=?";
            pst = conn.prepareStatement(sql);
            pst.setString(1, type.trim());
            pst.setString(2, name.trim());
            pst.execute();
            pst.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     *  插入数据
     */
    public static void insert(Connection conn,String table,String type,String name,String value){
        PreparedStatement pst;
        try {
            String sql = "insert into "+table+"(type,name,value) values(?,?,?)";

            pst =conn.prepareStatement(sql);
            pst.setString(1, type.trim());
            pst.setString(2, name.trim());
            pst.setString(3, value.trim());

            pst.execute();
            pst.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
      }

     /**
      *   查询
      */
    public static String query(Connection conn,String table,String type,String name) {
            String value=null;
            ResultSet rs;
            PreparedStatement pst;
        try {
            String sql = "select value from "+table+" where type=? and name=? order by id desc";
            pst = conn.prepareStatement(sql);
            pst.setString(1,type.trim());
            pst.setString(2,name.trim());
            rs=pst.executeQuery();
            if (rs.next()) {
              value = rs.getString("value");
            }
            rs.close();
            pst.close();
        } catch (SQLException e) {
            System.out.println("query error");
        }
            return value;
       }

    /**
     *  更新
     * @param
     */
    public static void update(Connection conn,String table,String type,String name,String value){
        String sql="update "+table+" set value=? where type=? and name=?";
        PreparedStatement pst;
         try{
             pst =conn.prepareStatement(sql);
             pst.setString(1,value.trim());
             pst.setString(2,type.trim());
             pst.setString(3,name.trim());
             pst.executeUpdate();
             pst.close();
         }
        catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     *  是否存在
     */
    public static boolean ifExists(Connection conn,String table,String type,String name){
        ResultSet rs;
        PreparedStatement pst;
        boolean flag=true;
        try {
            String sql = "select 1 from "+table+" where type=? and name=? limit 1";
            pst = conn.prepareStatement(sql);
            pst.setString(1,type.trim());
            pst.setString(2,name.trim());
            rs=pst.executeQuery();
            if (rs.next())  flag=true;
            else flag=false;
            rs.close();
            pst.close();
        } catch (SQLException e) {
            System.out.println("if exists error");
        }
        return  flag;
     }


    /**
     * close
     * @param
     */
   public static void  close(Connection conn){
       try {
           if (conn!=null) conn.close();
       } catch (SQLException e) {
           e.printStackTrace();
       }
    }

}
