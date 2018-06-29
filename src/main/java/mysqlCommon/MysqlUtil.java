package mysqlCommon;

import java.sql.Connection;

/**
 * Created by probdViewSonic on 2017/5/17.
 */
public class MysqlUtil {
    public String table;
    //mysql.paramlib

    public MysqlUtil(String table) {
        this.table = table;
    }
    public MysqlUtil() {}
    /**
     *  覆盖或者 新增
     * @param type
     * @param name
     * @param value
     */
    public void  overWriteOrAppend(String type,String name,String value){
        // 1
        Connection conn=JDBCUtil.getConnection();
        // 2
        Boolean flag=JDBCUtil.ifExists(conn, table, type, name);
        System.out.println("flag =" + flag);
        if (flag){ //true
            JDBCUtil.update(conn, table,type, name,value);
        }else{// false
            JDBCUtil.insert(conn, table,type, name,value);
        }
        JDBCUtil.close(conn);
    }

    /** 查询
     * query value
     * @param type
     * @param name
     */
    public String queryValue(String type,String name){
        //1
        Connection conn=JDBCUtil.getConnection();
        //2
       String value= JDBCUtil.query(conn,table, type, name);
        JDBCUtil.close(conn);
        return value;
    }

    /**
     *  删除
     * @param type
     * @param name
     */
    public  void deleteValue(String type,String name){
        //1
        Connection conn=JDBCUtil.getConnection();
        //2
        JDBCUtil.delete(conn,table, type, name);
        JDBCUtil.close(conn);
    }

}
