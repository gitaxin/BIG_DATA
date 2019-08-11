package jdbcTest;

import java.sql.*;

/**
 *
 * Mysql 8.0.15 版本驱动连接数据库测试
 * @Description:
 * @Author: Axin
 * @Date: Create in 16:59 2019/8/11
 */
public class Test {

    static final String USER = "root";
    static final String PASSWORD = "000000";
    static final String URL = "jdbc:mysql://bigdata111:3306/jdbc_test?useSSL=false&serverTimezone=UTC&useUnicode=true&characterEncoding=utf-8";

    /**
     * 5.*和8.*有区别
     * 5.* ：com.mysql.jdbc.Driver
     * 8.*：com.mysql.cj.jdbc.Driver
     */
     static final String DRIVER = "com.mysql.cj.jdbc.Driver";

    public static void main(String[] args) {

        Connection connection = null;
        Statement statement = null;
        ResultSet result = null;
        try {
            Class.forName(DRIVER);
             connection = DriverManager.getConnection(URL, USER, PASSWORD);
             statement = connection.createStatement();

             result = statement.executeQuery("select * from user;");

            while (result.next()){
                int id = result.getInt("id");
                String username = result.getString("username");
                System.out.println(id + " = " + username);


            }
             String sql = "insert into user values(10,'测试插入')";

            boolean execute = statement.execute(sql);
            System.out.println(execute);




        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }finally{
            if(result != null){
                try {
                    result.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if(statement != null){
                try {
                    statement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

            if(connection != null){
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }


            }

        }



    }
}
