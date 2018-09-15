package jinke.com.kylinOperation;

import java.sql.*;
import java.util.Properties;

/**
 * Created by tanghanzhuang on 2018/8/6
 */
public class KylinJDBCTest {
    private Driver kylin_driver;
    private Properties kylin_info;
    private Connection kylin_conn;
    private Statement kylin_state;
    private ResultSet kylin_resultSet;

    public void testConnected() throws ClassNotFoundException, IllegalAccessException, InstantiationException, SQLException {
        kylin_driver = (Driver) Class.forName("org.apache.kylin.jdbc.Driver").newInstance();
        kylin_info = new Properties();
        kylin_info.put("user","ADMIN");
        kylin_info.put("password","KYLIN");
        kylin_conn = kylin_driver.connect("jdbc:kylin://172.16.11.80:7070/douyin_fans_test",kylin_info);
        kylin_state = kylin_conn.createStatement();
        kylin_resultSet = kylin_state.executeQuery("select count(short_id),age_grades from DOUYIN_FANS group by age_grades;");
        while (kylin_resultSet.next()) {
/*            System.out.println(kylin_resultSet.getString(1));
            System.out.println(kylin_resultSet.getString(2));
            System.out.println(kylin_resultSet.getString(3));
            System.out.println(kylin_resultSet.getString(4));*/
        }
        kylin_conn.close();
    }

}
