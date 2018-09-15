package jinke.com.kylinOperation;

/*
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
*/

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.PreparedStatement;
import jinke.com.entity.MysqlUserEntity;

import java.sql.*;
import java.text.DecimalFormat;
import java.time.LocalDate;
import java.util.*;

/**
 * Created by tanghanzhuang on 2018/8/14
 */
public class KylinToMysqlDuoZuHu {
    private java.sql.Connection getKylinConn() throws SQLException, ClassNotFoundException, IllegalAccessException, InstantiationException {
        Driver kylin_driver = (Driver) Class.forName("org.apache.kylin.jdbc.Driver").newInstance();

        Properties kylin_info = new Properties();
        String a;
        kylin_info.put("user", "ADMIN");
        kylin_info.put("password", "KYLIN");
        java.sql.Connection conn = kylin_driver.connect("jdbc:kylin://172.18.245.121:7070/mission_bigdata", kylin_info);
        return conn;
    }

    //TODO 还需要区分租户数据库
    private Connection getMysqlConn(MysqlUserEntity mysqlUserEntity) {
        String driver = "com.mysql.jdbc.Driver";
        String url = mysqlUserEntity.getUrl();
        String username = mysqlUserEntity.getUsername();
        String password = mysqlUserEntity.getPassword();
        Connection conn = null;

        try {
            Class.forName(driver);
            conn = (Connection) DriverManager.getConnection(url, username, password);
            String b;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }


    public void prepareData() throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
        MysqlUserEntity mysqlUserEntity = new MysqlUserEntity();
        mysqlUserEntity.setUrl("jdbc:mysql://172.16.100.32:3306/mission2?useUnicode=true&characterEncoding=utf-8");
        mysqlUserEntity.setUsername("qiji");
        mysqlUserEntity.setPassword("qiji!@#2018");

        java.sql.Connection kylin_conn = getKylinConn();
        Connection mysql_conn = getMysqlConn(mysqlUserEntity);

        Statement kylin_state = kylin_conn.createStatement();
        Statement mysql_state = mysql_conn.createStatement();
        ResultSet kylin_resultSet;
        ResultSet mysql_resultSet;
        PreparedStatement pstm = null;

        String sql = "select username,password,url,tenant_type,domain_name from s_tenant;";

        mysql_resultSet = mysql_state.executeQuery(sql);
        List<MysqlUserEntity> userEntities = new ArrayList<>();
        while (mysql_resultSet.next()){
            MysqlUserEntity mysqlUserEntity1 ;
        }


        LocalDate now = LocalDate.now();
        String dt;
        DecimalFormat df = new DecimalFormat("00");
        if (now.getDayOfMonth() < 16) {
            dt = now.getYear() + "-" + df.format(now.getMonthValue()) + "-01";
        } else {
            dt = now.getYear() + "-" + df.format(now.getMonthValue()) + "-15";
        }
        String getGenderSql = "select count(DISTINCT  DOUYIN_FANS.UID ),DOUYIN_FANS.GENDER ,DOUYIN_FANS.FOLLOWING_ID,DOUYIN_FANS.DT,DOUYIN_FANS.TENANT_ID  from mission.DOUYIN_FANS where DOUYIN_FANS.DT ='" + dt + "' group by DOUYIN_FANS.TENANT_ID,DOUYIN_FANS.GENDER , DOUYIN_FANS.FOLLOWING_ID,DOUYIN_FANS.DT;";
        String getAgeGradesSql = "select count(DISTINCT  DOUYIN_FANS.UID ),DOUYIN_FANS.AGE_GRADES  ,DOUYIN_FANS.FOLLOWING_ID,DOUYIN_FANS.DT  from mission.DOUYIN_FANS where DOUYIN_FANS.DT ='" + dt + "' group by DOUYIN_FANS.TENANT_ID,DOUYIN_FANS.AGE_GRADES  , DOUYIN_FANS.FOLLOWING_ID,DOUYIN_FANS.DT;";
        String getConstellation = "select count(DISTINCT  DOUYIN_FANS.UID ), DOUYIN_FANS.CONSTELLATION ,DOUYIN_FANS.FOLLOWING_ID,DOUYIN_FANS.DT  from mission.DOUYIN_FANS where DOUYIN_FANS.DT ='" + dt + "' group by DOUYIN_FANS.TENANT_ID,DOUYIN_FANS.CONSTELLATION , DOUYIN_FANS.FOLLOWING_ID,DOUYIN_FANS.DT;";
        HashMap<String, Map<String, String>> genderMap = new HashMap();
        HashMap<String, Map<String, String>> ageMap = new HashMap();
        HashMap<String, Map<String, String>> conMap = new HashMap();

        kylin_resultSet = kylin_state.executeQuery(getGenderSql);
        while (kylin_resultSet.next()) {
            Map<String, String> map = null != genderMap.get(kylin_resultSet.getString(3)) ? genderMap.get(kylin_resultSet.getString(3)) : new HashMap<>();
            map.put(kylin_resultSet.getString(2), kylin_resultSet.getString(1));
            genderMap.put(kylin_resultSet.getString(3), map);
        }
        kylin_resultSet = kylin_state.executeQuery(getAgeGradesSql);
        while (kylin_resultSet.next()) {
            Map<String, String> map = null != ageMap.get(kylin_resultSet.getString(3)) ? ageMap.get(kylin_resultSet.getString(3)) : new HashMap<>();

            map.put(kylin_resultSet.getString(2), kylin_resultSet.getString(1));
            ageMap.put(kylin_resultSet.getString(3), map);
        }
        kylin_resultSet = kylin_state.executeQuery(getConstellation);
        while (kylin_resultSet.next()) {
            Map<String, String> map = null != conMap.get(kylin_resultSet.getString(3)) ? conMap.get(kylin_resultSet.getString(3)) : new HashMap<>();
            map.put(kylin_resultSet.getString(2), kylin_resultSet.getString(1));
            conMap.put(kylin_resultSet.getString(3), map);
        }
    }
}
