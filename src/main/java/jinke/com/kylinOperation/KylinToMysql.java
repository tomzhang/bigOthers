package jinke.com.kylinOperation;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.PreparedStatement;
import org.joda.time.LocalDateTime;

import java.sql.*;
import java.text.DecimalFormat;
import java.time.LocalDate;
import java.util.*;

/**
 * Created by tanghanzhuang on 2018/8/6
 */
public class KylinToMysql {
    private java.sql.Connection getKylinConn() throws SQLException, ClassNotFoundException, IllegalAccessException, InstantiationException {
        Driver kylin_driver = (Driver) Class.forName("org.apache.kylin.jdbc.Driver").newInstance();
        Properties kylin_info = new Properties();
        kylin_info.put("user", "ADMIN");
        kylin_info.put("password", "KYLIN");
        java.sql.Connection conn = kylin_driver.connect("jdbc:kylin://172.18.245.121:7070/mission_bigdata", kylin_info);
        return conn;
    }

    private Connection getMysqlConn() {
        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://172.16.100.32:3306/mission2?useUnicode=true&characterEncoding=utf-8";
        String username = "qiji";
        String password = "qiji!@#2018";
        Connection conn = null;
        try {
            Class.forName(driver);
            conn = (Connection) DriverManager.getConnection(url, username, password);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

/*    public void getConnection() throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
        java.sql.Connection kylin_conn = getKylinConn();
//        Connection mysql_conn = getMysqlConn();
        Statement kylin_state = kylin_conn.createStatement();
        ResultSet kylin_resultSet;
        String getGenderSql = "select count(uid) as geshu,DOUYIN_FANS.DT ,LOCATION_CITY,DOUYIN_FANS.FOLLOWING_ID  from mission.DOUYIN_FANS where DOUYIN_FANS.FOLLOWING_ID=\n" +
                "'22222222' group by DOUYIN_FANS.DT , DOUYIN_FANS.FOLLOWING_ID, DOUYIN_FANS.LOCATION_CITY ;";
        kylin_resultSet = kylin_state.executeQuery(getGenderSql);
        while (kylin_resultSet.next()) {
            System.out.println(kylin_resultSet.getString(1));
            System.out.println(kylin_resultSet.getString(2));
            System.out.println(kylin_resultSet.getString(3));
            System.out.println(kylin_resultSet.getString(4));
        }
        kylin_conn.close();

    }*/


    public void testExport1() throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
        java.sql.Connection kylin_conn = getKylinConn();
        Connection mysql_conn = getMysqlConn();
        Statement kylin_state = kylin_conn.createStatement();
//        ResultSet kylin_resultSet = kylin_state.executeQuery("select count(short_id),age_grades from DOUYIN_FANS group by age_grades;");
        ResultSet kylin_resultSet;
        PreparedStatement pstm = null;

//        String commonSql = "select uid,platform_id,log_Create_Time from DOUYIN_FANS where uid='1111111111';";
         LocalDate now = LocalDate.now();
        String dt;
        DecimalFormat df = new DecimalFormat("00");
        if (now.getDayOfMonth()<15){
            dt = now.getYear()+"-"+df.format(now.getMonthValue())+"-01";
        }else {
            dt = now.getYear()+"-"+df.format(now.getMonthValue())+"-15";
        }
        String getGenderSql = "select count(DISTINCT  DOUYIN_FANS.UID ),DOUYIN_FANS.GENDER ,DOUYIN_FANS.FOLLOWING_ID,DOUYIN_FANS.DT  from mission.DOUYIN_FANS where DOUYIN_FANS.DT ='"+dt+"' group by DOUYIN_FANS.GENDER , DOUYIN_FANS.FOLLOWING_ID,DOUYIN_FANS.DT;";
        String getAgeGradesSql = "select count(DISTINCT  DOUYIN_FANS.UID ),DOUYIN_FANS.AGE_GRADES  ,DOUYIN_FANS.FOLLOWING_ID,DOUYIN_FANS.DT  from mission.DOUYIN_FANS where DOUYIN_FANS.DT ='"+dt+"' group by DOUYIN_FANS.AGE_GRADES  , DOUYIN_FANS.FOLLOWING_ID,DOUYIN_FANS.DT;";
        String getConstellation = "select count(DISTINCT  DOUYIN_FANS.UID ), DOUYIN_FANS.CONSTELLATION ,DOUYIN_FANS.FOLLOWING_ID,DOUYIN_FANS.DT  from mission.DOUYIN_FANS where DOUYIN_FANS.DT ='"+dt+"' group by DOUYIN_FANS.CONSTELLATION , DOUYIN_FANS.FOLLOWING_ID,DOUYIN_FANS.DT;";
        HashMap<String, Map<String,String>> genderMap = new HashMap();
        HashMap<String, Map<String,String>> ageMap = new HashMap();
        HashMap<String, Map<String,String>> conMap = new HashMap();

        kylin_resultSet = kylin_state.executeQuery(getGenderSql);
        while (kylin_resultSet.next()) {
            Map<String,String> map =null != genderMap.get(kylin_resultSet.getString(3)) ? genderMap.get(kylin_resultSet.getString(3)) : new HashMap<>();
            map.put(kylin_resultSet.getString(2), kylin_resultSet.getString(1));
            genderMap.put(kylin_resultSet.getString(3),map);
        }
        kylin_resultSet = kylin_state.executeQuery(getAgeGradesSql);
        while (kylin_resultSet.next()) {
            Map<String,String> map = null != ageMap.get(kylin_resultSet.getString(3)) ? ageMap.get(kylin_resultSet.getString(3)) : new HashMap<>();

            map.put(kylin_resultSet.getString(2), kylin_resultSet.getString(1));
            ageMap.put(kylin_resultSet.getString(3),map);
        }
        kylin_resultSet = kylin_state.executeQuery(getConstellation);
        while (kylin_resultSet.next()) {
            Map<String,String> map =null != conMap.get(kylin_resultSet.getString(3)) ? conMap.get(kylin_resultSet.getString(3)) : new HashMap<>();
            map.put(kylin_resultSet.getString(2), kylin_resultSet.getString(1));
            conMap.put(kylin_resultSet.getString(3),map);
        }



        String sql = "insert into rpt_mission_fans(account_uid,platform_id,male_fans,female_fans,age_of_child,age_of_primary_school,age_of_middle_school,age_of_high_school,age_of_college,age_of_job_newcomer,age_of_marriage,constellation_aquarius,constellation_pisces,constellation_aries,constellation_taurus,constellation_gemini,constellation_cancer,constellation_leo,constellation_virgo,constellation_libra,constellation_scorpio,constellation_sagittarius,constellation_capricorn,create_date,unknown_gender_fans)" +
                " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        pstm = (PreparedStatement) mysql_conn.prepareStatement(sql);
        for(String accountId: genderMap.keySet()){
        pstm.setString(1, accountId);
        pstm.setString(2, "1");//platform_id
        pstm.setString(3, null != genderMap.get(accountId).get("1") ? genderMap.get(accountId).get("1") : "0");
        pstm.setString(4, null != genderMap.get(accountId).get("2") ? genderMap.get(accountId).get("2") : "0");
        pstm.setString(5, null != ageMap.get(accountId).get("1") ? ageMap.get(accountId).get("1") : "0");
        pstm.setString(6, null != ageMap.get(accountId).get("2") ? ageMap.get(accountId).get("2") : "0");
        pstm.setString(7, null != ageMap.get(accountId).get("3") ? ageMap.get(accountId).get("3") : "0");
        pstm.setString(8, null != ageMap.get(accountId).get("4") ? ageMap.get(accountId).get("4") : "0");
        pstm.setString(9, null != ageMap.get(accountId).get("5") ? ageMap.get(accountId).get("5") : "0");
        pstm.setString(10, null != ageMap.get(accountId).get("6") ? ageMap.get(accountId).get("6") : "0");
        pstm.setString(11, null != ageMap.get(accountId).get("7") ? ageMap.get(accountId).get("7") : "0");
        pstm.setString(12, null != conMap.get(accountId).get("1") ? conMap.get(accountId).get("1") : "0");
        pstm.setString(13, null != conMap.get(accountId).get("2") ? conMap.get(accountId).get("2") : "0");
        pstm.setString(14, null != conMap.get(accountId).get("3") ? conMap.get(accountId).get("3") : "0");
        pstm.setString(15, null != conMap.get(accountId).get("4") ? conMap.get(accountId).get("4") : "0");
        pstm.setString(16, null != conMap.get(accountId).get("5") ? conMap.get(accountId).get("5") : "0");
        pstm.setString(17, null != conMap.get(accountId).get("6") ? conMap.get(accountId).get("6") : "0");
        pstm.setString(18, null != conMap.get(accountId).get("7") ? conMap.get(accountId).get("7") : "0");
        pstm.setString(19, null != conMap.get(accountId).get("8") ? conMap.get(accountId).get("8") : "0");
        pstm.setString(20, null != conMap.get(accountId).get("9") ? conMap.get(accountId).get("9") : "0");
        pstm.setString(21, null != conMap.get(accountId).get("10") ? conMap.get(accountId).get("10") : "0");
        pstm.setString(22, null != conMap.get(accountId).get("11") ? conMap.get(accountId).get("11") : "0");
        pstm.setString(23, null != conMap.get(accountId).get("12") ? conMap.get(accountId).get("12") : "0");
        pstm.setString(24, dt);
        pstm.setString(25, null != genderMap.get(accountId).get("0") ? genderMap.get(accountId).get("0") : "0");
        pstm.addBatch();
        }
        int[] fansResult=pstm.executeBatch();
//        log.info("SUCCESS:{};record account:{}",sql,fansResult.length);


        String getCitySQl = "select DOUYIN_FANS.FOLLOWING_ID, DOUYIN_FANS.LOCATION_PROVINCE,DOUYIN_FANS.LOCATION_CITY, DOUYIN_FANS.LOCATION_CITYLEVEL, count(uid),DOUYIN_FANS.DT, DOUYIN_FANS.LOCATION_EAST_LONGITUDE,DOUYIN_FANS.LOCATION_NORTHERN_LATITUDE  from mission.DOUYIN_FANS  group by DOUYIN_FANS.DT,DOUYIN_FANS.LOCATION_PROVINCE,DOUYIN_FANS.LOCATION_CITYLEVEL, DOUYIN_FANS.FOLLOWING_ID, DOUYIN_FANS.LOCATION_CITY ,DOUYIN_FANS.LOCATION_EAST_LONGITUDE ,DOUYIN_FANS.LOCATION_NORTHERN_LATITUDE ;";
        String insertCitySql = "insert into rpt_fans_city(account_id,province,city,city_level,fans_number,date_time,east_longitude,northern_latitude) values(?,?,?,?,?,?,?,?)";
        pstm = (PreparedStatement) mysql_conn.prepareStatement(insertCitySql);
        kylin_resultSet = kylin_state.executeQuery(getCitySQl);
        int i = 0;
        while (kylin_resultSet.next() && i < 2) {
            System.out.println(kylin_resultSet.getString(1));
            System.out.println(kylin_resultSet.getString(2));
            pstm.setString(1,kylin_resultSet.getString(1));
            pstm.setString(2,kylin_resultSet.getString(2));
            pstm.setString(3,kylin_resultSet.getString(3));
            pstm.setString(4,kylin_resultSet.getString(4));
            pstm.setString(5,kylin_resultSet.getString(5));
            pstm.setString(6,kylin_resultSet.getString(6));
            pstm.setString(7,kylin_resultSet.getString(7));
            pstm.setString(8,kylin_resultSet.getString(8));
            pstm.addBatch();
            i++;
        }
        int[] rows = pstm.executeBatch();

        System.out.println("长度：" + i);


        pstm.close();
        kylin_resultSet.close();
        kylin_resultSet.close();
        kylin_conn.close();
        mysql_conn.close();
    }

    /*多租户*/
    public void testDuoZuhu() {
//        Map<String,List<Map<String>>> bigmap = new HashMap<>();

        Map<String,HashSet<String>> tenantMap = new HashMap<>();
//        tenantMap.get()
    }


}
