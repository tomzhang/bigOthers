package jinke.com.performAnaly;

import java.io.IOException;

import java.io.InputStream;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Properties;

/**
 * Created by tanghanzhuang on 2018/8/9
 */
public class MapAndProperties {
    public void mapAnaly(){
        Map map = MapTable.locationMap;
        LocalDateTime start = LocalDateTime.now();
        for (int i=0;i<10000000;i++){
            String a = (String) map.get("通化");
            System.out.println(a);
        }
        LocalDateTime end = LocalDateTime.now();
        System.out.println("start:"+start+"---end:"+end);
    }
    public void properties() throws IOException {
        InputStream inputStream = MapAndProperties.class.getClassLoader().getResourceAsStream("locationMap.properties");

        Properties properties = new Properties();
        properties.load(inputStream);
        LocalDateTime start = LocalDateTime.now();
        for (int i=0;i<10000000;i++){
            String b = properties.getProperty("aaa");
            System.out.println(b);
        }
        LocalDateTime end = LocalDateTime.now();
        System.out.println("start:"+start+"---end:"+end);
    }
}
