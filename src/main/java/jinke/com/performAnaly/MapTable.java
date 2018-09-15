package jinke.com.performAnaly;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import jinke.com.ov.CityVo;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by tanghanzhuang on 2018/8/9
 */
public class MapTable {
    public static Map<String, String> locationMap = new HashMap<>();

    static {
        locationMap.put("北京", "0");
        locationMap.put("上海", "0");
        locationMap.put("广州", "0");
        locationMap.put("深圳", "0");
        locationMap.put("成都", "1");
        locationMap.put("杭州", "1");
        locationMap.put("重庆", "1");
        locationMap.put("武汉", "1");
        locationMap.put("宿州", "1");
        locationMap.put("西安", "1");
        locationMap.put("天津", "1");
        locationMap.put("南京", "1");
        locationMap.put("郑州", "1");
        locationMap.put("长沙", "1");
        locationMap.put("沈阳", "1");
        locationMap.put("青岛", "1");
        locationMap.put("宁波", "1");
        locationMap.put("东莞", "1");
        locationMap.put("无锡", "1");
        locationMap.put("昆明", "2");
        locationMap.put("大连", "2");
        locationMap.put("厦门", "2");
        locationMap.put("合肥", "2");
        locationMap.put("佛山", "2");
        locationMap.put("福州", "2");
        locationMap.put("哈尔滨", "2");
        locationMap.put("济南", "2");
        locationMap.put("温州", "2");
        locationMap.put("长春", "2");
        locationMap.put("石家庄", "2");
        locationMap.put("常州", "2");
        locationMap.put("泉州", "2");
        locationMap.put("南宁", "2");
        locationMap.put("贵阳", "2");
        locationMap.put("南昌", "2");
        locationMap.put("南通", "2");
        locationMap.put("金华", "2");
        locationMap.put("徐州", "2");
        locationMap.put("太原", "2");
        locationMap.put("嘉兴", "2");
        locationMap.put("烟台", "2");
        locationMap.put("惠州", "2");
        locationMap.put("保定", "2");
        locationMap.put("台州", "2");
        locationMap.put("中山", "2");
        locationMap.put("绍兴", "2");
        locationMap.put("乌鲁木齐", "2");
        locationMap.put("潍坊", "2");
        locationMap.put("兰州", "2");
        locationMap.put("珠海", "3");
        locationMap.put("镇江", "3");
        locationMap.put("海口", "3");
        locationMap.put("扬州", "3");
        locationMap.put("临沂", "3");
        locationMap.put("洛阳", "3");
        locationMap.put("唐山", "3");
        locationMap.put("呼和浩特", "3");
        locationMap.put("盐城", "3");
        locationMap.put("汕头", "3");
        locationMap.put("廊坊", "3");
        locationMap.put("泰州", "3");
        locationMap.put("济宁", "3");
        locationMap.put("湖州", "3");
        locationMap.put("江门", "3");
        locationMap.put("银川", "3");
        locationMap.put("淄博", "3");
        locationMap.put("邯郸", "3");
        locationMap.put("芜湖", "3");
        locationMap.put("漳州", "3");
        locationMap.put("绵阳", "3");
        locationMap.put("桂林", "3");
        locationMap.put("三亚", "3");
        locationMap.put("遵义", "3");
        locationMap.put("咸阳", "3");
        locationMap.put("上饶", "3");
        locationMap.put("莆田", "3");
        locationMap.put("宜昌", "3");
        locationMap.put("赣州", "3");
        locationMap.put("淮安", "3");
        locationMap.put("揭阳", "3");
        locationMap.put("沧州", "3");
        locationMap.put("商丘", "3");
        locationMap.put("连云港", "3");
        locationMap.put("柳州", "3");
        locationMap.put("岳阳", "3");
        locationMap.put("信阳", "3");
        locationMap.put("株洲", "3");
        locationMap.put("衡阳", "3");
        locationMap.put("襄阳", "3");
        locationMap.put("南阳", "3");
        locationMap.put("威海", "3");
        locationMap.put("湛江", "3");
        locationMap.put("包头", "3");
        locationMap.put("鞍山", "3");
        locationMap.put("九江", "3");
        locationMap.put("大庆", "3");
        locationMap.put("许昌", "3");
        locationMap.put("新乡", "3");
        locationMap.put("宁德", "3");
        locationMap.put("西宁", "3");
        locationMap.put("宿迁", "3");
        locationMap.put("菏泽", "3");
        locationMap.put("蚌埠", "3");
        locationMap.put("邢台", "3");
        locationMap.put("铜陵", "3");
        locationMap.put("阜阳", "3");
        locationMap.put("荆州", "3");
        locationMap.put("驻马店", "3");
        locationMap.put("湘潭", "3");
        locationMap.put("滁州", "3");
        locationMap.put("肇庆", "3");
        locationMap.put("德阳", "3");
        locationMap.put("曲靖", "3");
        locationMap.put("秦皇岛 ", "3");
        locationMap.put("潮州", "3");
        locationMap.put("吉林", "3");
        locationMap.put("常德", "3");
        locationMap.put("宜春", "3");
        locationMap.put("黄冈", "3");
        locationMap.put("舟山", "4");
        locationMap.put("泰安", "4");
        locationMap.put("孝感", "4");
        locationMap.put("鄂尔多斯", "4");
        locationMap.put("开封", "4");
        locationMap.put("南平", "4");
        locationMap.put("齐齐哈尔", "4");
        locationMap.put("德州", "4");
        locationMap.put("宝鸡", "4");
        locationMap.put("马鞍山", "4");
        locationMap.put("郴州", "4");
        locationMap.put("安阳", "4");
        locationMap.put("龙岩", "4");
        locationMap.put("聊城", "4");
        locationMap.put("渭南", "4");
        locationMap.put("宿州", "4");
        locationMap.put("衢州", "4");
        locationMap.put("梅州", "4");
        locationMap.put("宣城", "4");
        locationMap.put("周口", "4");
        locationMap.put("丽水", "4");
        locationMap.put("安庆", "4");
        locationMap.put("三明", "4");
        locationMap.put("枣庄", "4");
        locationMap.put("南充", "4");
        locationMap.put("淮南", "4");
        locationMap.put("平顶山", "4");
        locationMap.put("东营", "4");
        locationMap.put("呼伦贝尔", "4");
        locationMap.put("乐山", "4");
        locationMap.put("张家口", "4");
        locationMap.put("清远", "4");
        locationMap.put("焦作", "4");
        locationMap.put("河源", "4");
        locationMap.put("运城", "4");
        locationMap.put("锦州", "4");
        locationMap.put("赤峰", "4");
        locationMap.put("六安", "4");
        locationMap.put("盘锦", "4");
        locationMap.put("宜宾", "4");
        locationMap.put("榆林", "4");
        locationMap.put("日照", "4");
        locationMap.put("晋中", "4");
        locationMap.put("怀化", "4");
        locationMap.put("承德", "4");
        locationMap.put("遂宁", "4");
        locationMap.put("毕节", "4");
        locationMap.put("佳木斯", "4");
        locationMap.put("滨州", "4");
        locationMap.put("益阳", "4");
        locationMap.put("汕尾", "4");
        locationMap.put("邵阳", "4");
        locationMap.put("玉林", "4");
        locationMap.put("衡水", "4");
        locationMap.put("韶关", "4");
        locationMap.put("吉安", "4");
        locationMap.put("北海", "4");
        locationMap.put("茂名", "4");
        locationMap.put("延边", "4");
        locationMap.put("黄山", "4");
        locationMap.put("阳江", "4");
        locationMap.put("抚州", "4");
        locationMap.put("娄底", "4");
        locationMap.put("营口", "4");
        locationMap.put("牡丹江", "4");
        locationMap.put("大理", "4");
        locationMap.put("咸宁", "4");
        locationMap.put("黔南", "4");
        locationMap.put("泸州", "4");
        locationMap.put("玉溪", "4");
        locationMap.put("通辽", "4");
        locationMap.put("丹东", "4");
        locationMap.put("临汾", "4");
        locationMap.put("眉山", "4");
        locationMap.put("十堰", "4");
        locationMap.put("黄石", "4");
        locationMap.put("濮阳", "4");
        locationMap.put("亳州", "4");
        locationMap.put("抚顺", "4");
        locationMap.put("永州", "4");
        locationMap.put("丽江", "4");
        locationMap.put("漯河", "4");
        locationMap.put("铜仁", "4");
        locationMap.put("大同", "4");
        locationMap.put("松原", "4");
        locationMap.put("通化", "4");
        locationMap.put("红河", "4");
        locationMap.put("内江", "4");
    }

    public void buildMap () {
        Map<String, CityVo> locationMap = new HashMap<>();
        String path = "E:\\youdu\\download\\file\\json\\newjingweidu.json";
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(path));
            String str;
            int i = 0;
            while ((str = bufferedReader.readLine()) != null) {
                JSONObject cityJson = JSON.parseObject(str);
                CityVo cityVo = new CityVo();
                cityVo.setCity(cityJson.getString("city"));
                cityVo.setProvince(cityJson.getString("province"));
                cityVo.setEastLongitude(cityJson.getString("eastLongitude"));
                cityVo.setNorthernLatitude(cityJson.getString("northernLatitude"));
                cityVo.setCityLevel(cityJson.getString("cityLevel"));
                locationMap.put(cityVo.getCity(), cityVo);
                i++;
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
//            log.error("ERROR:The json file cannot be found in the specified folder:"+path);
        } catch (IOException e) {
            e.printStackTrace();
//            log.error("ERROR:"+path+e);
        }
        System.out.println(locationMap.size());


/*        String path = "E:\\youdu\\download\\file\\json\\newjingweidu.json";
//        String fileName="E:\\youdu\\download\\file\\json\\newcityjignwedudaquan.json";
        *//*File file = new File(path);*//*
        BufferedReader bufferedReader = new BufferedReader(new FileReader(path));
        Map<String,CityVo> map = new HashMap<>();
//        List<CityVo> cityVoList = new ArrayList<>();
        String str;
//        FileWriter writer=new FileWriter(fileName);
        SerializerFeature[] featureArr = { SerializerFeature.WriteClassName };
        int i=0;
        while ((str = bufferedReader.readLine())!=null){
            JSONObject cityJson = JSON.parseObject(str);
            CityVo cityVo = new CityVo();
            cityVo.setCity(cityJson.getString("city"));
            cityVo.setProvince(cityJson.getString("province"));
            cityVo.setEastLongitude(cityJson.getString("eastLongitude"));
            cityVo.setNorthernLatitude(cityJson.getString("northernLatitude"));
            cityVo.setCityLevel(cityJson.getString("cityLevel"));
            map.put(cityVo.getCity(), cityVo);
//            cityVoList.add(cityVo);
            i++;
        }
//        writer.close();
    System.out.println(map.size());
    System.out.println(i);*/

    }





}
