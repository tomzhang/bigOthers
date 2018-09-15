package jinke.com.fileoperate;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;

import java.io.*;

/**
 * Created by tanghanzhuang on 2018/8/7
 */
public class GetFile {
    public static void  main(String[] args) throws IOException {
        String src = "E:\\opt";
        File file = new File(src);
        File[] userFileList = file.listFiles();
        BufferedReader bufferedReader = null;
        for(File file1:userFileList){
            String accountId = file1.getName();
            File accFansFile = new File(file1.getPath()+"/fans");
            File[] accFileList = accFansFile.listFiles();
            System.out.println(accountId);
            for(File file2:accFileList) {
                bufferedReader = new BufferedReader(new FileReader(file2));
                System.out.println(file2.getName());
                String str;
                while((str = bufferedReader.readLine())  != null){
                    System.out.println(str);
                }
            }
        }
    }

    public static void getOs(String id,String str){
        JSONObject jsonObject = (JSONObject) JSON.parse(str);

    }
}
