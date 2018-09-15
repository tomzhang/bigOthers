package jinke.com.fileoperate;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.*;

import java.io.File;

/**
 * Created by tanghanzhuang on 2018/8/2
 */
public class GetFileOss {
    public static void main(String[] args) {
        // Endpoint以杭州为例，其它Region请按实际情况填写。
        String endpoint = "http://oss-cn-hangzhou.aliyuncs.com";
        // 阿里云主账号AccessKey拥有所有API的访问权限，风险很高。强烈建议您创建并使用RAM账号进行API访问或日常运维，请登录 https://ram.console.aliyun.com 创建RAM账号。
        String accessKeyId = "LTAI7NY1JZULsbNB";
        String accessKeySecret = "G1Loy58bxfe2DPUEG0y1vdgVg71zHU";
        String bucketName = "mission-log";
        String video_objectName = "douyin_user/2018/08/02/";
        // 创建OSSClient实例。
        OSSClient ossClient = new OSSClient(endpoint, accessKeyId, accessKeySecret);

/*
        ossClient.getObject(new GetObjectRequest(bucketName, objectName), new File("E:\\youdu\\download\\file\\parquet\\mission_video\\"));

        GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, key);

// 下载Object到文件
        ObjectMetadata objectMetadata = client.getObject(getObjectRequest, new File("/path/to/file"));
*/




/*        // ossClient.listObjects返回ObjectListing实例，包含此次listObject请求的返回结果。
        ObjectListing objectListing = ossClient.listObjects(bucketName);
        objectListing.setPrefix("douyin_video/");
        // objectListing.getObjectSummaries获取所有文件的描述信息。
        for (OSSObjectSummary objectSummary : objectListing.getObjectSummaries()) {
            System.out.println(" - " + objectSummary.getKey() + "  " +
                    "(size = " + objectSummary.getSize() + ")");
        }
        // 关闭OSSClient。*/


        ListObjectsRequest listObjectsRequest = new ListObjectsRequest(bucketName);
//        listObjectsRequest.setDelimiter("/");
        listObjectsRequest.setPrefix(video_objectName);
        ObjectListing listing = ossClient.listObjects(listObjectsRequest);
        for (OSSObjectSummary objectSummary : listing.getObjectSummaries()) {
            String filename = objectSummary.getKey().replaceAll("/","_");
            try {
                GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, objectSummary.getKey());
                ObjectMetadata objectMetadata = ossClient.getObject(getObjectRequest, new File("E:\\youdu\\download\\file\\parquet\\mission_user\\" + filename));
                System.out.println("SUCCESS:" + filename);
            }catch (Exception e){
                System.out.println("failed:" + filename);
                e.printStackTrace();
            }
        }

        ossClient.shutdown();
    }
}
