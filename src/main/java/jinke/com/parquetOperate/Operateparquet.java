package jinke.com.parquetOperate;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import jinke.com.ov.DouYinFansVo;
import jinke.com.util.ParquetUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.schema.MessageType;

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * @program: bigOthers
 * @description:
 * @author: tanghanzhuang
 * @create: 2018-09-14 16:13
 */
public class Operateparquet {
    public static String PARQUET_PATH_FANS = "E:\\opt\\missionData\\new_douyin_fans";
    public static String OLD_PARQUET_PATH_FANS = "E:\\opt\\missionData\\douyin_fans";

    public void showParquetData(String filePath) throws IOException {
        GroupReadSupport readSupport = new GroupReadSupport();
        ParquetReader.Builder<Group> reader = ParquetReader.builder(readSupport, new Path(filePath));
        ParquetReader<Group> build = reader.build();
        Group line = null;
//        List<DouYinFansVo> fansVos = new ArrayList<>();
        line = build.read();
        System.out.println(line.toString());
//        while ((line = build.read()) != null) {
//
//        }
    }

    public void readParquet() throws IOException {

//        String inPath1 = "E:\\opt\\data\\douyin_fans\\dt=2018-08-01\\20180814164800_fans.parquet";
        File file = new File(OLD_PARQUET_PATH_FANS);
        File[] files = file.listFiles();
        for (File fs : files) {
            if (fs.isDirectory()) {
                File[] files1 = fs.listFiles();
                for (File fs1 : files1) {
                    if (fs1.getName().endsWith(".parquet")) {
                        String inPath = fs1.getPath();
                        String fileName = inPath.substring(inPath.lastIndexOf("\\")+1);
                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
                        DateTimeFormatter formatter1 = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
                        String fileTimeStr = fileName.substring(0, fileName.indexOf("_"));
                        LocalDateTime fileTime = LocalDateTime.parse(fileTimeStr, formatter1);
                        String newFilePath = "dt=" + formatter.format(fileTime);

                        GroupReadSupport readSupport = new GroupReadSupport();
                        ParquetReader.Builder<Group> reader = ParquetReader.builder(readSupport, new Path(inPath));
                        ParquetReader<Group> build = reader.build();
                        Group line = null;
                        List<DouYinFansVo> fansVos = new ArrayList<>();
                        while ((line = build.read()) != null) {
//                            Group a = line;
//                            System.out.println(a.getString("birthday", 0) + "--" + a.getString("age_grades", 0));
                            fansVos.add(analyParquet(line));
                        }
                        MessageType fansMessageType = DouYinFansParquetHandler.BuildMessageType();
                        String parquetFansPath = PARQUET_PATH_FANS + "\\" + newFilePath + "\\" + fileName;
                        ParquetWriter<Group> fansWriter = ParquetUtil.getParquetWriter(parquetFansPath, fansMessageType);
                        try {
                            /* 用户数据 */
                            for (DouYinFansVo fans : fansVos) {
                                Group group = DouYinFansParquetHandler.getGroup(fansMessageType, fans);
                                fansWriter.write(group);
                            }
//            log.info("SUCCESS: all the files in {} have been written to {}", Param.FANS_JSON_PATH, Param.PARQUET_PATH_FANS);
                            fansWriter.close();
                        } catch (IOException e) {
                            e.printStackTrace();
//            log.error("FAILURE : {} files appear unexpected exceptions in the process of writing {}.", Param.FANS_JSON_PATH, Param.PARQUET_PATH_FANS);
                        }
                        System.out.println("SUCCESS:"+inPath);
                    }
                }
            }
        }

    }
//    //新版本中new ParquetReader()所有构造方法好像都弃用了,用上面的builder去构造对象
//    static void parquetReader(String inPath) throws Exception{
//        GroupReadSupport readSupport = new GroupReadSupport();
//        ParquetReader<Group> reader = new ParquetReader<Group>(new Path(inPath),readSupport);
//        Group line=null;
//        while((line=reader.read())!=null){
//            System.out.println(line.toString());
//        }
//        System.out.println("读取结束");
//
//    }


    public DouYinFansVo analyParquet(Group group) {
        DouYinFansVo vo = null;

        vo = new DouYinFansVo();
        vo.setAcceptPrivatePolicy(group.getString("accept_private_policy", 0));
        vo.setAccountRegion(group.getString("account_region", 0));
        vo.setActivityDiggCount(group.getString("activity_digg_count", 0));
        vo.setActivityUseMusicCount(group.getString("activity_use_music_count", 0));
        vo.setAppleAccount(group.getString("apple_account", 0));
        vo.setAuthorityStatus(group.getString("authority_status", 0));
        vo.setAvatarLargerUrl(group.getString("avatar_larger_url", 0));
        vo.setAvatarMediumUrl(group.getString("avatar_medium_url", 0));
        vo.setAvatarThumbUrl(group.getString("avatar_thumb_url", 0));
        vo.setBindPhone(group.getString("bind_phone", 0));
        vo.setBirthday(group.getString("birthday", 0));
        if (!StringUtils.isEmpty(group.getString("birthday", 0))) {
            String birthdayStr = group.getString("birthday", 0);
            LocalDate today = LocalDate.now();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            LocalDate birth = LocalDate.parse(birthdayStr, formatter);
            int age = today.getYear() - birth.getYear();
            if (age < 6) {
                vo.setAgeGrades("1");
            } else if (age >= 6 && age <= 11) {
                vo.setAgeGrades("2");
            } else if (age >= 12 && age <= 14) {
                vo.setAgeGrades("3");
            } else if (age >= 15 && age <= 17) {
                vo.setAgeGrades("4");
            } else if (age >= 18 && age <= 21) {
                vo.setAgeGrades("5");
            } else if (age >= 22 && age <= 24) {
                vo.setAgeGrades("6");
            } else if (age >= 25 && age <= 30) {
                vo.setAgeGrades("7");
            } else if (age >= 31 && age <= 40) {
                vo.setAgeGrades("8");
            } else if (age >= 41 && age <= 50) {
                vo.setAgeGrades("9");
            } else if (age > 50) {
                vo.setAgeGrades("10");
            } else {
                vo.setAgeGrades("0");
            }
        } else {
            vo.setAgeGrades("0");
        }
//        System.out.println("new----"+group.getString("birthday", 0) + "--" + vo.getAgeGrades());
        vo.setAgeGrades(group.getString("age_grades", 0));
        vo.setCommerceUserLevel(group.getString("commerce_user_level", 0));
        vo.setConstellation(group.getString("constellation", 0));
        vo.setCustomVerify(group.getString("custom_verify", 0));
        vo.setDongtaiCount(group.getString("dongtai_count", 0));
        vo.setEnterpriseVerifyReason(group.getString("enterprise_verify_reason", 0));
        vo.setFavoritingCount(group.getString("favoriting_count", 0));
        vo.setFbExpireTime(group.getString("fb_expire_time", 0));
        vo.setFollowStatus(group.getString("follow_status", 0));
        vo.setFollowerCount(group.getString("follower_count", 0));
        vo.setFollowingId(group.getString("following_id", 0));
        vo.setFollowingCount(group.getString("following_count", 0));
        vo.setGender(group.getString("gender", 0));
        vo.setGoogleAccount(group.getString("google_account", 0));
        vo.setHasActivityMedal(group.getString("has_activity_medal", 0));
        vo.setHasEmail(group.getString("has_email", 0));
        vo.setHasFacebookToken(group.getString("has_facebook_token", 0));
        vo.setHasOrders(group.getString("has_orders", 0));
        vo.setHasTwitterToken(group.getString("has_twitter_token", 0));
        vo.setHasYoutubeToken(group.getString("has_youtube_token", 0));
        vo.setHideLocation(group.getString("hide_location", 0));
        vo.setHideSearch(group.getString("hide_search", 0));
        vo.setInsId(group.getString("ins_id", 0));
        vo.setIsAdFake(group.getString("is_ad_fake", 0));
        vo.setIsBindedWeibo(group.getString("is_binded_weibo", 0));
        vo.setIsBlock(group.getString("is_block", 0));
        vo.setIsDisciplineMember(group.getString("is_discipline_member", 0));
        vo.setIsFlowcardMember(group.getString("is_flowcard_member", 0));
        vo.setIsGovMediaVip(group.getString("is_gov_media_vip", 0));
        vo.setIsPhoneBinded(group.getString("is_phone_binded", 0));
        vo.setIsVerified(group.getString("is_verified", 0));
        vo.setLiveAgreement(group.getString("live_agreement", 0));
        vo.setLiveVerify(group.getString("live_verify", 0));
        vo.setLocation(group.getString("location", 0));
        vo.setLocationProvince(group.getString("location_province", 0));
        vo.setLocationCity(group.getString("location_city", 0));
        vo.setLocationCitylevel(group.getString("location_citylevel", 0));
        vo.setLocationEastLongitude(group.getString("location_east_longitude", 0));
        vo.setLocationNorthernLatitude(group.getString("location_northern_latitude", 0));
        vo.setLoginPlatform(group.getString("login_platform", 0));
        vo.setMplatformFollowersCount(group.getString("mplatform_followers_count", 0));
        vo.setNeedRecommend(group.getString("need_recommend", 0));
        vo.setNeiguangShield(group.getString("neiguang_shield", 0));
        vo.setNickname(group.getString("nickname", 0));
        vo.setOriginalMusicDiggCount(group.getString("original_music_digg_count", 0));
        vo.setOriginalMusicCount(group.getString("original_music_count", 0));
        vo.setOriginalMusicUsedCount(group.getString("original_music_used_count", 0));
        vo.setPreventDownload(group.getString("prevent_download", 0));
        vo.setRealnameVerifyStatus(group.getString("realname_verify_status", 0));
        vo.setRegion(group.getString("region", 0));
        vo.setRoomId(group.getString("room_id", 0));
        vo.setSchoolName(group.getString("school_name", 0));
        vo.setSchoolPoiId(group.getString("school_poi_id", 0));
        vo.setSchoolType(group.getString("school_type", 0));
        vo.setSecret(group.getString("secret", 0));
        vo.setShareInfoBoolPersist(group.getString("share_info_bool_persist", 0));
        vo.setShareInfoDesc(group.getString("share_info_desc", 0));
        vo.setShareInfoImageUrl(group.getString("share_info_image_url", 0));
        vo.setShareInfoQrcodeUrl(group.getString("share_info_qrcode_url", 0));
        vo.setShareInfoTitle(group.getString("share_info_title", 0));
        vo.setShareInfoUrl(group.getString("share_info_url", 0));
        vo.setShareInfoWeiboDesc(group.getString("share_info_weibo_desc", 0));
        vo.setShieldCommentNotice(group.getString("shield_comment_notice", 0));
        vo.setShieldDiggNotice(group.getString("shield_digg_notice", 0));
        vo.setShieldFollowNotice(group.getString("shield_follow_notice", 0));
        vo.setShortId(group.getString("short_id", 0));
        vo.setSignature(group.getString("signature", 0));
        vo.setSpecialLock(group.getString("special_lock", 0));
        vo.setStarUseNewDownload(group.getString("star_use_new_download", 0));
        vo.setStoryCount(group.getString("story_count", 0));
        vo.setStoryOpen(group.getString("story_open", 0));
        vo.setSyncToToutiao(group.getString("sync_to_toutiao", 0));
        vo.setTenantId(group.getString("tenant_id", 0));
        vo.setTotalFavorited(group.getString("total_favorited", 0));
        vo.setTwExpireTime(group.getString("tw_expire_time", 0));
        vo.setTwitterId(group.getString("twitter_id", 0));
        vo.setTwitterName(group.getString("twitter_name", 0));
        vo.setUid(group.getString("uid", 0));
        vo.setUniqueId(group.getString("unique_id", 0));
        vo.setUniqueIdModifyTime(group.getString("unique_id_modify_time", 0));
        vo.setUserCanceled(group.getString("user_canceled", 0));
        vo.setVerifyInfo(group.getString("verify_info", 0));
        vo.setVideoIconUrl(group.getString("video_icon_url", 0));
        vo.setWeiboName(group.getString("weibo_name", 0));
        vo.setWeiboSchema(group.getString("weibo_schema", 0));
        vo.setWeiboUrl(group.getString("weibo_url", 0));
        vo.setWeiboVerify(group.getString("weibo_verify", 0));
        vo.setWithCommerceEntry(group.getString("with_commerce_entry", 0));
        vo.setWithDouEntry(group.getString("with_dou_entry", 0));
        vo.setWithDouplusEntry(group.getString("with_douplus_entry", 0));
        vo.setWithFusionShopEntry(group.getString("with_fusion_shop_entry", 0));
        vo.setWithNewGoods(group.getString("with_new_goods", 0));
        vo.setWithShopEntry(group.getString("with_shop_entry", 0));
        vo.setYoutubeChannelId(group.getString("youtube_channel_id", 0));
        vo.setYoutubeChannelTitle(group.getString("youtube_channel_title", 0));
        vo.setYoutubeExpireTime(group.getString("youtube_expire_time", 0));
        return vo;
    }

}
