package jinke.com.entity;

import lombok.Data;

/**
 * Created by tanghanzhuang on 2018/8/14
 */
@Data
public class MysqlUserEntity {

    private String username;

    private String password;

    private String url;

    private String tenantType;

    private String domainName;
}
