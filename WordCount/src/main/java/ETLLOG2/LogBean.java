package ETLLOG2;

import lombok.*;

/**
 * @Description: 日志
 * @Author: Axin
 * @Date: Create in 23:07 2019/6/27
 */

@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class LogBean {

    //用户IP
    private String remote_addr;
    //用户名称
    private String remote_user;
    //时间
    private String time_local;
    //请求URL
    private String request;
    //返回状态码
    private String status;
    //文件内容大小
    private String body_bytes_sent;
    //链接页面
    private String http_referer;
    //浏览的相关信息
    private String http_user_agent;

    private boolean valid = true;

    //alt+Insert //生成方法


    @Override
    public String toString()
    {
      StringBuilder sb = new StringBuilder();
        sb.append(this.valid);
        sb.append("\001").append(this.remote_addr);
        sb.append("\001").append(this.remote_user);
        sb.append("\001").append(this.time_local);
        sb.append("\001").append(this.request);
        sb.append("\001").append(this.status);
        sb.append("\001").append(this.body_bytes_sent);
        sb.append("\001").append(this.http_referer);
        sb.append("\001").append(this.http_user_agent);
      return sb.toString();



    }

}
