package zkapi;


import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.List;

/**
 * @Description:
 * @Author: Axin
 * @Date: Create in 14:57 2019/8/4
 */
public class zk {

    //连接集群的配置
    private static String connectString = "bigdata111:2181";
    private static int sessionTimeout = 2000;
    private ZooKeeper zkClient = null;

    @Before
    public void init() throws Exception {
        zkClient = new ZooKeeper(connectString,sessionTimeout,new Watcher(){

            public void process(WatchedEvent event) {
                //收到事件通知后的回调函数（用户的业务逻辑）
                System.out.println(event.getPath() + " -- " + event.getType() + "--" +  event.getState());


                //再次启动监听
                try {
                    zkClient.getChildren("/",true);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });


    }

    /**
     * 创建节点
     * @throws KeeperException
     * @throws InterruptedException
     */
   @Test
    public void create() throws KeeperException, InterruptedException {
        String nodeCreated = zkClient.create("/test", "random".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(nodeCreated);
    }


    /**
     * 获取节点
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void getChildren() throws KeeperException, InterruptedException {
        List<String> children = zkClient.getChildren("/", true);
        for (String child : children) {
            System.out.println(child);
        }

        Thread.sleep(Long.MAX_VALUE);
    }


    @Test
    public void isExists() throws KeeperException, InterruptedException {
        Stat exists = zkClient.exists("/test", true);
        System.out.println(exists);
        System.out.println(exists == null ? "无":"有");
    }
}
