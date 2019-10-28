package memcached_test;

import net.spy.memcached.MemcachedClient;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Created by Axin in 2019/10/14 22:01
 */
public class MemCached_Test {

    public static void main(String[] args) throws InterruptedException, ExecutionException, IOException {
        //set();
        //get();
        //setObject();
        d();
    }

    public static void set() throws IOException, ExecutionException, InterruptedException {
//        建立MemcachedClient实例，指定Memcac服务的IP地址和端口号
        MemcachedClient client = new MemcachedClient(new InetSocketAddress("192.168.110.111",11211));

//        使用api操作时不用指定长度，API会自动计算Value值的长度，
//        第二个参数是过期时间
//        第三个参数可以为int类型，也可以放对象类型
        Future<Boolean> f = client.set("key01", 50, "hello world");
        if(f.get().booleanValue()){
            client.shutdown();
        }

    }

    public static void get() throws IOException, ExecutionException, InterruptedException {
//        建立MemcachedClient实例，指定Memcac服务的IP地址和端口号
        MemcachedClient client = new MemcachedClient(new InetSocketAddress("192.168.110.111",11211));

        Object f = client.get("key01");

        System.out.println("取到的值为：" + f.toString());
        client.shutdown();

    }



    public static void setObject() throws IOException, ExecutionException, InterruptedException {
        //建立MemcachedClient实例，指定Memcac服务的IP地址和端口号
        MemcachedClient client = new MemcachedClient(new InetSocketAddress("192.168.110.111", 11211));

        //使用api操作时不用指定长度，API会自动计算Value值的长度，
        //第二个参数是过期时间
        //第三个参数可以为int类型，也可以放对象类型
        Future<Boolean> f = client.set("student01", 0, new Student());
        if (f.get().booleanValue()) {
            client.shutdown();
        }
    }

    public static void d() throws IOException, InterruptedException {
        //客户端路由算法
        List<InetSocketAddress> list = new ArrayList<>();

        list.add(new InetSocketAddress("192.168.110.111",11211));
        list.add(new InetSocketAddress("192.168.110.111",11212));
        list.add(new InetSocketAddress("192.168.110.111",11213));


        //建立MemcachedClient实例，指定Memcac服务的IP地址和端口号
        MemcachedClient client = new MemcachedClient(list);

        for (int i = 0; i < 20; i++) {
            System.out.println("插入数据：" + i);
            client.set("key" + i,0,"value"+i);
            Thread.sleep(1000);
        }
        client.shutdown();
        /*
        *演示路由算法
        *通过telnet方式访问三个节点中的Key，可以发现数据分散在各个端口上，
        * 此方法演示官方的版本的memcached 节点之间都是独立的，这样导致的每次取数据时只能到特定的节点去取
        * */

    }









}

class Student implements Serializable{
    //一定要继承序列化接口，才可以网络传输

}