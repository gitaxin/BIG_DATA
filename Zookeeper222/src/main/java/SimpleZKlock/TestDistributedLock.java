package SimpleZKlock;


import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * @Description:
 * @Author: Axin
 * @Date: Create in 15:45 2019/8/4
 */
public class TestDistributedLock {


    public static void main(String[] args) {
        ExponentialBackoffRetry policy = new ExponentialBackoffRetry(1000, 10);

    }
}
