package toUpCase;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;

/**
 * @Description:
 * @Author: Axin
 * @Date: Create in 21:31 2019/8/9
 */
public class MyInterceptor implements Interceptor {
    @Override
    public void initialize() {
        System.out.println("================初始化方法================");
    }

    @Override
    public Event intercept(Event event) {
        byte[] body = event.getBody();
        event.setBody(new String(body).toUpperCase().getBytes());
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        //Ctrl + Shift + Alt + S 打开设置
        ArrayList<Event> li = new ArrayList<>();
        for (Event event : list) {
            li.add(intercept(event));
        }
        return li;
    }

    @Override
    public void close() {
        System.out.println("================结束方法================");
    }


    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new MyInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }

}


