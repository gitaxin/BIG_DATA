package Main;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


/**
 * @Description:
 * @Author: Axin
 * @Date: Create in 18:02 2019/8/25
 */
public class PropertiesUtil {

    public static Properties properties = new Properties();

    static{
        InputStream is = ClassLoader.getSystemResourceAsStream("conf.properties");
        try {
            properties.load(is);
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    public static String getProperties(String key){
        return properties.getProperty(key);
    }

}
