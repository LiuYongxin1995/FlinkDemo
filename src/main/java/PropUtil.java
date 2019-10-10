import java.io.*;
import java.util.Properties;

public class PropUtil {

    private static Properties prop = null;

    public static Properties loadProperties(String fileName) {
        InputStream in = null;
        try {
            in = PropUtil.class.getResourceAsStream(fileName);
            prop = new Properties();
            prop.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return prop;
    }


    public static String getProperties(String key) {
        return prop.getProperty(key);
    }
}
