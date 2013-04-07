package org.apache.hadoop.hbase.regionserver;

import org.apache.log4j.PropertyConfigurator;

import java.io.InputStream;
import java.util.Properties;

/**
 * User: IvyTang
 * Date: 13-4-2
 * Time: 下午6:18
 */
public class Log4jProperties {
    static private Properties properties = new Properties();

    static public void init() {

        loadProperties("hbasescannerlog4j.properties");
        PropertyConfigurator.configure(Log4jProperties.getProperties());
    }

    private static void loadProperties(String fileName) {
        try {
            Properties temp = new Properties();
            InputStream is = Log4jProperties.class.getClassLoader().getResourceAsStream(fileName);
            temp.load(is);

            for (Object key : temp.keySet()) {
                properties.setProperty((String) key, temp.getProperty((String) key));
            }
        } catch (Exception e) {
            System.out.println("inti failed" + e);
        }
    }

    private static Properties getProperties() {
        return properties;
    }
}
