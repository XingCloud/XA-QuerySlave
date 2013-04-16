package com.xingcloud.mongodb;

import com.mongodb.*;
import com.xingcloud.basic.conf.ConfigReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.net.UnknownHostException;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 12-8-14
 * Time: 下午5:50
 * To change this template use File | Settings | File Templates.
 */
public class MongoResourceManager {
    private static String DB_NAME;

    private static String USER_COLLECTION = "user_hash";

    private static String EVENT_LIST_COLLECTION = "events_list";

    private static int port;

    private static String host;

    private static int poolSize;

    private Mongo mongo;

    private static Log logger = LogFactory.getLog(MongoResourceManager.class);

    public synchronized static MongoResourceManager getInstance() {
        return InnerHolder.INSTANCE;
    }

    private MongoResourceManager(){
        try {
            init();
        } catch (UnknownHostException e) {
            logger.error("Mongo init got UnknownHostException", e);
        } catch (MongoException e) {
            logger.error("Mongo init got MongoException", e);
        } catch (Exception e) {
          e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    private void init() throws Exception {
        if (mongo == null) {
            host = ConfigReader.getConfig("mongodb_conf.xml", "mongodb", "host");
            port = Integer.parseInt(ConfigReader.getConfig("mongodb_conf.xml",
                    "mongodb", "port"));
            DB_NAME = ConfigReader.getConfig("mongodb_conf.xml", "mongodb", "dbname");
            poolSize = Integer.parseInt(ConfigReader.getConfig("mongodb_conf.xml", "mongodb", "pool_size"));
            int maxWait = Integer.parseInt(ConfigReader.getConfig("mongodb_conf.xml", "mongodb", "max_wait"));
            int socketTimeOut = Integer.parseInt(ConfigReader.getConfig("mongodb_conf.xml", "mongodb", "socket_timeout"));
            int connectionTimeOut =  Integer.parseInt(ConfigReader.getConfig("mongodb_conf.xml", "mongodb", "connection_timeout"));
            int threadsAllowedToBlockForConnectionMultiplier = Integer.parseInt(ConfigReader.getConfig("mongodb_conf.xml", "mongodb", "threads_allowed_to_block_for_connection_multiplier"));

            logger.info("\nHost: " + host + "\nPort: " + port + "\nPool size: " + poolSize + "\nMax wait: " + maxWait + "\nSocket timeout: " + socketTimeOut
                    + "\nConnection time out: " + connectionTimeOut + "\nthreadsAllowedToBlockForConnectionMultiplier: " + threadsAllowedToBlockForConnectionMultiplier);


            MongoOptions options = new MongoOptions();
            options.autoConnectRetry = true;
            options.connectionsPerHost = poolSize;
            options.maxWaitTime = maxWait;
            options.connectTimeout = connectionTimeOut;
            options.socketTimeout = socketTimeOut;
            options.threadsAllowedToBlockForConnectionMultiplier = threadsAllowedToBlockForConnectionMultiplier;

            mongo = new Mongo(new ServerAddress(host, port), options);
        }
    }

    private static class InnerHolder {
        static final MongoResourceManager INSTANCE = new MongoResourceManager();
    }

    public DB getDB() {
        return mongo.getDB(DB_NAME);
    }

    public DB getDB(String dbName) {
        return mongo.getDB(dbName);
    }

    public DBCollection getColl(String dbName, String collName) {
        return mongo.getDB(dbName).getCollection(collName);
    }

    public DBCollection getUidColl() {
        return getDB().getCollection(USER_COLLECTION);
    }

    public DBCollection getEventListColl() {
        return getDB().getCollection(EVENT_LIST_COLLECTION);
    }

    public void destory() {
        if (mongo != null) {
            mongo.close();
        }
    }
}
