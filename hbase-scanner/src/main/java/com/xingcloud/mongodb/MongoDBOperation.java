package com.xingcloud.mongodb;

import com.google.common.base.Strings;
import com.mongodb.*;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 12-8-14
 * Time: 下午5:56
 * To change this template use File | Settings | File Templates.
 */
public class MongoDBOperation {
    private static Log logger = LogFactory.getLog(MongoDBOperation.class);

    public static Set<String> getEventSet(String pID, String eventFilter) throws IOException {
        long startTime = System.nanoTime();

        String[] levelArray = split2LevelArray(eventFilter);
        if (levelArray == null || levelArray.length == 0) {
            throw new IOException("Invalid filter pattern: " + eventFilter);
        }

        Map<String, Object> condition = new HashMap<String, Object>();
        condition.put("project_id", pID);
        for (int i = 0; i < levelArray.length; i++) {
            if (!Strings.isNullOrEmpty(levelArray[i])) {
                if (levelArray[i].contains("%")) {
                    levelArray[i] = levelArray[i].replace("%", ".*");
                    Pattern pattern = Pattern.compile(levelArray[i]);
                    condition.put("l" + i, pattern);
                } else {
                    condition.put("l" + i, levelArray[i]);
                }
            }
        }

        DBCollection collection = MongoResourceManager.getInstance().getEventListColl();
        DBObject queryObj = new BasicDBObject(condition.size());
        if (condition != null && !condition.isEmpty()) {
            queryObj.putAll(condition);
        }

        DBCursor cursor = collection.find(queryObj).limit(50000);

        List<DBObject> queryResult = cursor.toArray();
        if (CollectionUtils.isEmpty(queryResult)) {
            return null;
        }

        Set<String> eventSet = new HashSet<String>();
        String tmpL0 = null;
        String tmpL1 = null;
        String tmpL2 = null;
        String tmpL3 = null;
        String tmpL4 = null;
        String tmpL5 = null;
        for (DBObject obj : queryResult) {
            tmpL0 = object2String(obj.get("l0"));
            tmpL1 = object2String(obj.get("l1"));
            tmpL2 = object2String(obj.get("l2"));
            tmpL3 = object2String(obj.get("l3"));
            tmpL4 = object2String(obj.get("l4"));
            tmpL5 = object2String(obj.get("l5"));
            String event = tmpL0 + ".";
            if (tmpL1 != null) {
                event += tmpL1 + ".";

                if (tmpL2 != null) {
                    event += tmpL2 + ".";

                    if (tmpL3 != null) {
                        event += tmpL3 + ".";

                        if (tmpL4 != null) {
                            event += tmpL4 + ".";

                            if (tmpL5 != null) {
                                event += tmpL5 + ".";
                            }
                        }
                    }
                }
            }
            eventSet.add(event);
        }

        printSummary(pID, eventFilter, eventSet, startTime);
        return eventSet;
    }

    private static void printSummary(String pID, String eventFilter, Set<String> eventSet, long startTime) {
        String summary = "------Get event list: \n" + " pID: " + pID + "\n Event: " + eventFilter + "\n Event size: " + eventSet.size() +
                "\n Taken: " + (System.nanoTime() - startTime) / 1.0e9 + "\n";
        logger.info(summary);
    }


    private static String object2String(Object o) {
        return o == null ? null : o.toString();
    }

    public static String truncateAllStar(String star) {
        if (star == null || !star.endsWith(".*")) {
            return star;
        }
        return truncateAllStar(star.substring(0, star.lastIndexOf(".*")));
    }

    public static String[] split2LevelArray(String event) {
        if (Strings.isNullOrEmpty(event)) {
            return null;
        }
        event = truncateAllStar(event);
        String[] event6LevenArray = event.split("\\.");
        if (event6LevenArray == null || event6LevenArray.length == 0) {
            return null;
        }
        for (int i = 0; i < event6LevenArray.length; i++) {
            if ("*".equals(event6LevenArray[i])) {
                event6LevenArray[i] = null;
            }
        }
        return event6LevenArray;
    }




}
