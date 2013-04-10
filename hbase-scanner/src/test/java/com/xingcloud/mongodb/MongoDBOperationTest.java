package com.xingcloud.mongodb;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-4-10
 * Time: 下午3:28
 * To change this template use File | Settings | File Templates.
 */


import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class MongoDBOperationTest {

  @Test
  public void testGetEventSet() {
    String eventFilter = "visit.*";
    String pID = "xa_test";
    try {
      Set<String> events = MongoDBOperation.getEventSet(pID, eventFilter);
      String[] expect = {"visit.", "visit.a1.", "visit.a2.", "visit.a3.", "visit.a1.b1.", "visit.a1.b2."};
      String[] results = events.toArray(new String[0]);
      Arrays.sort(expect);
      Arrays.sort(results);

      assertArrayEquals(expect, results);
    } catch (IOException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }


  }
}
