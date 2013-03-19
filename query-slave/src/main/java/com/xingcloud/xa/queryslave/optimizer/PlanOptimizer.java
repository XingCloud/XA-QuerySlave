package com.xingcloud.xa.queryslave.optimizer;

import org.apache.drill.common.logical.LogicalPlan;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-3-13
 * Time: 上午10:59
 * To change this template use File | Settings | File Templates.
 */
public interface PlanOptimizer {

    public LogicalPlan optimize(LogicalPlan plan) throws IOException;
}
