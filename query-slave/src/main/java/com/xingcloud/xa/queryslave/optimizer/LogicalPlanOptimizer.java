package com.xingcloud.xa.queryslave.optimizer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.expression.*;
import org.apache.drill.common.logical.JSONOptions;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.logical.OperatorGraph;
import org.apache.drill.common.logical.data.*;
import org.apache.drill.common.logical.graph.AdjacencyList;
import org.apache.hadoop.hbase.util.Pair;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-3-13
 * Time: 上午11:00
 * To change this template use File | Settings | File Templates.
 */
public class LogicalPlanOptimizer implements PlanOptimizer {

    private static Logger logger = LoggerFactory.getLogger(LogicalPlanOptimizer.class);

    private static LogicalPlanOptimizer instance = new LogicalPlanOptimizer();

    private LogicalPlanOptimizer() {
    }

    public static LogicalPlanOptimizer getInstance() {
        return instance;
    }

    @Override
    public LogicalPlan optimize(LogicalPlan plan) throws IOException {
        LogicalPlan optimizedPlan = optimizeLogicalPlanStructure(plan);
        optimizedPlan = combineLogicalOperators(optimizedPlan);
        return optimizedPlan;
    }

    private LogicalPlan combineLogicalOperators(LogicalPlan plan) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        OperatorGraph graph = plan.getGraph();
        Collection<SourceOperator> sources = graph.getSources();
        List<SourceOperator> newSources = new ArrayList<SourceOperator>();
        for (SourceOperator source : sources) {
            if (source instanceof Scan) {
                String se = ((Scan) source).getStorageEngine();
                if (se.equals("mysql")) {
                    JSONOptions selection = null;
                    Filter filter = null;
                    /* Combine filter and scan to mysql scanner */
                    for (LogicalOperator operator : source) {
                        if (operator instanceof Filter) {
                            /* Should have only one filter */
                            filter = (Filter) operator;
                            String sql = changeToSQL((Scan) source, filter);
                            selection = mapper.readValue(new String("{\"sql\":\"" + sql + "\"}").getBytes(), JSONOptions.class);
                        }
                    }

                    if (filter != null) {
                        source = new Scan(((Scan) source).getStorageEngine(), selection, ((Scan) source).getOutputReference());
                        for (LogicalOperator child : filter) {
                            if (child instanceof Join) {
                                if (((Join) child).getLeft() == filter) {
                                    ((Join) child).setLeft(source);
                                } else if (((Join) child).getRight() == filter) {
                                    ((Join) child).setRight(source);
                                }
                            } else if (child instanceof SingleInputOperator) {
                                ((SingleInputOperator) child).setInput(source);
                            }
                        }
                    }

                } else if (se.equals("hbase")) {
                    /* Combine filter and scan to hbase scanner */
                    Filter filter = null;
                    JSONOptions selection = null;
                    for (LogicalOperator operator : source) {
                        if (operator instanceof Filter) {
                            /* Should have only one filter */
                            filter = (Filter) operator;
                            LogicalExpression expr = filter.getExpr();
                            JSONObject hbaseScanInfo = getInitHBaseScanInfo();
                            getHBaseScanInfo(expr, hbaseScanInfo);
                            selection = mapper.readValue(hbaseScanInfo.toJSONString().getBytes(), JSONOptions.class);
                        }
                    }
                    if (filter != null) {
                        source = new Scan(((Scan) source).getStorageEngine(), selection, ((Scan) source).getOutputReference());
                        for (LogicalOperator child : filter) {
                            if (child instanceof Join) {
                                if (((Join) child).getLeft() == filter) {
                                    ((Join) child).setLeft(source);
                                } else if (((Join) child).getRight() == filter) {
                                    ((Join) child).setRight(source);
                                }
                            } else if (child instanceof SingleInputOperator) {
                                ((SingleInputOperator) child).setInput(source);
                            }
                        }
                    }

                }
                newSources.add(source);
            }
        }
        List<LogicalOperator> operators = getLogicalOperatorsFromSource(newSources);
        return new LogicalPlan(plan.getProperties(), plan.getStorageEngines(), operators);
    }

    private JSONObject getInitHBaseScanInfo() {
        JSONObject hbaseScanInfo = new JSONObject();
        for (int i=0; i<5; i++) {
            hbaseScanInfo.put("l"+i, "*");
        }
        return hbaseScanInfo;
    }

    private void getHBaseScanInfo(LogicalExpression expr, JSONObject scanInfo) {
        if (expr instanceof FunctionCall) {
            ImmutableList<LogicalExpression> args = ((FunctionCall) expr).args;
            FunctionDefinition definition = ((FunctionCall) expr).getDefinition();
            if (args.get(0) instanceof FieldReference) {
                String ref = ((FieldReference) args.get(0)).getPath().toString();
                String[] tableAndAttr = ref.split("\\.");
                if (tableAndAttr[1].equals("l0")) {
                    String value = ((ValueExpressions.QuotedString) args.get(1)).value;
                    scanInfo.put("l0", value);
                } else if (tableAndAttr[1].equals("l1")) {
                    String value = ((ValueExpressions.QuotedString) args.get(1)).value;
                    scanInfo.put("l1", value);
                } else if (tableAndAttr[1].equals("l2")) {
                    String value = ((ValueExpressions.QuotedString) args.get(1)).value;
                    scanInfo.put("l2", value);
                } else if (tableAndAttr[1].equals("l3")) {
                    String value = ((ValueExpressions.QuotedString) args.get(1)).value;
                    scanInfo.put("l3", value);
                } else if (tableAndAttr[1].equals("l4")) {
                    String value = ((ValueExpressions.QuotedString) args.get(1)).value;
                    scanInfo.put("l4", value);
                } else if (tableAndAttr[1].equals("date")) {
                    String value = ((ValueExpressions.QuotedString) args.get(1)).value;
                    if (definition.getName().equals("less than or equal to")) {
                        scanInfo.put("startDate", value);
                    } else if (definition.getName().equals("greater than or equal to")) {
                        scanInfo.put("endDate", value);
                    } else if (definition.getName().equals("equal")) {
                        scanInfo.put("startDate", value);
                        scanInfo.put("endDate", value);
                    } else {
                        throw new DrillRuntimeException("Can't parse hbase scan info " + definition.getName());
                    }
                }
            }
            if (args.get(0) instanceof FunctionCall) {
                getHBaseScanInfo(args.get(0), scanInfo);
            }
            if (args.get(1) instanceof FunctionCall) {
                getHBaseScanInfo(args.get(1), scanInfo);
            }
        }
    }


    private List<LogicalOperator> getLogicalOperatorsFromSource(List<SourceOperator> sources) {
        Set<LogicalOperator> dup = new HashSet<LogicalOperator>();
        List<LogicalOperator> operators = new ArrayList<LogicalOperator>();
        for (SourceOperator sourceOperator : sources) {
            addLogicalOperatorChildren(sourceOperator, dup, operators);
            operators.add(sourceOperator);
        }
        return operators;
    }

    private void addLogicalOperatorChildren(LogicalOperator operator, Set<LogicalOperator> dup, List<LogicalOperator> operators) {
        for (LogicalOperator child : operator) {
            if (!dup.contains(child)) {
                operators.add(child);
                dup.add(child);
                addLogicalOperatorChildren(child, dup, operators);
            }
        }
    }

    private LogicalPlan optimizeLogicalPlanStructure(LogicalPlan plan) {
        OperatorGraph graph = plan.getGraph();
        AdjacencyList<OperatorGraph.OpNode> adjacencyList = graph.getAdjList();

        List<LogicalOperator> operators = new ArrayList<LogicalOperator>();
        OperatorGraph.OpNode filterNode = null;
        /* Find filter */
        for (OperatorGraph.OpNode opNode : adjacencyList.getNodeSet()) {
            if (opNode.getNodeValue() instanceof Filter) {
                filterNode = opNode;
            } else {
                operators.add(opNode.getNodeValue());
            }
        }

        if (filterNode != null) {
            Filter filter =  (Filter) filterNode.getNodeValue();

            /* Check if parent is source operator */
            LogicalOperator filterParent = filter.getInput();
            if (!(filterParent instanceof SourceOperator)) {
                /* Pick up it, so it can follow source operator */
                Collection<SourceOperator> sources = graph.getSources();
                for (SourceOperator source : sources) {
                    LogicalExpression newLogicalExpr = getLogicalExpr(filter, (Scan) source);
                    Filter optimizedFilter = new Filter(newLogicalExpr);
                    optimizedFilter.setInput(source);
                    operators.add(optimizedFilter);

                    for (LogicalOperator children : source) {
                        if (children instanceof SingleInputOperator) {
                            ((SingleInputOperator) children).setInput(optimizedFilter);
                        } else if (children instanceof Join){
                            /* Join condition should match join relation order, a inner join b on a.val=b.val */
                            JoinCondition condition = ((Join) children).getConditions()[0];
                            String rootPathOfCondition = ((FieldReference)condition.getLeft()).getRootSegment().
                                    getNameSegment().getPath().toString();
                            String rootPathOfRelation = ((Scan) source).getOutputReference().getPath().toString();
                            if (rootPathOfCondition.equals(rootPathOfRelation)) {
                                ((Join) children).setLeft(optimizedFilter);
                            } else  {
                                ((Join) children).setRight(optimizedFilter);
                            }

                        }
                    }

                    for (LogicalOperator filterChild : filter) {
                        if (filterChild instanceof SingleInputOperator) {
                            ((SingleInputOperator) filterChild).setInput(filterParent);
                        }
                    }

                }
            } else {
                /* Don't need any optimization */
                operators.add(filter);
            }

        }

        return new LogicalPlan(plan.getProperties(), plan.getStorageEngines(), operators);
    }

    private LogicalExpression getLogicalExpr(Filter filter, Scan scan) {
        LogicalExpression logicalExpr = filter.getExpr();
        String tableName = scan.getOutputReference().getPath().toString();
        LogicalExpression simplifiedExpr = removeExtraExpression(logicalExpr, tableName).getSecond();
        return simplifiedExpr;
    }

    private Pair<Boolean, LogicalExpression> removeExtraExpression(LogicalExpression logicalExpression, String tableName) {
        if (logicalExpression instanceof FunctionCall) {
            ImmutableList<LogicalExpression> argsTmp = ((FunctionCall) logicalExpression).args;
            Pair<Boolean, LogicalExpression> left = removeExtraExpression(argsTmp.get(0), tableName);
            Pair<Boolean, LogicalExpression> right = removeExtraExpression(argsTmp.get(1), tableName);

            if (left.getSecond() instanceof FunctionCall && right.getSecond() instanceof FunctionCall) {
                if (left.getFirst() && right.getFirst()) {
                    return new Pair<Boolean, LogicalExpression>(true, logicalExpression);
                } else if (left.getFirst()) {
                    return left;
                } else {
                    return right;
                }
            } else if (left.getFirst() && right.getFirst()) {
                return new Pair<Boolean, LogicalExpression>(true, logicalExpression);
            } else {
                return new Pair<Boolean, LogicalExpression>(false, logicalExpression);
            }

        } else if (logicalExpression instanceof FieldReference) {
            PathSegment ps = ((FieldReference) logicalExpression).getRootSegment();
            boolean belongTo = ps.getNameSegment().getPath().toString().equals(tableName);
            Pair<Boolean, LogicalExpression> pair = new Pair<Boolean, LogicalExpression>(belongTo, logicalExpression);
            return pair;
        } else if (logicalExpression instanceof ValueExpressions.BooleanExpression) {
            return new Pair<Boolean, LogicalExpression>(true, logicalExpression);
        } else if (logicalExpression instanceof ValueExpressions.DoubleExpression) {
            return new Pair<Boolean, LogicalExpression>(true, logicalExpression);
        } else if (logicalExpression instanceof ValueExpressions.LongExpression) {
            return new Pair<Boolean, LogicalExpression>(true, logicalExpression);
        } else if (logicalExpression instanceof ValueExpressions.QuotedString) {
            return new Pair<Boolean, LogicalExpression>(true, logicalExpression);
        }
        throw new DrillRuntimeException("Can't parse Logical Expression: " + logicalExpression);
    }

    private String changeToSQL(Scan scan, Filter filter) {
        String dataBaseName = scan.getOutputReference().getPath().toString();
        LogicalExpression expr = filter.getExpr();
        Set<String> tables = new HashSet<String>();
        String where = getSQLInfoFilter(tables, expr);
        StringBuilder sql = new StringBuilder("SELECT ");

        boolean firstTime = true;
        for (String table : tables) {
            if(!firstTime) {
                sql.append(",");
            }
            sql.append(table).append(".").append("uid");
            sql.append(",");
            sql.append(table).append(".").append("val");
            firstTime = false;
        }
        sql.append(" FROM ");
        firstTime = true;
        for (String table : tables) {
            if (!firstTime) {
                sql.append(",");
            }
            sql.append(table);
        }
        sql.append(" WHERE ");
        sql.append(where).append(";");

        System.out.println(sql.toString());
        return sql.toString();
    }

    private String getSQLInfoFilter(Set<String> tables, LogicalExpression expr) {
        if (expr instanceof FunctionCall) {
            /* Right now we just have binary operator */
            ImmutableList<LogicalExpression> argsTmp = ((FunctionCall) expr).args;
            FunctionDefinition definition = ((FunctionCall) expr).getDefinition();
            String left = getSQLInfoFilter(tables, argsTmp.get(0));
            String right = getSQLInfoFilter(tables, argsTmp.get(1));
            if (definition.getName().equals("and")) {
                return left +  " AND " + right;
            } else if (definition.getName().equals("or")) {
                return left + " OR " + right;
            } else if (definition.getName().equals("equal")) {
                return left + "=" + right;
            } else if (definition.getName().equals("greater than")) {
                return left + ">" + right;
            } else if (definition.getName().equals("greater than or equal to")) {
                return left + ">=" + right;
            } else if (definition.getName().equals("less than")) {
                return left + "<" + right;
            } else if (definition.getName().equals("less than or equal to")) {
                return left + "<=" + right;
            }
        } else if (expr instanceof FieldReference) {

            String ref = ((FieldReference) expr).getPath().toString();
            String tableName = ref.split("\\.")[1];
            tables.add(tableName);
            return tableName + ".val";
        } else if (expr instanceof ValueExpressions.LongExpression) {
            return String.valueOf(((ValueExpressions.LongExpression) expr).getLong());
        } else if (expr instanceof ValueExpressions.QuotedString) {
            return "'" + ((ValueExpressions.QuotedString) expr).value + "'";
        } else if (expr instanceof ValueExpressions.DoubleExpression) {
            return String.valueOf(((ValueExpressions.DoubleExpression) expr).getDouble());
        }
        throw new DrillRuntimeException("Can't parse LogicalExpression " + expr + " to sql!");
    }

}
