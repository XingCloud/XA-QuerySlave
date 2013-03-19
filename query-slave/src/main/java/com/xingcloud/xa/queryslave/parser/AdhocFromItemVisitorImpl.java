package com.xingcloud.xa.queryslave.parser;


import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.drill.common.expression.*;
import org.apache.drill.common.logical.JSONOptions;
import org.apache.drill.common.logical.data.*;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-2-20
 * Time: 上午11:45
 * To change this template use File | Settings | File Templates.
 */
public class AdhocFromItemVisitorImpl implements FromItemVisitor {

    private LogicalOperator lop;
    private LogicalExpression where;


    public AdhocFromItemVisitorImpl(LogicalExpression where){
        super();
        this.where = where;
    }

    @Override
    public void visit(Table table) {
        String tableName = table.getName();
        FieldReference fr = new FieldReference(tableName);
        String storageEngine = null;
        JSONOptions selection =  null;
        ObjectMapper mapper = new ObjectMapper();
        try{
            if (tableName.contains("deu")){
                storageEngine = "hbase";
                String _selection = String.format(new String("{\"hbase\":\"None\"}"));
                selection = mapper.readValue(_selection.getBytes(), JSONOptions.class);
            }else{
                storageEngine="mysql";
                selection = mapper.readValue(new String("{\"sql\":\"None\"}").getBytes(), JSONOptions.class);
            }
        }catch (Exception e){
            e.printStackTrace();
            System.out.println(e.getMessage());
        }

        lop = new Scan(storageEngine,selection,fr);

    }

    @Override
    public void visit(SubSelect subSelect) {
        AdhocSQLQueryVisitorImpl visitor = new AdhocSQLQueryVisitorImpl();
        subSelect.getSelectBody().accept(visitor);
        List<LogicalOperator> lops = visitor.getLogicalOperators();
        for (LogicalOperator lopTmp : lops) {
            System.out.println(lopTmp);
        }
    }

    @Override
    public void visit(SubJoin subJoin) {
        FromItem left = subJoin.getLeft();
        FromItem right = subJoin.getJoin().getRightItem();
        AdhocFromItemVisitorImpl lv = new AdhocFromItemVisitorImpl(where);
        left.accept(lv);
        AdhocFromItemVisitorImpl rv = new AdhocFromItemVisitorImpl(where);
        right.accept(rv);

        LogicalOperator leftLop = lv.getLogicalOperator();
        LogicalOperator rightLop = rv.getLogicalOperator();

        Expression expr = subJoin.getJoin().getOnExpression();
        JoinCondition[] jcs = null;
        if (expr != null) {
            AdhocExpressionVisitorImpl exprVisitor = new AdhocExpressionVisitorImpl();
            expr.accept(exprVisitor);
            FunctionCall funcExpr = (FunctionCall) exprVisitor.getLogicalExpression();
            List<LogicalExpression> args = funcExpr.args;
            String relationship = getRelationship(funcExpr.getDefinition().getName());
            String rootPath = ((FieldReference)args.get(0)).getRootSegment().
                    getNameSegment().getPath().toString();

            JoinCondition jc = null;
            /* Join condition should match join relation order */
            if (rootPath.equals(left.toString())) {
               jc = new JoinCondition(relationship, args.get(0), args.get(1));
            } else if (rootPath.equals(right.toString())) {
               jc = new JoinCondition(relationship, args.get(1), args.get(0));
            }

            jcs = new JoinCondition[1];
            jcs[0] = jc;
        } else {
            jcs = new JoinCondition[0];
        }
        lop = new Join(leftLop, rightLop, jcs, returnJoinType(subJoin));
    }

    public LogicalOperator getLogicalOperator() {
        return lop;
    }

    private String returnJoinType(SubJoin subJoin) {
        if (subJoin.getJoin().isInner()) {
            return "INNER";
        } else if (subJoin.getJoin().isNatural()) {
            return "OUTER";
        } else if (subJoin.getJoin().isOuter()) {
            return "OUTER";
        } else if (subJoin.getJoin().isLeft()) {
            return "LEFT";
        }
        //throw new AdhocRuntimeException("Can't parse join type of " + subJoin.toString());
        //throw new Exception(); //wcl
        return "";
    }

    private String getRelationship(String name) {
        if (name.equals("equal")) {
            return "==";
        }
        //throw new AdhocRuntimeException("Can't parse join condition relationship " + name);
        return "";
    }

    private String getMySQLSelection(LogicalExpression where){
        return null;
    }
}
