package com.xingcloud.xa.queryslave.parser;

import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.LogicalExpression;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;


/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-2-21
 * Time: 上午11:00
 * To change this template use File | Settings | File Templates.
 */
public class AdhocSelectItemVisitorImpl implements SelectItemVisitor {

    private LogicalExpression logicalExpr;
    private boolean isDistinct;

    public LogicalExpression getLogicalExpr() {
        return logicalExpr;
    }

    @Override
    public void visit(AllColumns allColumns) {
        logicalExpr = new FieldReference(allColumns.toString());
    }

    @Override
    public void visit(AllTableColumns allTableColumns) {
        logicalExpr = new FieldReference(allTableColumns.toString());
    }

    @Override
    public void visit(SelectExpressionItem selectExpressionItem) {
        Expression expression = selectExpressionItem.getExpression();
        AdhocExpressionVisitorImpl exprVisitor = new AdhocExpressionVisitorImpl();
        expression.accept(exprVisitor);
        logicalExpr = exprVisitor.getLogicalExpression();
        isDistinct = exprVisitor.isDistinct();
    }

    public boolean isDistinct() {
        return isDistinct;
    }
}
