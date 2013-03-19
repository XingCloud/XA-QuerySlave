package com.xingcloud.xa.queryslave.parser;

import org.apache.drill.common.expression.FunctionDefinition;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.fn.AggregationFunctions;
import org.apache.drill.common.expression.fn.BooleanFunctions;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.ValueExpressions;


import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;

import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-2-20
 * Time: 下午10:35
 * To change this template use File | Settings | File Templates.
 */
public class AdhocExpressionVisitorImpl implements ExpressionVisitor {

    private LogicalExpression le;
    private boolean isDistinct = false;


    public LogicalExpression getLogicalExpression() {
        return le;
    }

    @Override
    public void visit(Column column) {
        le = new FieldReference(column.getWholeColumnName());
    }

    @Override
    public void visit(DoubleValue doubleValue) {
        le = ValueExpressions.getNumericExpression(doubleValue.toString());
    }

    @Override
    public void visit(LongValue longValue) {
        le = ValueExpressions.getNumericExpression(longValue.toString());
    }

    @Override
    public void visit(StringValue stringValue) {
        le = new ValueExpressions.QuotedString(stringValue.getValue());
    }

    /* Boolean Expressions */
    @Override
    public void visit(EqualsTo equalsTo) {
        visitBinaryBooleanExpression(equalsTo, "equal");
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        visitBinaryBooleanExpression(greaterThan, "greater than");
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        visitBinaryBooleanExpression(greaterThanEquals, "greater than or equal to");
    }

    @Override
    public void visit(MinorThan minorThan) {
        visitBinaryBooleanExpression(minorThan, "less than");
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        visitBinaryBooleanExpression(minorThanEquals, "less than or equal to");
    }

    @Override
    public void visit(AndExpression andExpression) {
        visitBinaryBooleanExpression(andExpression, "and");
    }

    @Override
    public void visit(OrExpression orExpression) {
        visitBinaryBooleanExpression(orExpression, "or");
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {

    }

    private void visitBinaryBooleanExpression(BinaryExpression e, String registeredName) {
        Expression left = e.getLeftExpression();
        ExpressionVisitor leftVisitor = new AdhocExpressionVisitorImpl();
        left.accept(leftVisitor);

        Expression right = e.getRightExpression();
        ExpressionVisitor rightVisitor = new AdhocExpressionVisitorImpl();
        right.accept(rightVisitor);

        List<LogicalExpression> args = new ArrayList<LogicalExpression>();
        args.add(((AdhocExpressionVisitorImpl)leftVisitor).getLogicalExpression());
        args.add(((AdhocExpressionVisitorImpl)rightVisitor).getLogicalExpression());

        FunctionDefinition definition = BooleanFunctions.getFunctionDefintion(registeredName);
        le = new FunctionCall(definition, args);
    }

    public boolean isDistinct() {
        return isDistinct;
    }

    /* Function : COUNT/SUM/DISTINCT */
    @Override
    public void visit(Function function) {
        isDistinct = function.isDistinct();

        String funcName = function.getName();
        List<Expression> expressions = function.getParameters().getExpressions();

        List<LogicalExpression> agrs = new ArrayList<LogicalExpression>();
        for (Expression expr : expressions) {
            AdhocExpressionVisitorImpl exprVisitor = new AdhocExpressionVisitorImpl();
            expr.accept(exprVisitor);
            agrs.add(exprVisitor.getLogicalExpression());
        }

        if (funcName.toLowerCase().equals("count")) {
            FunctionDefinition definition = AggregationFunctions.getFunctionDefintion("count");
            le = new FunctionCall(definition, agrs);
        } else if (funcName.toLowerCase().equals("sum")) {
            FunctionDefinition definition = AggregationFunctions.getFunctionDefintion("sum");
            le = new FunctionCall(definition, agrs);
        }

    }

    @Override
    public void visit(NullValue nullValue) {
        //To change body of implemented methods use File | Settings | File Templates.
    }



    @Override
    public void visit(InverseExpression inverseExpression) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void visit(JdbcParameter jdbcParameter) {
        //To change body of implemented methods use File | Settings | File Templates.
    }



    @Override
    public void visit(DateValue dateValue) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void visit(TimeValue timeValue) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void visit(TimestampValue timestampValue) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void visit(Parenthesis parenthesis) {
        //To change body of implemented methods use File | Settings | File Templates.
    }



    @Override
    public void visit(Addition addition) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void visit(Division division) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void visit(Multiplication multiplication) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void visit(Subtraction subtraction) {
        //To change body of implemented methods use File | Settings | File Templates.
    }



    @Override
    public void visit(Between between) {
        //To change body of implemented methods use File | Settings | File Templates.
    }



    @Override
    public void visit(InExpression inExpression) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void visit(IsNullExpression isNullExpression) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void visit(LikeExpression likeExpression) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void visit(SubSelect subSelect) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void visit(CaseExpression caseExpression) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void visit(WhenClause whenClause) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void visit(ExistsExpression existsExpression) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void visit(AllComparisonExpression allComparisonExpression) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void visit(AnyComparisonExpression anyComparisonExpression) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void visit(Concat concat) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void visit(Matches matches) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void visit(BitwiseAnd bitwiseAnd) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void visit(BitwiseOr bitwiseOr) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void visit(BitwiseXor bitwiseXor) {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
