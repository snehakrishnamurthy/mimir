package mimir.sql;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by prasad on 12/4/16.
 */
public class Feedback implements Statement{
    private SelectBody body;
    private List<Expression> onExpressions = null;
    private Expression expression = null;
    private int columnIndex = -1;
    private Table table = null;

    @Override
    public void accept(StatementVisitor statementVisitor) {
        Select sel = new Select();
        sel.setSelectBody(body);
        statementVisitor.visit(sel);
    }

    public Table getTable(){
        return table;
    }

    public int getColumnIndex(){
        return columnIndex;
    }

    public Expression getExpression(){
        return expression;
    }

    public List<Expression> getOnExpressions() {
        return onExpressions;
    }

    public void setOnExpressions(List<Expression> onExpressions) {
        this.onExpressions = new ArrayList<>(onExpressions);
    }

    public void setColumnIndex(int column){
        this.columnIndex = column;
    }

    public void setExpression(Expression expression){
        this.expression = expression;
    }

    public void setTable(Table table){
        this.table = table;
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder("FEEDBACK "+ table.getName()+" ");
        if(getColumnIndex() != -1){
            sb.append("," + getColumnIndex()+" ");
        }
        if(getOnExpressions() != null){
            sb.append("ON " + getOnExpressions()+ " ");
        }
        if(getExpression() != null){
            sb.append("SET " + getExpression());
        }
        sb.append(";");
        return sb.toString();
    }
}
