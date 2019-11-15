package onthego.database.core.sqlprocessor.value;

import onthego.database.core.sqlprocessor.expression.Expression;
import onthego.database.core.sqlprocessor.expression.ExpressionEvaluationException;
import onthego.database.core.table.Cursor;

public abstract class Value {
	
	public static enum Type {STRING, NUMBER, BOOLEAN, ID, NULL};
	
	protected Type type;
	
	protected Value(Type type) {
		this.type = type;
	}
	
	public Type getType() {
		return this.type;
	}
	
	public static String evaluate(Expression expr, Cursor[] tables) throws ExpressionEvaluationException {
		return expr.evaluate(tables).toString();
	}
	
}
