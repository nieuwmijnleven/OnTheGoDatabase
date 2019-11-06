package onthego.database.core.sqlprocessor.expression;

import onthego.database.core.sqlprocessor.value.BooleanValue;
import onthego.database.core.sqlprocessor.value.Value;
import onthego.database.core.table.Cursor;

public interface Expression {
	
	public static class LogicalOperator { private LogicalOperator(){} };
	public static final LogicalOperator OR = new LogicalOperator();
	public static final LogicalOperator AND = new LogicalOperator();
	
	public static class RelationalOperator { private RelationalOperator(){} };
	public static final RelationalOperator LT = new RelationalOperator();
	public static final RelationalOperator GT = new RelationalOperator();
	public static final RelationalOperator EQ = new RelationalOperator();
	public static final RelationalOperator NE = new RelationalOperator();
	public static final RelationalOperator LTE = new RelationalOperator();
	public static final RelationalOperator GTE = new RelationalOperator();
	
	public static class ArithmeticOperator { private ArithmeticOperator(){} };
	public static final ArithmeticOperator ADD = new ArithmeticOperator();
	public static final ArithmeticOperator SUB = new ArithmeticOperator();
	public static final ArithmeticOperator MUL = new ArithmeticOperator();
	public static final ArithmeticOperator DIV = new ArithmeticOperator();
	public static final ArithmeticOperator REM = new ArithmeticOperator();
	
	Value evaluate(Cursor[] cursor) throws ExpressionEvaluationException;
	
	public static class NullExpression implements Expression {
		@Override
		public Value evaluate(Cursor[] cursor) throws ExpressionEvaluationException {
			return new BooleanValue(false);
		}
	}
	
	public static final Expression NULL_EXPRESSION = new NullExpression();
}
