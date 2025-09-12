package onthego.database.core.sqlprocessor.expression;

import onthego.database.core.sqlprocessor.value.BooleanValue;
import onthego.database.core.sqlprocessor.value.Value;
import onthego.database.core.table.Cursor;

public interface Expression {
	
	class LogicalOperator { private LogicalOperator(){} }

    LogicalOperator OR = new LogicalOperator();
	LogicalOperator AND = new LogicalOperator();
	
	class RelationalOperator { private RelationalOperator(){} }

    RelationalOperator LT = new RelationalOperator();
	RelationalOperator GT = new RelationalOperator();
	RelationalOperator EQ = new RelationalOperator();
	RelationalOperator NE = new RelationalOperator();
	RelationalOperator LTE = new RelationalOperator();
	RelationalOperator GTE = new RelationalOperator();
	
	class ArithmeticOperator { private ArithmeticOperator(){} }

    ArithmeticOperator ADD = new ArithmeticOperator();
	ArithmeticOperator SUB = new ArithmeticOperator();
	ArithmeticOperator MUL = new ArithmeticOperator();
	ArithmeticOperator DIV = new ArithmeticOperator();
	ArithmeticOperator REM = new ArithmeticOperator();
	
	Value evaluate(Cursor[] cursor) throws ExpressionEvaluationException;
	
	class FalseExpression implements Expression {
		@Override
		public Value evaluate(Cursor[] cursor) throws ExpressionEvaluationException {
			return new BooleanValue(false);
		}
	}
	
	class TrueExpression implements Expression {
		@Override
		public Value evaluate(Cursor[] cursor) throws ExpressionEvaluationException {
			return new BooleanValue(true);
		}
	}
	
	Expression NULL_EXPRESSION = new FalseExpression();
	Expression TRUE_EXPRESSION = new TrueExpression();
	Expression FALSE_EXPRESSION = new FalseExpression();
}
