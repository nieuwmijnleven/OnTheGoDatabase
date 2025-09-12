package onthego.database.core.sqlprocessor.expression;

import onthego.database.core.sqlprocessor.value.NumberValue;
import onthego.database.core.sqlprocessor.value.Value;
import onthego.database.core.table.Cursor;

public class ArithmeticExpression implements Expression {
	
	private final ArithmeticOperator op;
	
	private final Expression lhs;
	
	private final Expression rhs;

	public ArithmeticExpression(ArithmeticOperator op, Expression lhs, Expression rhs) {
		this.op = op;
		this.lhs = lhs;
		this.rhs = rhs;
	}

	@Override
	public Value evaluate(Cursor[] cursor) throws ExpressionEvaluationException {
		Value lValue = lhs.evaluate(cursor);
		Value rValue = rhs.evaluate(cursor);
		
		if (lValue.getType() == Value.Type.NUMBER && rValue.getType() == Value.Type.NUMBER) {
			double l = ((NumberValue)lValue).getValue();
			double r = ((NumberValue)rValue).getValue();
			
			if (op == Expression.ADD) {
				return new NumberValue(l + r);
			} else if (op == Expression.SUB) {
				return new NumberValue(l - r);
			} else if (op == Expression.MUL) {
				return new NumberValue(l * r);
			} else {//if (op == Expression.DIV)
				return new NumberValue(l / r);
			}
		}
		
		throw new ExpressionEvaluationException("The type of operands is required to be Integer or Numeric.");
	}
}
