package onthego.database.core.sqlprocessor.expression;

import onthego.database.core.sqlprocessor.value.BooleanValue;
import onthego.database.core.sqlprocessor.value.NumberValue;
import onthego.database.core.sqlprocessor.value.StringValue;
import onthego.database.core.sqlprocessor.value.Value;
import onthego.database.core.table.Cursor;

public class RelationalExpression implements Expression {
	
	private final RelationalOperator op;
	
	private final Expression lhs;
	
	private final Expression rhs;

	public RelationalExpression(RelationalOperator op, Expression lhs, Expression rhs) {
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
			
			if (op == Expression.EQ) {
				return new BooleanValue(l == r);
			} else if (op == Expression.NE) {
				return new BooleanValue(l != r);
			} else if (op == Expression.GT) {
				return new BooleanValue(l > r);
			} else if (op == Expression.LT) {
				return new BooleanValue(l < r);
			} else if (op == Expression.GTE) {
				return new BooleanValue(l >= r);
			} else if (op == Expression.LTE) {
				return new BooleanValue(l <= r);
			} else {
				throw new ExpressionEvaluationException("Number values cannot use this relational operator.");
			}
		} else if (lValue.getType() == Value.Type.STRING && rValue.getType() == Value.Type.STRING) {
			String l = ((StringValue)lValue).getValue();
			String r = ((StringValue)rValue).getValue();
			
			if (op == Expression.EQ) {
				return new BooleanValue(l.equals(r));
			} else if (op == Expression.NE) {
				return new BooleanValue(!l.equals(r));
			} else {
				throw new ExpressionEvaluationException("String values can only use '=' or '<>' relational operators.");
			}
		} else if (lValue.getType() == Value.Type.NULL && rValue.getType() == Value.Type.NULL) {
			if (op == Expression.EQ) {
				return new BooleanValue(true);
			} else if (op == Expression.NE) {
				return new BooleanValue(false);
			} else {
				throw new ExpressionEvaluationException("Null values can only use '=' or '<>' relational operators.");
			}
		} else if (lValue.getType() == Value.Type.BOOLEAN && rValue.getType() == Value.Type.BOOLEAN) {
			boolean l = ((BooleanValue)lValue).getValue();
			boolean r = ((BooleanValue)rValue).getValue();
			
			if (op == Expression.EQ) {
				return new BooleanValue(l == r);
			} else if (op == Expression.NE) {
				return new BooleanValue(l != r);
			} else {
				throw new ExpressionEvaluationException("Boolean values can only use '=' or '<>' relational operators.");
			}
		}
		
		throw new ExpressionEvaluationException("The type of operands is required to be String, Null, Boolean type.");
	}
}
