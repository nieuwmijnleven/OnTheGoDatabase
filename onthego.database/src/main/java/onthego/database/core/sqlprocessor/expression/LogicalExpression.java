package onthego.database.core.sqlprocessor.expression;

import onthego.database.core.sqlprocessor.value.BooleanValue;
import onthego.database.core.sqlprocessor.value.Value;
import onthego.database.core.table.Cursor;

public class LogicalExpression implements Expression {
	
	private LogicalOperator op;
	
	private Expression lhs;
	
	private Expression rhs;

	public LogicalExpression(LogicalOperator op, Expression lhs, Expression rhs) {
		this.op = op;
		this.lhs = lhs;
		this.rhs = rhs;
	}

	@Override
	public Value evaluate(Cursor[] cursor) throws ExpressionEvaluationException {
		Value lValue = lhs.evaluate(cursor);
		Value rValue = rhs.evaluate(cursor);
		
		if (lValue.getType() == Value.Type.BOOLEAN && rValue.getType() == Value.Type.BOOLEAN) {
			boolean l = ((BooleanValue)lValue).getValue();
			boolean r = ((BooleanValue)rValue).getValue();
			
			if (op == Expression.AND) {
				return new BooleanValue(l && r);
			} else {
				return new BooleanValue(l || r);
			}
		}
		
		throw new ExpressionEvaluationException("The type of operands is required to be Boolean.");
	}
}
