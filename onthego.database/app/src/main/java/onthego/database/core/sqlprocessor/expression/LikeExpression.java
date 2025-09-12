package onthego.database.core.sqlprocessor.expression;

import onthego.database.core.sqlprocessor.value.BooleanValue;
import onthego.database.core.sqlprocessor.value.StringValue;
import onthego.database.core.sqlprocessor.value.Value;
import onthego.database.core.table.Cursor;

public final class LikeExpression implements Expression {
	
	private final Expression lhs;
	
	private final Expression rhs;

	public LikeExpression(Expression lhs, Expression rhs) {
		this.lhs = lhs;
		this.rhs = rhs;
	}

	@Override
	public Value evaluate(Cursor[] cursor) throws ExpressionEvaluationException {
		Value lValue = lhs.evaluate(cursor);
		Value rValue = rhs.evaluate(cursor);
		
		if (lValue.getType() == Value.Type.STRING && rValue.getType() == Value.Type.STRING) {
			String target = ((StringValue)lValue).getValue();
			String pattern = ((StringValue)rValue).getValue();
			return new BooleanValue(target.matches(pattern.replaceAll("%", ".*")));
		}
		
		throw new ExpressionEvaluationException("The type of operands is required to be String type.");
	}
}
