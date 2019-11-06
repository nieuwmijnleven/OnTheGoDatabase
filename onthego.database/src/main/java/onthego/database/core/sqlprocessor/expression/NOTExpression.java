package onthego.database.core.sqlprocessor.expression;

import onthego.database.core.sqlprocessor.value.BooleanValue;
import onthego.database.core.sqlprocessor.value.Value;
import onthego.database.core.table.Cursor;

public final class NOTExpression implements Expression {
	
	private Expression expression;

	public NOTExpression(Expression expression) {
		this.expression = expression;
	}

	@Override
	public Value evaluate(Cursor[] cursor) throws ExpressionEvaluationException {
		Value value = expression.evaluate(cursor);
		if (value.getType() != Value.Type.BOOLEAN) {
			throw new ExpressionEvaluationException("NOT expression can be only used with a boolean value.");
		}
		
		return new BooleanValue(!((BooleanValue)value).getValue());
	}
}
