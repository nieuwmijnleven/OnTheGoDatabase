package onthego.database.core.sqlprocessor.expression;

import java.text.ParseException;

import onthego.database.core.sqlprocessor.value.IdValue;
import onthego.database.core.sqlprocessor.value.Value;
import onthego.database.core.table.Cursor;

public class AtomicExpression implements Expression {
	
	private Value value;
	
	public AtomicExpression(Value value) {
		this.value = value;
	}

	@Override
	public Value evaluate(Cursor[] cursor) throws ExpressionEvaluationException {
		if (value.getType() == Value.Type.ID) {
			try {
				return ((IdValue)value).getValue(cursor);
			} catch (ParseException e) {
				throw new ExpressionEvaluationException("failed to evaluate id value.");
			}
		}
		return value;
	}
}
