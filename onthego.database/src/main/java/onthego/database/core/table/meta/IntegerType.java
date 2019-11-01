package onthego.database.core.table.meta;

import java.io.DataOutput;
import java.io.IOException;

public class IntegerType extends Type {

	public IntegerType(int length) {
		super(TypeConstants.INTEGER, length);
	}

	@Override
	protected String generateValuePatternString(int length, int decimalLength) {
		if (length == 0) {
			return "(^([-+]?[1-9]\\d*|0)$)";
		} else if (length > 0) {
			return "(^([-+]?[1-9]\\d{0," + length + "}|0)$)";
		} else {
			throw new TypeException("The type length(" + length + ") is not valid.");
		}
	}
	
//	@Override
//	public void sendDataWriteRequest(DataOutput out, Object value) throws IOException {
//		if (!(value instanceof Integer)) {
//			throw new RuntimeException("The value is not a proper type.");
//		}
//		
//		out.writeInt((Integer)value);
//	}
}
