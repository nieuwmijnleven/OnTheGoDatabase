package onthego.database.core.table.meta;

import java.io.DataOutput;
import java.io.IOException;

public class CharType extends Type {
	
	public CharType(int length) {
		super(TypeConstants.CHAR, length);
	}

	@Override
	protected String generateValuePatternString(int length, int decimalLength) {
		if (length == 0) {
			return "^.*$";
		} else if (length > 0) {
			return "^.{1," + length + "}$";
		} else {
			throw new TypeException("The type length(" + length + ") is not valid.");
		}
	}
	
//	@Override
//	public void sendDataWriteRequest(DataOutput out, Object value) throws IOException {
//		if (!(value instanceof String)) {
//			throw new RuntimeException("The value is not a proper type.");
//		}
//		
//		out.writeUTF((String)value);
//	}
}
