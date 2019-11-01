package onthego.database.core.table.meta;

import java.io.DataOutput;
import java.io.IOException;

public class BoolType extends Type {
	
	public BoolType() {
		super(TypeConstants.BOOL);
	}

	@Override
	protected String generateValuePatternString(int length, int decimalLength) {
		return "(true|TRUE|false|FALSE)";
	}
	
//	@Override
//	public void sendDataWriteRequest(DataOutput out, Object value) throws IOException {
//		if (!(value instanceof Boolean)) {
//			throw new RuntimeException("The value is not a proper type.");
//		}
//		
//		out.writeBoolean((Boolean)value);
//	}
}
