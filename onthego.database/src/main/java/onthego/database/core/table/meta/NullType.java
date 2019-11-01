package onthego.database.core.table.meta;

import java.io.DataOutput;
import java.io.IOException;

public class NullType extends Type {
	
	public NullType() {
		super(TypeConstants.CONST);
	}

	@Override
	protected String generateValuePatternString(int length, int decimalLength) {
		return "";
	}
//	
//	@Override
//	public void sendDataWriteRequest(DataOutput out, Object value) throws IOException {
//		throw new UnsupportedOperationException();
//	}
}
