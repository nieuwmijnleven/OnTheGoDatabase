package onthego.database.core.table.meta;

public class NullType extends Type {
	
	public NullType() {
		super(TypeConstants.CONST);
	}

	@Override
	protected String generateValuePatternString(int length, int decimalLength) {
		return "";
	}
}
