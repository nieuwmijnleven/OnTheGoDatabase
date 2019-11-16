package onthego.database.core.table.meta;

public class ConstType extends Type {
	
	public ConstType() {
		super(TypeConstants.CONST);
	}

	@Override
	protected String generateValuePatternString(int length, int decimalLength) {
		return "";
	}
}
