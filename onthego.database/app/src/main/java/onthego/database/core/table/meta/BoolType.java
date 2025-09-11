package onthego.database.core.table.meta;

public class BoolType extends Type {
	
	public BoolType() {
		super(TypeConstants.BOOL);
	}

	@Override
	protected String generateValuePatternString(int length, int decimalLength) {
		return "(true|TRUE|false|FALSE)";
	}
}
