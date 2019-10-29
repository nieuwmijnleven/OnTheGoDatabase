package onthego.database.core.table.meta;

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
}
