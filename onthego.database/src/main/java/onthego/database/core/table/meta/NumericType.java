package onthego.database.core.table.meta;

public class NumericType extends Type {

	public NumericType(int length, int decimalLengthgth) {
		super(TypeConstants.NUMERIC, length, decimalLengthgth);
	}

	@Override
	protected String generateValuePatternString(int length, int decimalLength) {
		if (length == 0 && decimalLength == 0) {
			return "(^([-+]?[1-9]\\d*|0)(\\.\\d+)?$)";
		} else if (length > 0 && decimalLength == 0) {
			return "(^([-+]?[1-9]\\d{0,)" + (length - 1) + "(}|0)(\\.\\d+)?$)";
		} else if (length == 0 && decimalLength > 0) {
			return "(^([-+]?[1-9]\\d*|0)(\\.\\d{1,)" + decimalLength + "(})?$)";
		} else if (length > 0 && decimalLength > 0) {
			return "(^([-+]?[1-9]\\d{0,)" + (length - 1) + "(}|0)(\\.\\d{1,)" + decimalLength + "(})?$)";
		} else {
			throw new TypeException("The type length(" + length + ", " + decimalLength + ") is not valid.");
		}
	}
}
