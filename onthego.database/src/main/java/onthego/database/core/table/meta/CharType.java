package onthego.database.core.table.meta;

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
}
