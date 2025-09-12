package onthego.database.core.table.meta;

public class Types {

	private Types() {}
	
	public static Type of(TypeConstants typeConstants, int length, int decimalLength) {
		if (typeConstants == TypeConstants.CHAR) {
			return new CharType(length);
		} else if (typeConstants == TypeConstants.VARCHAR) {
			return new VarcharType(length);
		} else if (typeConstants == TypeConstants.INTEGER) {
			return new IntegerType(length);
		} else if (typeConstants == TypeConstants.NUMERIC) {
			return new NumericType(length, decimalLength);
		} else if (typeConstants == TypeConstants.BOOL) {
			return new BoolType();
		} else if (typeConstants == TypeConstants.CONST) {
			return new ConstType();
		} else if (typeConstants == TypeConstants.NULL) {
			return new NullType();
		} else {
			throw new TypeException("Could not create a type with a wrong TypeConstants's value.");
		}
	}
}
