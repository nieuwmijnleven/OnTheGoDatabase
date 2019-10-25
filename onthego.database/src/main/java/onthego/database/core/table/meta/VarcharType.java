package onthego.database.core.table.meta;

public class VarcharType extends CharType {

	public VarcharType(int length) {
		super(length);
		setTypeConstant(TypeConstants.VARCHAR);
	}

}
