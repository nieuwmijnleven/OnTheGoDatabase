package onthego.database.core.table.meta;

import onthego.database.core.serializer.Serializer;
import onthego.database.core.serializer.StringSerializer;

import java.util.Comparator;

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

    @Override
    public Serializer<Object> getSerializer() {
        return new StringSerializer();
    }

    @Override
    public Comparator<Object> getComparator() {
        return Comparator.comparing(String.class::cast);
    }

    @Override
    public Object cast(String obj) {
        return obj;
    }
}
