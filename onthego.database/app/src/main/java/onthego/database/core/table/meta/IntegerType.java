package onthego.database.core.table.meta;

import onthego.database.core.serializer.IntegerSerializer;
import onthego.database.core.serializer.Serializer;

import java.util.Comparator;

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

    @Override
    public Serializer<Object> getSerializer() {
        return new IntegerSerializer();
    }

    @Override
    public Comparator<Object> getComparator() {
        return Comparator.comparingInt(Integer.class::cast);
    }

    @Override
    public Object cast(String obj) {
        return Integer.parseInt(obj);
    }
}
