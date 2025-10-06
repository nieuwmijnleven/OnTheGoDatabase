package onthego.database.core.table.meta;

import onthego.database.core.serializer.DoubleSerializer;
import onthego.database.core.serializer.Serializer;

import java.util.Comparator;

public class NumericType extends Type {

	public NumericType(int length, int decimalLength) {
		super(TypeConstants.NUMERIC, length, decimalLength);
	}

	@Override
	protected String generateValuePatternString(int length, int decimalLength) {
		if (length == 0 && decimalLength == 0) {
			return "(^([-+]?[1-9]\\d*|0)(\\.\\d+)?$)";
		} else if (length > 0 && decimalLength == 0) {
			return "(^([-+]?[1-9]\\d{0," + (length - 1) + "}|0)(\\.\\d+)?$)";
		} else if (length == 0 && decimalLength > 0) {
			return "(^([-+]?[1-9]\\d*|0)(\\.\\d{1," + decimalLength + "})?$)";
		} else if (length > 0 && decimalLength > 0) {
			return "(^([-+]?[1-9]\\d{0," + (length - 1) + "}|0)(\\.\\d{1," + decimalLength + "})?$)";
		} else {
			throw new TypeException("The type length(" + length + ", " + decimalLength + ") is not valid.");
		}
	}

    @Override
    public Serializer<Object> getSerializer() {
        return new DoubleSerializer();
    }

    @Override
    public Comparator<Object> getComparator() {
        return Comparator.comparingInt(Integer.class::cast);
    }

    @Override
    public Object cast(String obj) {
        return Double.parseDouble(obj);
    }
}
