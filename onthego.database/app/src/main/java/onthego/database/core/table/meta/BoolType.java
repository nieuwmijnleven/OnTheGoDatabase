package onthego.database.core.table.meta;

import onthego.database.core.serializer.BooleanSerializer;
import onthego.database.core.serializer.Serializer;

import java.util.Comparator;

public class BoolType extends Type {
	
	public BoolType() {
		super(TypeConstants.BOOL);
	}

	@Override
	protected String generateValuePatternString(int length, int decimalLength) {
		return "(true|TRUE|false|FALSE)";
	}

    @Override
    public Serializer<Object> getSerializer() {
        return new BooleanSerializer();
    }

    @Override
    public Comparator<Object> getComparator() {
        return Comparator.comparing(Boolean.class::cast);
    }

    @Override
    public Object cast(String obj) {
        return Boolean.parseBoolean(obj);
    }

}
