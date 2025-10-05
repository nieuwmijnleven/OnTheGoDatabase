package onthego.database.core.table.meta;

import onthego.database.core.serializer.Serializer;

import java.util.Comparator;

public class ConstType extends Type {
	
	public ConstType() {
		super(TypeConstants.CONST);
	}

	@Override
	protected String generateValuePatternString(int length, int decimalLength) {
		return "";
	}

    @Override
    public Serializer<?> getSerializer() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Comparator<?> getComparator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object cast(String obj) { throw new UnsupportedOperationException(); }
}
