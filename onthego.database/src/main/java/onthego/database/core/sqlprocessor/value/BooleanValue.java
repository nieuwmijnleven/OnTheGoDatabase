package onthego.database.core.sqlprocessor.value;

public final class BooleanValue extends Value {
	
	private boolean value;

	public BooleanValue(boolean value) {
		super(Value.Type.BOOLEAN);
		this.value = value;
	}
	
	public BooleanValue(String value) {
		super(Value.Type.BOOLEAN);
		this.value = Boolean.valueOf(value);
	}
	
	public boolean getValue() {
		return this.value;
	}
	
	@Override
	public String toString() {
		return String.valueOf(value);
	}
}
