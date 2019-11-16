package onthego.database.core.sqlprocessor.value;

public final class NumberValue extends Value {
	
	private double value;
	
	public NumberValue(double value) {
		super(Value.Type.NUMBER);
		this.value = value;
	}

	public NumberValue(String value) {
		super(Value.Type.NUMBER);
		this.value = Double.parseDouble(value);
	}
	
	public double getValue() {
		return this.value;
	}
	
	@Override
	public String toString() {
		if (Math.abs(value - Math.floor(value)) < 1.0E-20) {
			return String.valueOf((long)value);
		} else {
			return String.valueOf(value);
		}
	}
}
