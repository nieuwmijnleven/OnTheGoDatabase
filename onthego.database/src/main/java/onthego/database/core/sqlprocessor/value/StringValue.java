package onthego.database.core.sqlprocessor.value;

public final class StringValue extends Value {
	
	private String value;

	public StringValue(String value) {
		super(Value.Type.STRING);
		this.value = value.replaceAll("['\"](.*?)['\"]", "$1");
	}
	
	public String getValue() {
		return this.value;
	}
	
	@Override
	public String toString() {
		return this.value;
	}
}
