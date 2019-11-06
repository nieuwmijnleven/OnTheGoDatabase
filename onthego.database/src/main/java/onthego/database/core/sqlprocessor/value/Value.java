package onthego.database.core.sqlprocessor.value;

public abstract class Value {
	
	public static enum Type {STRING, NUMBER, BOOLEAN, ID, NULL};
	
	protected Type type;
	
	protected Value(Type type) {
		this.type = type;
	}
	
	public Type getType() {
		return this.type;
	}
}
