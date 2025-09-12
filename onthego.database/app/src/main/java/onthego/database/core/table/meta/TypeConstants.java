package onthego.database.core.table.meta;

public enum TypeConstants {
	
	CHAR("CHAR", 1), VARCHAR("VARCHAR", 2), INTEGER("INTEGER", 3), 
	NUMERIC("NUMERIC", 4), BOOL("BOOL", 5), CONST("CONST", 6), NULL("NIL", 7);
	
	private final String name;
	
	private final int order;

	TypeConstants(String name, int order) {
		this.name = name;
		this.order = order;
	}

	public String getName() {
		return name;
	}

	public int getOrder() {
		return order;
	}
	
	@Override
	public String toString() {
		return name;
	}
}
