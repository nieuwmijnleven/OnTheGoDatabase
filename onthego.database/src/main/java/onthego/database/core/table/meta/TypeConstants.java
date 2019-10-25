package onthego.database.core.table.meta;

public enum TypeConstants {
	
	CHAR("CHAR"), VARCHAR("VARCHAR"), INTEGER("INTEGER"), 
	NUMERIC("NUMERIC"), BOOL("BOOL"), CONST("CONST"), NIL("NIL");
	
	private String name;

	private TypeConstants(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}	
}
