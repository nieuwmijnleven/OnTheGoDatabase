package onthego.database.core.table.meta;

public class Column {
	
	private String name;
	
	private Type type;
	
	private boolean isKey;
	
	private boolean isNullable;
	
	public Column(String name, Type type, boolean isKey, boolean isNullable) {
		this.name = name;
		this.type = type;
		this.isKey = isKey;
		this.isNullable = isNullable;
	}
	
	public Column(String name, Type type) {
		this(name, type, false, false);
	}
	
	public Type getType() {
		return type;
	}

	public void setType(Type type) {
		this.type = type;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public boolean isKey() {
		return isKey;
	}

	public void setKey(boolean isKey) {
		this.isKey = isKey;
	}

	public boolean isNullable() {
		return isNullable;
	}

	public void setNullable(boolean isNullable) {
		this.isNullable = isNullable;
	}
}