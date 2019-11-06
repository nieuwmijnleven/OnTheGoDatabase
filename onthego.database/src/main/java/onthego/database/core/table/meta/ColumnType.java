package onthego.database.core.table.meta;

public class ColumnType {
	
	private String name;
	
	private Type type;
	
	private boolean isKey;
	
	private boolean isNullable;
	
	public ColumnType(String name, Type type, boolean isKey, boolean isNullable) {
		this.name = name;
		this.type = type;
		this.isKey = isKey;
		this.isNullable = isNullable;
	}
	
	public ColumnType(String name, Type type) {
		this(name, type, false, false);
	}
	
	public ColumnType(String name) {
		this(name, new NullType(), false, false);
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
	
	@Override
	public boolean equals(Object obj) {		
		if (!(obj instanceof ColumnType)) {
			return false;
		}
		
		ColumnType rhs = (ColumnType)obj;
		return this.name.equals(rhs.name)
			&& this.type.equals(rhs.type)
			&& this.isKey == rhs.isKey
			&& this.isNullable == rhs.isNullable;
	}
}