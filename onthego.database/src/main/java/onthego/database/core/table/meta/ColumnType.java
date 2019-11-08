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
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + ((type == null) ? 0 : type.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ColumnType other = (ColumnType) obj;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (type == null) {
			if (other.type != null)
				return false;
		} else if (!type.equals(other.type))
			return false;
		return true;
	}
}