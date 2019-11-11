package onthego.database.core.table;

import java.util.Iterator;

import onthego.database.core.table.meta.ColumnType;

public interface Cursor {

	String getTableName();
	
	boolean next();
	
	int getColumnCount();
	
	String getColumn(int columnIdx);
	
	String getColumn(String columnName);
	
	ColumnType getColumnType(int columnIdx);
	
	ColumnType getColumnType(String columnName);
	
	byte[] getRawRecord();
	
	Iterator<String> getRecord();
	
//	boolean isTraversing(Table t);
	
	String update(String columnName, String newValue);
	
	void delete();
}
