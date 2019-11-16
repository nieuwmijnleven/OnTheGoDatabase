package onthego.database.core.table;

import java.util.Iterator;

import onthego.database.core.table.meta.ColumnMeta;

public interface Cursor {

	String getTableName();
	
	boolean next();
	
	int getColumnCount();
	
	String getColumn(int columnIdx);
	
	String getColumn(String columnName);
	
	ColumnMeta getColumnType(int columnIdx);
	
	ColumnMeta getColumnType(String columnName);
	
	byte[] getRawRecord();
	
	Iterator<String> getRecord();
	
//	boolean isTraversing(Table t);
	
	String update(String columnName, String newValue);
	
	void delete();
}
