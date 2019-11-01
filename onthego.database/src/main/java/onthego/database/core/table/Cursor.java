package onthego.database.core.table;

import java.util.Iterator;

public interface Cursor {

	String getTableName();
	
	boolean next();
	
	String getColumn(String columnName);
	
	byte[] getRawRecord();
	
	Iterator<String> getRecord();
	
//	boolean isTraversing(Table t);
	
	String update(String columnName, String newValue);
	
	void delete();
}
