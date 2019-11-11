package onthego.database.jdbc;

import java.sql.SQLException;

import onthego.database.core.table.Cursor;
import onthego.database.core.table.meta.Type;

public class JDBCResultSetMetaData extends ResultSetMetaDataAdapter {
	
	private Cursor cursor;

	public JDBCResultSetMetaData(Cursor cursor) {
		this.cursor = cursor;
	}

	@Override
	public int getColumnCount() throws SQLException {
		return cursor.getColumnCount();
	}
	
	@Override
	public int getColumnDisplaySize(int column) throws SQLException {
		Type type = cursor.getColumnType(column).getType();
		return type.getLength() + type.getDecimalLength();
	}

	@Override
	public String getColumnLabel(int column) throws SQLException {
		return cursor.getColumnType(column).getName();
	}

	@Override
	public String getColumnName(int column) throws SQLException {
		return cursor.getColumnType(column).getName();
	}

	@Override
	public String getTableName(int column) throws SQLException {
		return cursor.getTableName();
	}

	@Override
	public int getColumnType(int column) throws SQLException {
		switch (cursor.getColumnType(column).getType().getTypeConstant()) {
		case CHAR:
			return java.sql.Types.CHAR;
		case VARCHAR:
			return java.sql.Types.VARCHAR;
		case INTEGER:
			return java.sql.Types.INTEGER;
		case NUMERIC:
			return java.sql.Types.NUMERIC;	
		case BOOL:
			return java.sql.Types.BOOLEAN;
		case NULL:
			return java.sql.Types.NULL;
		default: 
			return java.sql.Types.VARCHAR;
		}
	}

	@Override
	public String getColumnTypeName(int column) throws SQLException {
		switch (cursor.getColumnType(column).getType().getTypeConstant()) {
		case CHAR:
			return "CHAR";
		case VARCHAR:
			return "VARCHAR";
		case INTEGER:
			return "INTEGER";
		case NUMERIC:
			return "NUMERIC";
		case BOOL:
			return "BOOL";
		case NULL:
			return "NULL";
		default: 
			return "NULL";
		}
	}
}
