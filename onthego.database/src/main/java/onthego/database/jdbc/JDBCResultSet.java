package onthego.database.jdbc;

import java.sql.SQLException;

import onthego.database.core.table.Cursor;

public class JDBCResultSet extends ResultSetAdapter {
	
	private Cursor cursor;

	public JDBCResultSet(Cursor cursor) {
		this.cursor = cursor;
	}
	
	@Override
	public boolean next() throws SQLException {
		return cursor.next();
	}
	
	@Override
	public String getString(String columnLabel) throws SQLException {
		return cursor.getColumn(columnLabel);
	}

	@Override
	public int getInt(String columnLabel) throws SQLException {
		return Integer.parseInt(cursor.getColumn(columnLabel));
	}

	@Override
	public long getLong(String columnLabel) throws SQLException {
		return Long.parseLong(cursor.getColumn(columnLabel));
	}

	@Override
	public double getDouble(String columnLabel) throws SQLException {
		return Double.parseDouble(cursor.getColumn(columnLabel));
	}

	@Override
	public void updateInt(String columnLabel, int x) throws SQLException {
		cursor.update(columnLabel, String.valueOf(x));
	}

	@Override
	public void updateLong(String columnLabel, long x) throws SQLException {
		cursor.update(columnLabel, String.valueOf(x));
	}

	@Override
	public void updateDouble(String columnLabel, double x) throws SQLException {
		cursor.update(columnLabel, String.valueOf(x));
	}

	@Override
	public void updateString(String columnLabel, String x) throws SQLException {
		cursor.update(columnLabel, x);
	}
}
