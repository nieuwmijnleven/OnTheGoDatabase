package onthego.database.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

import onthego.database.core.database.Database;
import onthego.database.core.database.DatabaseException;
import onthego.database.core.table.Table;

public class JDBCStatement extends StatementAdapter {
	
	private final Database database;
	
	public JDBCStatement(Database database) {
		this.database = database;
	}

	@Override
	public ResultSet executeQuery(String sql) throws SQLException {
		Table resultTable;
		try {
			resultTable = database.execute(sql);
		} catch (DatabaseException e) {
			throw new SQLException(e.getMessage());
		}
		return new JDBCResultSet(resultTable.getCursor());
	}

	@Override
	public int executeUpdate(String sql) throws SQLException {
		try {
			database.execute(sql);
		} catch (DatabaseException e) {
			throw new SQLException(e.getMessage());
		}
		return database.getAffectedRowCount();
	}
}
