package onthego.database.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

import onthego.database.core.database.Database;
import onthego.database.core.table.Table;

public class JDBCStatement extends StatementAdapter {
	
	private Database database;
	
	public JDBCStatement(Database database) {
		this.database = database;
	}

	@Override
	public ResultSet executeQuery(String sql) throws SQLException {
		Table resultTable = database.execute(sql);
		return new JDBCResultSet(resultTable.getCursor());
	}

	@Override
	public int executeUpdate(String sql) throws SQLException {
		database.execute(sql);
		return database.getAffectedRowCount();
	}
}
