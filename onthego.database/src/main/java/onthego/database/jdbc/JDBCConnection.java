package onthego.database.jdbc;

import java.sql.SQLException;
import java.sql.Statement;

import onthego.database.core.database.Database;

public class JDBCConnection extends ConnectionAdapter {
	
	private Database database;
	
	public JDBCConnection(String url) {
		this.database = new Database(url);
	}

	@Override
	public Statement createStatement() throws SQLException {
		return new JDBCStatement(database);
	}

	@Override
	public void setAutoCommit(boolean autoCommit) throws SQLException {
		autoCommitState.setAutoCommit(autoCommit);
	}

	@Override
	public boolean getAutoCommit() throws SQLException {
		return autoCommitState == enabled;
	}

	@Override
	public void commit() throws SQLException {
		autoCommitState.commit();
	}

	@Override
	public void rollback() throws SQLException {
		autoCommitState.rollback();
	}

	@Override
	public void close() throws SQLException {
		autoCommitState.close();
	}
	
	private interface AutoCommitState {
		void rollback() throws SQLException;
		void commit() throws SQLException;
		void close() throws SQLException;
		void setAutoCommit(boolean autoCommit) throws SQLException;
	}
	
	private AutoCommitState enabled = new AutoCommitState() {
		@Override
		public void rollback() throws SQLException {}

		@Override
		public void commit() throws SQLException {}

		@Override
		public void close() throws SQLException {}

		@Override
		public void setAutoCommit(boolean autoCommit) throws SQLException {
			if (autoCommit == false) {
				database.begin();
				autoCommitState = disabled;
			}
		}
	};
	
	private AutoCommitState disabled = new AutoCommitState() {
		@Override
		public void rollback() throws SQLException {
			database.rollback();
			database.begin();
		}

		@Override
		public void commit() throws SQLException {
			database.commit();
			database.begin();
		}

		@Override
		public void close() throws SQLException {
			database.commit();
		}

		@Override
		public void setAutoCommit(boolean autoCommit) throws SQLException {
			if (autoCommit == true) {
				database.commit();
				autoCommitState = enabled;
			}
		}
	};
	
	private AutoCommitState autoCommitState = enabled;
}
