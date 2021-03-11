package onthego.database.jdbc;

import java.sql.SQLException;
import java.sql.Statement;

import onthego.database.core.database.Database;
import onthego.database.core.database.DatabaseException;

public class JDBCConnection extends ConnectionAdapter {
	
	private Database database;
	
	public JDBCConnection(String url) throws SQLException {
		try {
			this.database = new Database(url);
		} catch (DatabaseException e) {
			throw new SQLException(e.getMessage());
		}
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
		public void close() throws SQLException {
			database.close();
		}

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
			try {
				database.rollback();
			} catch (DatabaseException e) {
				throw new SQLException(e.getMessage());
			}
			
			database.begin();
		}

		@Override
		public void commit() throws SQLException {
			try {
				database.commit();
			} catch (DatabaseException e) {
				throw new SQLException(e.getMessage());
			}
			
			database.begin();
		}

		@Override
		public void close() throws SQLException {
			try {
				database.commit();
			} catch (DatabaseException e) {
				throw new SQLException(e.getMessage());
			}
		}

		@Override
		public void setAutoCommit(boolean autoCommit) throws SQLException {
			if (autoCommit == true) {
				try {
					database.commit();
				} catch (DatabaseException e) {
					throw new SQLException(e.getMessage());
				}
				autoCommitState = enabled;
			}
		}
	};
	
	private AutoCommitState autoCommitState = enabled;
}
