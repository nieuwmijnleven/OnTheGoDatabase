package onthego.database.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

public class JDBCDriver implements Driver {
	
	public static final String ONTHEGO_JDBC_PREFIX = "jdbc:onthego:";
	
	static {
		try {
			java.sql.DriverManager.registerDriver(new JDBCDriver());
		} catch(SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Connection connect(String url, Properties info) throws SQLException {
		return new JDBCConnection(url.substring(ONTHEGO_JDBC_PREFIX.length()));
	}

	@Override
	public boolean acceptsURL(String url) throws SQLException {
		return url.startsWith(ONTHEGO_JDBC_PREFIX);
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
		return new DriverPropertyInfo[0];
	}

	@Override
	public int getMajorVersion() {
		return 1;
	}

	@Override
	public int getMinorVersion() {
		return 0;
	}

	@Override
	public boolean jdbcCompliant() {
		return false;
	}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		throw new SQLFeatureNotSupportedException();
	}

}
