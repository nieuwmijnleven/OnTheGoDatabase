package onthego.database.console;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;

public class SimpleConsole {
	
	public static final Path DATABASE_HOME_PATH = Paths.get(".");
	
	private static final String JDBC_PREFIX = "jdbc:onthego:";
	
	public static final String JDBC_URL = JDBC_PREFIX + DATABASE_HOME_PATH.toString();
	
	public static final String PROMPT = "SQL> ";
	
	public static final String COMMAND_EXIT = "exit";
	
	private Connection connection;
	
	public void start() throws SimpleConsoleException {
		loadJDBCDriver();
		openDatabase();
		
		Scanner scanner = new Scanner(System.in);
		while (true) {
			System.out.print(PROMPT);
			
			String query = scanner.nextLine().strip();
			if (query.length() == 0) continue;
			if (COMMAND_EXIT.equalsIgnoreCase(query)) break;
			
			try {
				execute(query);
			} catch (SimpleConsoleException sce) {
				System.out.println(sce.getMessage());
			} 
		}
		scanner.close();
		
		closeDatabase();
	}
	
	private void loadJDBCDriver() throws SimpleConsoleException {
		try {
			Class.forName("onthego.database.jdbc.JDBCDriver");
		} catch (ClassNotFoundException e) {
			throw new SimpleConsoleException("Could not load the onthego database's jdbc driver.");
		}
	}
	
	private void openDatabase() throws SimpleConsoleException {
		try {
			connection = DriverManager.getConnection(JDBC_URL);
		} catch (SQLException e) {
			throw new SimpleConsoleException("Could not get a connection from " + JDBC_URL);
		}
	}
	
	private void closeDatabase() throws SimpleConsoleException {
		try {
			if (connection != null) 
				connection.close();
		} catch (SQLException e) {
			throw new SimpleConsoleException("Failed to close a connection");
		}
	}

	public void execute(String query) throws SimpleConsoleException {
		if (query.toLowerCase().startsWith("select")) {
			try (Statement statement = connection.createStatement(); 
				 ResultSet resultSet = statement.executeQuery(query);) {
				printResultTable(resultSet);
			} catch (SQLException e) {
				throw new SimpleConsoleException(e.getMessage());
			}
		} else {
			try (Statement statement = connection.createStatement();) {
				int affectedRowCount = statement.executeUpdate(query);
				System.out.println("The number of affected rows is " + affectedRowCount);
			} catch (SQLException e) {
				throw new SimpleConsoleException(e.getMessage());
			}
		}
	}

	private void printResultTable(ResultSet resultSet) throws SQLException {
		printDecorationLine(resultSet.getMetaData());
		printTableHeader(resultSet.getMetaData());
		printDecorationLine(resultSet.getMetaData());
		printTableRecords(resultSet);
		printDecorationLine(resultSet.getMetaData());
	}

	private void printTableRecords(ResultSet resultSet) throws SQLException {
		ResultSetMetaData metaData = resultSet.getMetaData();
		while (resultSet.next()) {
			System.out.print("|");
			for (int i = 0; i < metaData.getColumnCount(); ++i) {
				System.out.printf("%-" + metaData.getColumnDisplaySize(i) + "s|", resultSet.getString(i));
			}
			System.out.println();
		}
	}

	private void printTableHeader(ResultSetMetaData metaData) throws SQLException {
		System.out.print("|");
		for (int i = 0; i < metaData.getColumnCount(); ++i) {
			System.out.printf("%-" + metaData.getColumnDisplaySize(i) + "s|", metaData.getColumnName(i));
		}
		System.out.println();
	}

	private void printDecorationLine(ResultSetMetaData metaData) throws SQLException {
		int decorationLineCount = metaData.getColumnCount();
		for (int i = 0; i < metaData.getColumnCount(); ++i) {
			decorationLineCount += metaData.getColumnDisplaySize(i);
		}
		
		for (int i = 0; i <= decorationLineCount; ++i) {
			System.out.print("-");
		}
		System.out.println();
	}
	
	public static void main(String[] args) {
		try {
			new SimpleConsole().start();
		} catch (Exception e) {
			System.err.println(e.getMessage());
		} 
	}
}
