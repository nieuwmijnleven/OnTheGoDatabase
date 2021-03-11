package onthego.database.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class JDBCTest {
	
	public static final int TEST_RECORD_COUNT = 100;
	
	public static final Path DATABASE_PATH = Paths.get(".", "onthego_database");
	
	public static final Path TABLE_PATH = DATABASE_PATH.resolve("product.db");
	
	public static final String JDBC_URL = "jdbc:onthego:" + DATABASE_PATH.toString();
	
	@Before
	public void setUp() throws Exception {
		loadJDBCDriver();
		createTestDatabase();
		createTestTable();
		inserTestData();
	}

	private void loadJDBCDriver() throws ClassNotFoundException {
		try {
			Class.forName("onthego.database.jdbc.JDBCDriver");
		} catch (ClassNotFoundException e) {
			throw e;
		}
	}

	private void inserTestData() throws SQLException {
		try (Connection conn = DriverManager.getConnection(JDBC_URL);
			 Statement stmt = conn.createStatement();) {
			for (int i = 0; i < TEST_RECORD_COUNT; ++i) {
				String insertQuery = "insert into product(serial_no, name, price) "
								   + "values("+ i +", 'smartphone" + i + "', " + i * 100 + ")";
				stmt.executeUpdate(insertQuery);
			}
		} catch (SQLException e) {
			throw e;
		}
	}

	private void createTestTable() throws SQLException {
		try (Connection conn = DriverManager.getConnection(JDBC_URL);
			 Statement stmt = conn.createStatement();) {
			String createQuery = "create table product("
					+ "serial_no integer(10),"
					+ "name char(30),"
					+ "price numeric(10,3)"				
					+ ")";
			stmt.executeUpdate(createQuery);
		} catch (SQLException e) {
			throw e;
		}
	}

	private void createTestDatabase() throws SQLException {
		try (Connection conn = DriverManager.getConnection("jdbc:onthego:.");
			 Statement stmt = conn.createStatement();) {
			String createQuery = "create database onthego_database";
			stmt.executeUpdate(createQuery);
		} catch (SQLException e) {
			throw e;
		}
	}

	@After
	public void tearDown() throws Exception {
		Files.deleteIfExists(TABLE_PATH);
		Files.deleteIfExists(DATABASE_PATH);
	}

	@Test
	public void testSelect() throws Exception {
		String query = "select serial_no, name, price from product";
		
		try (Connection conn = DriverManager.getConnection(JDBC_URL);
			 Statement stmt = conn.createStatement();
			 ResultSet rset = stmt.executeQuery(query);) {
			
			boolean[] isSelected = new boolean[TEST_RECORD_COUNT];
			while (rset.next()) {
				int serialNo = rset.getInt("serial_no");
				if (isSelected[serialNo]) {
					fail();
				}
				isSelected[serialNo] = true;
				
				assertEquals(serialNo, rset.getInt("serial_no"));
				assertEquals("smartphone" + serialNo, rset.getString("name"));
				assertEquals((double)serialNo * 100, rset.getDouble("price"), 0);
			}
		} catch (SQLException e) {
			throw e;
		}
	}
	
	@Test
	public void testUpdate() throws Exception {
		String updateQuery = "update product "
						   + "set serial_no = serial_no + 1 "
						   + "where serial_no = ";
		
		try (Connection conn = DriverManager.getConnection(JDBC_URL);
			 Statement stmt = conn.createStatement();) {
			for (int i = TEST_RECORD_COUNT - 1; i >= 0 ; --i) {
				stmt.executeUpdate(updateQuery + i);
			}
		} catch (SQLException e) {
			throw e;
		}
		
		String query = "select serial_no, name, price from product";
		try (Connection conn = DriverManager.getConnection(JDBC_URL);
			 Statement stmt = conn.createStatement();
			 ResultSet rset = stmt.executeQuery(query);) {
				
			boolean[] isSelected = new boolean[TEST_RECORD_COUNT + 1];
			while (rset.next()) {
				int serialNo = rset.getInt("serial_no");
				if (isSelected[serialNo]) {
					fail();
				}
				isSelected[serialNo] = true;
				
				assertEquals(serialNo--, rset.getInt("serial_no"));
				assertEquals("smartphone" + serialNo, rset.getString("name"));
				assertEquals((double)serialNo * 100, rset.getDouble("price"), 0);
			}
			
			if (isSelected[0]) {
				fail();
			}
		} catch (SQLException e) {
			throw e;
		}
	}
	
	@Test
	public void testDelete() throws Exception {
		String deleteQuery = "delete from product "
						   + "where serial_no = ";
		
		try (Connection conn = DriverManager.getConnection(JDBC_URL);
			 Statement stmt = conn.createStatement();) {
			for (int i = 0; i < TEST_RECORD_COUNT ; ++i) {
				stmt.executeUpdate(deleteQuery + i);
			}
		} catch (SQLException e) {
			throw e;
		} 
		
		String query = "select serial_no, name, price from product";
		try (Connection conn = DriverManager.getConnection(JDBC_URL);
			 Statement stmt = conn.createStatement();
			 ResultSet rset = stmt.executeQuery(query);) {
			
			assertFalse(rset.next());
		} catch (SQLException e) {
			throw e;
		}
	}
}
