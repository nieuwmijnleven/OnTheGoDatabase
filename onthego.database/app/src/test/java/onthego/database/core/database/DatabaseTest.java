package onthego.database.core.database;


import onthego.database.core.sqlprocessor.SQLProcessor;
import onthego.database.core.sqlprocessor.SQLResult;
import onthego.database.core.sqlprocessor.expression.Expression;
import onthego.database.core.table.Cursor;
import onthego.database.core.table.StandardTable;
import onthego.database.core.table.Table;
import onthego.database.core.table.meta.ColumnMeta;
import onthego.database.core.table.meta.TypeConstants;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class DatabaseTest {
	
	private Database database;
	
	private final Path databasePath = Paths.get(".", "onthego-database");
	
	private final Path tablePath = databasePath.resolve("product.db");
	
	private final String tableName = "product";
	
	@BeforeEach
	public void setUp() throws Exception {
		database = new Database(".");
		database.createDatabase(databasePath.getFileName().toString());
	}

	@AfterEach
	public void tearDown() throws Exception {
		database.close();
		Files.deleteIfExists(tablePath);
		Files.deleteIfExists(databasePath);
	}

	@Test
	public void testCreateDatabase() {
		assertTrue(Files.isDirectory(databasePath));
	}
	
	@Test
	public void testCreateTable() throws DatabaseException, IOException {
		SQLResult result = createTable();
		
		Table table = StandardTable.load(databasePath.toString(), result.getTable());
		assertEquals(result.getTable(), table.getTableName());
		
		List<ColumnMeta> columns = table.getColumnList();
		assertEquals(3, columns.size());
		
		ColumnMeta column = columns.get(0);
		assertEquals("serial_no", column.getName());
		assertEquals(TypeConstants.INTEGER, column.getType().getTypeConstant());
		assertEquals(10, column.getType().getLength());
		
		column = columns.get(1);
		assertEquals("name", column.getName());
		assertEquals(TypeConstants.CHAR, column.getType().getTypeConstant());
		assertEquals(30, column.getType().getLength());
		
		column = columns.get(2);
		assertEquals("price", column.getName());
		assertEquals(TypeConstants.NUMERIC, column.getType().getTypeConstant());
		assertEquals(10, column.getType().getLength());
		assertEquals(3, column.getType().getDecimalLength());
		
		table.close();
	}

	private SQLResult createTable() throws DatabaseException {
		String sql = "create table product("
				+ "serial_no integer(10),"
				+ "name char(30),"
				+ "price numeric(10,3)"				
				+ ")";
		
		SQLProcessor processor = new SQLProcessor(sql);
		SQLResult result = processor.process();
		
		database.createTable(result.getTable(), result.getColumns());
		assertTrue(Files.exists(tablePath));

		return result;
	}
	
	@Test
	public void testDropTable() throws IOException, DatabaseException {
		createTable();
		
		database.dropTable(tableName);
		assertFalse(Files.exists(tablePath));
	}

	@Test
	public void testBegin() throws DatabaseException {
		database.begin();
		database.commit();
		
		try {
			database.rollback();
			fail("fail to manage transactions.");
		} catch(Exception e) {
        }
		
		database.begin();
		database.rollback();
		
		try {
			database.commit();
			fail("fail to manage transactions.");
		} catch(Exception e) {
        }
	}

	@Test
	public void testCommit() throws DatabaseException {
		createTable();
		
		database.begin();
		insertRecord();
		
		String sql = "select serial_no, name, price\n"
				+ "from product\n"
				+ "where serial_no = 100\n"
				+ "and name = 'smartphone'\n"
				+ "and price = 200.1";
		
		SQLProcessor processor = new SQLProcessor(sql);
		SQLResult result = processor.process();
		
		Table resultTable = database.select(result.getTable(), result.getColumns(), result.getWhere());
		assertEquals(result.getTable(), resultTable.getTableName());
		
		Cursor cursor = resultTable.getCursor();
		assertTrue(cursor.next());
		
		Iterator<String> it = cursor.getRecord();
		assertEquals("100", it.next());
		assertEquals("smartphone", it.next());
		assertEquals("200.1", it.next());
		
		database.commit();
		
		resultTable = database.select(result.getTable(), result.getColumns(), result.getWhere());
		assertEquals(result.getTable(), resultTable.getTableName());
		
		cursor = resultTable.getCursor();
		assertTrue(cursor.next());
		
		it = cursor.getRecord();
		assertEquals("100", it.next());
		assertEquals("smartphone", it.next());
		assertEquals("200.1", it.next());
	}

	@Test
	public void testRollback() throws DatabaseException {
		createTable();
		
		database.begin();
		insertRecord();
		
		String sql = "select serial_no, name, price\n"
				+ "from product\n"
				+ "where serial_no = 100\n"
				+ "and name = 'smartphone'\n"
				+ "and price = 200.1";
		
		SQLProcessor processor = new SQLProcessor(sql);
		SQLResult result = processor.process();
		
		Table resultTable = database.select(result.getTable(), result.getColumns(), result.getWhere());
		assertEquals(result.getTable(), resultTable.getTableName());
		
		Cursor cursor = resultTable.getCursor();
		assertTrue(cursor.next());
		
		Iterator<String> it = cursor.getRecord();
		assertEquals("100", it.next());
		assertEquals("smartphone", it.next());
		assertEquals("200.1", it.next());
		
		database.rollback();
		
		resultTable = database.select(result.getTable(), result.getColumns(), result.getWhere());
		assertEquals(result.getTable(), resultTable.getTableName());
		
		cursor = resultTable.getCursor();
		assertFalse(cursor.next());
	}

	@Test
	public void testSelectWithSpecificColumns() throws DatabaseException {
		createTable();
		insertRecord();
		
		String sql = "select serial_no, name, price\n"
				+ "from product\n"
				+ "where serial_no = 100\n"
				+ "and name = 'smartphone'\n"
				+ "and price = 200.1";
		
		SQLProcessor processor = new SQLProcessor(sql);
		SQLResult result = processor.process();
		
		Table resultTable = database.select(result.getTable(), result.getColumns(), result.getWhere());
		assertEquals(result.getTable(), resultTable.getTableName());
		
		Cursor cursor = resultTable.getCursor();
		assertTrue(cursor.next());
		
		Iterator<String> it = cursor.getRecord();
		assertEquals("100", it.next());
		assertEquals("smartphone", it.next());
		assertEquals("200.1", it.next());
	}
	
	@Test
	public void testSelectWithAllColumns() throws DatabaseException {
		createTable();
		insertRecord();
		
		String sql = "select *\n"
				+ "from product\n"
				+ "where serial_no = 100\n"
				+ "and name = 'smartphone'\n"
				+ "and price = 200.1";
		
		SQLProcessor processor = new SQLProcessor(sql);
		SQLResult result = processor.process();
		
		Table resultTable = database.select(result.getTable(), result.getColumns(), result.getWhere());
		assertEquals(result.getTable(), resultTable.getTableName());
		
		Cursor cursor = resultTable.getCursor();
		assertTrue(cursor.next());
		
		Iterator<String> it = cursor.getRecord();
		assertEquals("100", it.next());
		assertEquals("smartphone", it.next());
		assertEquals("200.1", it.next());
	}

	private SQLResult insertRecord() throws DatabaseException {
		String sql = "insert into product(serial_no, name, price) "
				   + "values(100, 'smartphone', 200.1)";
		
		SQLProcessor processor = new SQLProcessor(sql);
		SQLResult result = processor.process();
		
		database.insert(result.getTable(), result.getColumns(), result.getValues());
		return result;
	}

	@Test
	public void testInsertWithSpecificColumns() throws DatabaseException {
		createTable();
		SQLResult result = insertRecord();
		
		Table resultTable = database.select(result.getTable(), result.getColumns(), Expression.TRUE_EXPRESSION);
		assertEquals(result.getTable(), resultTable.getTableName());
		
		Cursor cursor = resultTable.getCursor();
		assertTrue(cursor.next());
		
		Iterator<String> it = cursor.getRecord();
		assertEquals("100", it.next());
		assertEquals("smartphone", it.next());
		assertEquals("200.1", it.next());
	}
	
	@Test
	public void testInsertWithAllColumns() throws DatabaseException {
		createTable();
		
		String sql = "insert into product "
				   + "values(100, 'smartphone', 200.1)";
		
		SQLProcessor processor = new SQLProcessor(sql);
		SQLResult result = processor.process();
		
		database.insert(result.getTable(), result.getColumns(), result.getValues());
		
		Table resultTable = database.select(result.getTable(), result.getColumns(), Expression.TRUE_EXPRESSION);
		assertEquals(result.getTable(), resultTable.getTableName());
		
		Cursor cursor = resultTable.getCursor();
		assertTrue(cursor.next());
		
		Iterator<String> it = cursor.getRecord();
		assertEquals("100", it.next());
		assertEquals("smartphone", it.next());
		assertEquals("200.1", it.next());
	}

	@Test
	public void testUpdate() throws DatabaseException {
		createTable();
		insertRecord();
		
		String sql = "update product set price = 123.45\n"
				   + "where serial_no = 100";
		
		SQLProcessor processor = new SQLProcessor(sql);
		SQLResult result = processor.process();
		
		int affectedRowCount = database.update(result.getTable(), result.getColumns(), result.getValues(), result.getWhere());
		assertEquals(1, affectedRowCount);
		
		sql = "select *\n"
			+ "from product\n"
			+ "where serial_no = 100\n"
			+ "and name = 'smartphone'\n"
			+ "and price = 123.45";
		
		processor = new SQLProcessor(sql);
		result = processor.process();
		
		Table resultTable = database.select(result.getTable(), result.getColumns(), result.getWhere());
		assertEquals(result.getTable(), resultTable.getTableName());
		
		Cursor cursor = resultTable.getCursor();
		assertTrue(cursor.next());
		
		Iterator<String> it = cursor.getRecord();
		assertEquals("100", it.next());
		assertEquals("smartphone", it.next());
		assertEquals("123.45", it.next());
	}

	@Test
	public void testDelete() throws DatabaseException {
		createTable();
		insertRecord();
		
		String sql = "delete from product\n"
					+ "where serial_no = 100\n"
					+ "and name = 'smartphone'\n"
					+ "and price = 200.10";
		
		SQLProcessor processor = new SQLProcessor(sql);
		SQLResult result = processor.process();
		
		int affectedRowCount = database.delete(result.getTable(), result.getWhere());
		assertEquals(1, affectedRowCount);
		
		sql = "select * from product";
		
		processor = new SQLProcessor(sql);
		result = processor.process();
		
		Table resultTable = database.select(result.getTable(), result.getColumns(), result.getWhere());
		assertEquals(result.getTable(), resultTable.getTableName());
		
		Cursor cursor = resultTable.getCursor();
		assertFalse(cursor.next());
	}

	@Test
	public void testExecute() throws DatabaseException {
		String query = "create table product("
					+ "serial_no integer(10),"
					+ "name char(30),"
					+ "price numeric(10,3)"				
					+ ")";
		database.execute(query);
		
		query = "select * from product";
		Table resultTable = database.execute(query);
		Cursor cursor = resultTable.getCursor();
		
		assertFalse(cursor.next());
		
		database.begin();
		
		query = "insert into product(serial_no, name, price) "
			  + "values(100, 'smartphone', 200.1)";
		database.execute(query);
		
		query = "insert into product(serial_no, name, price) "
			  + "values(200, 'tablet/pad', 400.2)";
		database.execute(query);
			
		query = "insert into product(serial_no, name, price) "
			  + "values(300, 'notebook', 600.3)";
		database.execute(query);
				
		query = "select * from product";
		resultTable = database.execute(query);
		cursor = resultTable.getCursor();

        assertTrue(cursor.next());
        Iterator<String> it = cursor.getRecord();
        assertEquals("100", it.next());
        assertEquals("smartphone", it.next());
        assertEquals("200.1", it.next());
		
		assertTrue(cursor.next());
		it = cursor.getRecord();
		assertEquals("200", it.next());
		assertEquals("tablet/pad", it.next());
		assertEquals("400.2", it.next());
		
		assertTrue(cursor.next());
		it = cursor.getRecord();
		assertEquals("300", it.next());
		assertEquals("notebook", it.next());
		assertEquals("600.3", it.next());

		query = "update product set serial_no = 101\n"
			  + "where name = 'smartphone' and price = 200.1";
		database.execute(query);
		
		query = "update product set name = 'the-state-of-art tablet/pad'\n"
			  + "where serial_no = 200 and price = 400.2";
		database.execute(query);
		
		query = "update product set price = 800.3\n"
			  + "where serial_no = 300 and name = 'notebook'";
		database.execute(query);
			
		query = "select * from product";
		resultTable = database.execute(query);
		cursor = resultTable.getCursor();
		
		assertTrue(cursor.next());
		it = cursor.getRecord();
        assertEquals("101", it.next());
        assertEquals("smartphone", it.next());
        assertEquals("200.1", it.next());

        assertTrue(cursor.next());
        it = cursor.getRecord();
        assertEquals("300", it.next());
        assertEquals("notebook", it.next());
        assertEquals("800.3", it.next());
		
		assertTrue(cursor.next());
		it = cursor.getRecord();
		assertEquals("200", it.next());
		assertEquals("the-state-of-art tablet/pad", it.next());
		assertEquals("400.2", it.next());
		
		query = "delete from product where serial_no = 200";
		database.execute(query);
		
		query = "select * from product";
		resultTable = database.execute(query);
		cursor = resultTable.getCursor();
		
		assertTrue(cursor.next());
		it = cursor.getRecord();
        assertEquals("101", it.next());
        assertEquals("smartphone", it.next());
        assertEquals("200.1", it.next());
		
		assertTrue(cursor.next());
		it = cursor.getRecord();
        assertEquals("300", it.next());
        assertEquals("notebook", it.next());
        assertEquals("800.3", it.next());
		
		database.rollback();
		
		query = "select * from product";
		resultTable = database.execute(query);
		cursor = resultTable.getCursor();
		
		assertFalse(cursor.next());
	}
}
