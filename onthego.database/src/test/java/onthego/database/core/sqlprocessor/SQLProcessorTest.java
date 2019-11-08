package onthego.database.core.sqlprocessor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.util.List;

import org.junit.Test;

import onthego.database.core.sqlprocessor.expression.Expression;
import onthego.database.core.sqlprocessor.expression.ExpressionEvaluationException;
import onthego.database.core.table.meta.ColumnType;
import onthego.database.core.table.meta.TypeConstants;

public class SQLProcessorTest {

	@Test
	public void testCreateDatabase() throws SQLProcessorException {
		String sql = "create database store";
		
		SQLProcessor processor = new SQLProcessor(sql);
		SQLResult result = processor.process();
		
		assertEquals(SQLResult.CommandType.CREATE_DATABASE, result.getCommand());
		assertEquals("store", result.getDatabase());
	}
	
	@Test
	public void testUseDatabase() throws SQLProcessorException {
		String sql = "use database store";
		
		SQLProcessor processor = new SQLProcessor(sql);
		SQLResult result = processor.process();
		
		assertEquals(SQLResult.CommandType.USE_DATABASE, result.getCommand());
		assertEquals("store", result.getDatabase());
	}
	
	@Test
	public void testCreateTable() throws SQLProcessorException {
		String sql = "create table product("
				+ "serial_no integer(10),"
				+ "name char(30),"
				+ "price numeric(10,3)"				
				+ ")";
		
		SQLProcessor processor = new SQLProcessor(sql);
		SQLResult result = processor.process();
		
		assertEquals(SQLResult.CommandType.CREATE_TABLE, result.getCommand());
		assertEquals("product", result.getTable());
		
		List<ColumnType> columns = result.getColumns();
		assertEquals(3, columns.size());
		
		ColumnType column = columns.get(0);
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
	}
	
	@Test
	public void testDropTable() throws SQLProcessorException {
		String sql = "drop table product";
		
		SQLProcessor processor = new SQLProcessor(sql);
		SQLResult result = processor.process();
		
		assertEquals(SQLResult.CommandType.DROP_TABLE, result.getCommand());
		assertEquals("product", result.getTable());
	}
	
	@Test
	public void testBegin() throws SQLProcessorException {
		String sql = " begin ";
		
		SQLProcessor processor = new SQLProcessor(sql);
		SQLResult result = processor.process();
		
		assertEquals(SQLResult.CommandType.BEGIN_TRANSACTION, result.getCommand());
	}
	
	@Test
	public void testCommit() throws SQLProcessorException {
		String sql = " commit ";
		
		SQLProcessor processor = new SQLProcessor(sql);
		SQLResult result = processor.process();
		
		assertEquals(SQLResult.CommandType.COMMIT, result.getCommand());
	}
	
	@Test
	public void testRollback() throws SQLProcessorException {
		String sql = " rollback ";
		
		SQLProcessor processor = new SQLProcessor(sql);
		SQLResult result = processor.process();
		
		assertEquals(SQLResult.CommandType.ROLLBACK, result.getCommand());
	}
	
	@Test
	public void testSelectWithSpecificColumns() throws SQLProcessorException {
		String sql = "select serial_no, name, price\n"
				+ "from product\n"
				+ "where serial_no = 100\n"
				+ "and name = 'smartphone'\n"
				+ "and price = 200.10";
		
		SQLProcessor processor = new SQLProcessor(sql);
		SQLResult result = processor.process();
		
		assertEquals(SQLResult.CommandType.SELECT, result.getCommand());
		assertEquals("product", result.getTable());
		
		List<ColumnType> columns = result.getColumns();
		assertEquals(3, columns.size());
		
		ColumnType column = columns.get(0);
		assertEquals("serial_no", column.getName());
		column = columns.get(1);
		assertEquals("name", column.getName());
		column = columns.get(2);
		assertEquals("price", column.getName());
		
		assertNotEquals(Expression.NULL_EXPRESSION, result.getWhere());
	}
	
	@Test
	public void testSelectWithAllColumns() throws SQLProcessorException {
		String sql = "select *\n"
				+ "from product\n"
				+ "where serial_no = 100\n"
				+ "and name = 'smartphone'\n"
				+ "and price = 200.10";
		
		SQLProcessor processor = new SQLProcessor(sql);
		SQLResult result = processor.process();
		
		assertEquals(SQLResult.CommandType.SELECT, result.getCommand());
		assertEquals("product", result.getTable());
		
		List<ColumnType> columns = result.getColumns();
		assertEquals(0, columns.size());
		
		assertNotEquals(Expression.NULL_EXPRESSION, result.getWhere());
	}
	
	@Test
	public void testInsertWithSpecificColumns() throws SQLProcessorException, ExpressionEvaluationException {
		String sql = "insert into product(serial_no, name, price) "
				   + "values(100, 'smartphone', 200.1)";
		
		SQLProcessor processor = new SQLProcessor(sql);
		SQLResult result = processor.process();
		
		assertEquals(SQLResult.CommandType.INSERT, result.getCommand());
		assertEquals("product", result.getTable());
		
		List<ColumnType> columns = result.getColumns();
		assertEquals(3, columns.size());
		
		ColumnType column = columns.get(0);
		assertEquals("serial_no", column.getName());
		column = columns.get(1);
		assertEquals("name", column.getName());
		column = columns.get(2);
		assertEquals("price", column.getName());
		
		List<Expression> values = result.getValues();
		assertEquals(3, values.size());
		
		assertEquals("100", values.get(0).evaluate(null).toString());
		assertEquals("smartphone", values.get(1).evaluate(null).toString());
		assertEquals("200.1", values.get(2).evaluate(null).toString());
	}
	
	@Test
	public void testInsertWithAllColumns() throws SQLProcessorException, ExpressionEvaluationException {
		String sql = "insert into product "
				   + "values(100, 'smartphone', 200.1)";
		
		SQLProcessor processor = new SQLProcessor(sql);
		SQLResult result = processor.process();
		
		assertEquals(SQLResult.CommandType.INSERT, result.getCommand());
		assertEquals("product", result.getTable());
		
		List<ColumnType> columns = result.getColumns();
		assertEquals(0, columns.size());
		
		List<Expression> values = result.getValues();
		assertEquals(3, values.size());
		
		assertEquals("100", values.get(0).evaluate(null).toString());
		assertEquals("smartphone", values.get(1).evaluate(null).toString());
		assertEquals("200.1", values.get(2).evaluate(null).toString());
	}
	
	@Test
	public void testUpdate() throws SQLProcessorException, ExpressionEvaluationException {
		String sql = "update product set price = 200.5 "
				   + "where serial_no = 100";
		
		SQLProcessor processor = new SQLProcessor(sql);
		SQLResult result = processor.process();
		
		assertEquals(SQLResult.CommandType.UPDATE, result.getCommand());
		assertEquals("product", result.getTable());
		
		List<ColumnType> columns = result.getColumns();
		assertEquals(1, columns.size());
		
		ColumnType column = columns.get(0);
		assertEquals("price", column.getName());
		
		List<Expression> values = result.getValues();
		assertEquals(1, values.size());
		assertEquals("200.5", values.get(0).evaluate(null).toString());
		
		assertNotEquals(Expression.NULL_EXPRESSION, result.getWhere());
	}
	
	@Test
	public void testDelete() throws SQLProcessorException {
		String sql = "delete from product\n"
				+ "where serial_no = 100\n"
				+ "and name = 'smartphone'\n"
				+ "and price = 200.10";
		
		SQLProcessor processor = new SQLProcessor(sql);
		SQLResult result = processor.process();
		
		assertEquals(SQLResult.CommandType.DELETE, result.getCommand());
		assertEquals("product", result.getTable());
		
		assertNotEquals(Expression.NULL_EXPRESSION, result.getWhere());
	}

}
