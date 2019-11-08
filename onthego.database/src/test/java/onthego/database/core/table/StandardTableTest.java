package onthego.database.core.table;

import static onthego.database.core.table.StandardTableUtil.readUTF;
import static onthego.database.core.table.StandardTableUtil.readUnsignedShort;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import onthego.database.core.table.meta.ColumnType;
import onthego.database.core.table.meta.Type;
import onthego.database.core.table.meta.TypeConstants;
import onthego.database.core.tablespace.meta.TableMetaInfo;

public class StandardTableTest {
	
	private Table table;

	@Before
	public void setUp() throws Exception {
		TableMetaInfo tableMetaInfo = generateTableMetaInfo();
		table = StandardTable.create(".", "test", tableMetaInfo);
		
		List<ColumnType> columnList = tableMetaInfo.getColumnList();
		Map<ColumnType,String> values = Map.of(columnList.get(0), "100", columnList.get(1), "smartphone", columnList.get(2), "123.4", columnList.get(3), "true");
		
		table.insert(values);
	}

	@After
	public void tearDown() throws Exception {
		File file = new File("./test.db");
		if (file.exists()) {
			file.delete();
		}
	}
	
	private TableMetaInfo generateTableMetaInfo() {
		String tableName = "product";
		
		List<ColumnType> columnList = new ArrayList<>();
		columnList.add(new ColumnType("serial_no", Type.of(TypeConstants.INTEGER, 10, 0)));
		columnList.add(new ColumnType("name", Type.of(TypeConstants.CHAR, 20, 0)));
		columnList.add(new ColumnType("price", Type.of(TypeConstants.NUMERIC, 10, 3)));
		columnList.add(new ColumnType("on_sale", Type.of(TypeConstants.BOOL, 0, 0)));
		
		TableMetaInfo tableMetaInfo = new TableMetaInfo(tableName, columnList);
		return tableMetaInfo;
	}
	
	@Test
	public void testSelect() throws IOException {
		Table resultTable = table.select(new Filtration.DefaultFilter() {
			@Override
			public boolean filter(Cursor[] cursor) {
				return cursor[0].getColumn("serial_no").equals("100")
					&& cursor[0].getColumn("name").equals("smartphone")
					&& cursor[0].getColumn("price").equals("123.4")
					&& cursor[0].getColumn("on_sale").equals("true");
			}
		});
		
		Cursor cursor = resultTable.getCursor();
		cursor.next();
		
		Iterator<String> iterator = cursor.getRecord();
		assertEquals("100", iterator.next());
		assertEquals("smartphone", iterator.next());
		assertEquals("123.4", iterator.next());
		assertEquals("true", iterator.next());
	}

	@Test
	public void testInsert() throws IOException {
		Cursor cursor = table.getCursor();
		cursor.next();
		
		ByteBuffer recordBuffer = ByteBuffer.wrap(cursor.getRawRecord());
		assertEquals(4, readUnsignedShort(recordBuffer));
		assertEquals(10, readUnsignedShort(recordBuffer));
		assertEquals(15, readUnsignedShort(recordBuffer));
		assertEquals(27, readUnsignedShort(recordBuffer));
		assertEquals(34, readUnsignedShort(recordBuffer));
		assertEquals("100", readUTF(recordBuffer));
		assertEquals("smartphone", readUTF(recordBuffer));
		assertEquals("123.4", readUTF(recordBuffer));
		assertEquals("true", readUTF(recordBuffer));
		
		Iterator<String> iterator = cursor.getRecord();
		assertEquals("100", iterator.next());
		assertEquals("smartphone", iterator.next());
		assertEquals("123.4", iterator.next());
		assertEquals("true", iterator.next());
	}
	
	@Test
	public void testUpdate() throws IOException {
		table.update(new Filtration() {
			@Override
			public boolean filter(Cursor[] cursor) {
				return cursor[0].getColumn("serial_no").equals("100")
					&& cursor[0].getColumn("price").equals("123.4")
					&& cursor[0].getColumn("on_sale").equals("true");
			}

			@Override
			public void update(Cursor cursor) {
				cursor.update("name", "the state of art smartphone");
			}
		});
		
		Cursor cursor = table.getCursor();
		cursor.next();
		
		ByteBuffer recordBuffer = ByteBuffer.wrap(cursor.getRawRecord());
		assertEquals(4, readUnsignedShort(recordBuffer));
		assertEquals(10, readUnsignedShort(recordBuffer));
		assertEquals(15, readUnsignedShort(recordBuffer));
		assertEquals(44, readUnsignedShort(recordBuffer));
		assertEquals(51, readUnsignedShort(recordBuffer));
		assertEquals("100", readUTF(recordBuffer));
		assertEquals("the state of art smartphone", readUTF(recordBuffer));
		assertEquals("123.4", readUTF(recordBuffer));
		assertEquals("true", readUTF(recordBuffer));
		
		Iterator<String> iterator = cursor.getRecord();
		assertEquals("100", iterator.next());
		assertEquals("the state of art smartphone", iterator.next());
		assertEquals("123.4", iterator.next());
		assertEquals("true", iterator.next());
	}
	
	@Test
	public void testDelete() throws IOException {
		table.delete(new Filtration.DefaultFilter() {
			@Override
			public boolean filter(Cursor[] cursor) {
				return cursor[0].getColumn("serial_no").equals("100")
					&& cursor[0].getColumn("name").equals("smartphone")
					&& cursor[0].getColumn("price").equals("123.4")
					&& cursor[0].getColumn("on_sale").equals("true");
			}
		});
		
		assertFalse(table.getCursor().next());
	}
	
	@Test
	public void testRollback() throws IOException {
		Cursor cursor = table.getCursor();
		assertTrue(cursor.next());
		
		table.begin();
		
		Iterator<String> iterator = cursor.getRecord();
		assertEquals("100", iterator.next());
		assertEquals("smartphone", iterator.next());
		assertEquals("123.4", iterator.next());
		assertEquals("true", iterator.next());
		
		table.update(new Filtration() {
			@Override
			public boolean filter(Cursor[] cursor) {
				return cursor[0].getColumn("serial_no").equals("100")
					&& cursor[0].getColumn("price").equals("123.4")
					&& cursor[0].getColumn("on_sale").equals("true");
			}

			@Override
			public void update(Cursor cursor) {
				cursor.update("name", "the state of art smartphone");
			}
		});
		
		cursor = table.getCursor();
		assertTrue(cursor.next());
		
		iterator = cursor.getRecord();
		assertEquals("100", iterator.next());
		assertEquals("the state of art smartphone", iterator.next());
		assertEquals("123.4", iterator.next());
		assertEquals("true", iterator.next());
		
		table.delete(new Filtration.DefaultFilter() {
			@Override
			public boolean filter(Cursor[] cursor) {
				return cursor[0].getColumn("serial_no").equals("100")
					&& cursor[0].getColumn("name").equals("the state of art smartphone")
					&& cursor[0].getColumn("price").equals("123.4")
					&& cursor[0].getColumn("on_sale").equals("true");
			}
		});
		
		cursor = table.getCursor();
		assertFalse(cursor.next());
		
		table.rollback(true);
		
		cursor = table.getCursor();
		assertTrue(cursor.next());
		
		iterator = cursor.getRecord();
		assertEquals("100", iterator.next());
		assertEquals("smartphone", iterator.next());
		assertEquals("123.4", iterator.next());
		assertEquals("true", iterator.next());
	}
}
