package onthego.database.tablespace.manager;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import onthego.database.core.exception.InsufficientPayloadSpaceException;
import onthego.database.core.table.meta.ColumnMeta;
import onthego.database.core.table.meta.TypeConstants;
import onthego.database.core.table.meta.Types;
import onthego.database.core.tablespace.manager.SingleTablespaceManager;
import onthego.database.core.tablespace.manager.TablespaceManager;
import onthego.database.core.tablespace.manager.TablespaceManagerException;
import onthego.database.core.tablespace.meta.SingleTablespaceHeader;
import onthego.database.core.tablespace.meta.TableMetaInfo;
import onthego.database.core.tablespace.meta.TablespaceHeader;

public class SingleTablespaceManagerTest {
	
	private static final byte[] MAGIC = {0x11, 0x10, 0x09, 0x08, 0x07, 0x06, 0x05, 0x04};

	private TablespaceManager tsManager;
	
	@Before
	public void setUp() throws Exception {
		createSingleTablespace();
	}

	@After
	public void tearDown() throws Exception {
		if (tsManager != null) {
			tsManager.close();
		}
		
		removeSingleTablespace();
	}

	private void createSingleTablespace() throws IOException {
		removeSingleTablespace();
		TablespaceHeader tsHeader = new SingleTablespaceHeader.Builder()
										.magic(MAGIC)
										.chunkSize(4)
										.crc(10)
										.firstBlockPos(100)
										.firstFreeBlockPos(0)
										.tableRootPos(200)
										.tableMetaInfoPos(300)
										.recordCount(400)
										.build();
		
		tsManager = SingleTablespaceManager.create("./dummytable.db", tsHeader);
	}

	private void removeSingleTablespace() {
		File file = new File("./dummytable.db");
		if (file.exists()) {
			file.delete();
		}
	}
	
	private TableMetaInfo generateTableMetaInfo() {
		String tableName = "product";
		
		List<ColumnMeta> columnList = new ArrayList<>();
		columnList.add(new ColumnMeta("serial_no", Types.of(TypeConstants.INTEGER, 10, 0)));
		columnList.add(new ColumnMeta("name", Types.of(TypeConstants.CHAR, 20, 0)));
		columnList.add(new ColumnMeta("price", Types.of(TypeConstants.NUMERIC, 10, 3)));
		columnList.add(new ColumnMeta("on_sale", Types.of(TypeConstants.BOOL, 0, 0)));
		
		TableMetaInfo tableMetaInfo = new TableMetaInfo(tableName, columnList);
		tsManager.createTableInfoEntry(tableMetaInfo);
		return tableMetaInfo;
	}
	
	@Test
	public void testCreate() {
		try (RandomAccessFile io = new RandomAccessFile("./dummytable.db", "r")) {
			byte[] writtenMagic = new byte[SingleTablespaceHeader.MAGIC_NUMBER_SIZE];
			io.read(writtenMagic);
			
			assertArrayEquals(MAGIC, writtenMagic);
			assertEquals(4, io.readInt());
			assertEquals(10, io.readInt());
			assertEquals(100, io.readLong());
			assertEquals(0, io.readLong());
			assertEquals(200, io.readLong());
			assertEquals(300, io.readLong());
			assertEquals(400, io.readInt());
		} catch(IOException ioe) {
			fail("io error occurred : " + ioe.getMessage());
		}
	}

	@Test
	public void testLoad() throws IOException {
		generateTableMetaInfo();
		
		TablespaceManager tsManager = SingleTablespaceManager.load("./dummytable.db");
		tsManager.close();
		
		TablespaceHeader tsHeader = tsManager.getHeader();
		assertArrayEquals(MAGIC, tsHeader.getMagic());
		assertEquals(4, tsHeader.getChunkSize());
		assertEquals(10, tsHeader.getCrc());
		assertEquals(100, tsHeader.getFirstBlockPos());
		assertEquals(0, tsHeader.getFirstFreeBlockPos());
		assertEquals(200, tsHeader.getTableRootPos());
		assertEquals(80, tsHeader.getTableMetaInfoPos());
		assertEquals(400, tsHeader.getRecordCount());
	}
	
	@Test
	public void testCreateTableMetaInfo() {
		TableMetaInfo tableMetaInfo = generateTableMetaInfo();
		
		long tableMetaInfoPos = tsManager.getHeader().getTableMetaInfoPos();
		try (RandomAccessFile io = new RandomAccessFile("./dummytable.db", "r")) {
			io.seek(tableMetaInfoPos);

			assertEquals(tableMetaInfo.getTableName(), io.readUTF());
			assertEquals(tableMetaInfo.getColumnList().size(), io.readInt());
			
			for (int i = 0; i < tableMetaInfo.getColumnList().size(); ++i) {
				ColumnMeta column = tableMetaInfo.getColumnList().get(i);
				assertEquals(column.getName(), io.readUTF());
				assertEquals(column.getType().getTypeConstant(), TypeConstants.valueOf(io.readUTF()));
				assertEquals(column.getType().getLength(), io.readInt());
				assertEquals(column.getType().getDecimalLength(), io.readInt());
			}
		} catch(IOException ioe) {
			fail("io error occurred : " + ioe.getMessage());
		}
	}

	@Test
	public void testLoadTableMetaInfo() {
		TableMetaInfo generatedTableMetaInfo = generateTableMetaInfo();
		
		tsManager.loadTableInfoEntry();
		TableMetaInfo loadedTableMetaInfo = tsManager.getHeader().getTableMetaInfo();
		
		assertTrue(loadedTableMetaInfo.equals(generatedTableMetaInfo));
	}
	
	@Test
	public void testSaveRootPos() {
		tsManager.saveRootPos(Long.MAX_VALUE);
		assertEquals(tsManager.getRootPos(), Long.MAX_VALUE);
	}
	
	@Test
	public void testGetRootPos() {
		assertEquals(200, tsManager.getRootPos());
	}
	
	@Test
	public void testIncreaseRecordCount() {
		IntStream.range(0, 200).forEach(i -> tsManager.increaseRecordCount());
		assertEquals(600, tsManager.getRecordCount());
	}
	
	@Test
	public void testDecreaseRecordCount() {
		IntStream.range(0, 100).forEach(i -> tsManager.decreaseRecordCount());
		assertEquals(300, tsManager.getRecordCount());
	}

	@Test
	public void testGetRecordCount() {
		IntStream.range(0, 200).forEach(i -> tsManager.increaseRecordCount());
		IntStream.range(0, 100).forEach(i -> tsManager.decreaseRecordCount());
		assertEquals(500, tsManager.getRecordCount());
	}

	@Test
	public void testAllocate() {
		int size = 3 * (Integer.BYTES + Long.BYTES);
		List<Long> blockPosList = new ArrayList<>(500);
		
		IntStream.range(0, 500).forEach(i -> {
			long blockPos = tsManager.allocate(size);
			blockPosList.add(blockPos);
			
			try (ByteArrayOutputStream bout = new ByteArrayOutputStream();
				 DataOutputStream dout = new DataOutputStream(bout)) {
				dout.writeInt(i);
				dout.writeInt(i + 1);
				dout.writeInt(i + 2);
				dout.writeLong(i + 3);
				dout.writeLong(i + 4);
				dout.writeLong(i + 5);
				
				byte[] payload = bout.toByteArray();
				tsManager.writeBlock(blockPos, payload);
			} catch(InsufficientPayloadSpaceException ioe) {
				throw new TablespaceManagerException(ioe);
			} catch(IOException ioe) {
				throw new TablespaceManagerException(ioe);
			}
		});
		
		IntStream.range(0, 500).forEach(i -> {
			long blockPos = blockPosList.get(i);

			try (ByteArrayInputStream bin = new ByteArrayInputStream(tsManager.readBlock(blockPos));
				 DataInputStream din = new DataInputStream(bin)) {
				assertEquals(i, din.readInt());
				assertEquals(i + 1, din.readInt());
				assertEquals(i + 2, din.readInt());
				assertEquals(i + 3, din.readLong());
				assertEquals(i + 4, din.readLong());
				assertEquals(i + 5, din.readLong());
			} catch(IOException ioe) {
				throw new TablespaceManagerException(ioe);
			}
		});
	}

	@Test
	public void testFree() {
		Random random = new Random(System.currentTimeMillis());
		List<Long> blockPosList = new ArrayList<>(500);
		
		IntStream.range(0, 500).forEach(i -> {
			int size = random.nextInt(128) + 1;
			long blockPos = tsManager.allocate(size);
			blockPosList.add(blockPos);
		});
		
		Collections.shuffle(blockPosList);
		IntStream.range(0, 500).forEach(i -> {
			long blockPos = blockPosList.get(i);
			tsManager.free(blockPos);
			tsManager.printFreeListBlock();
		});
	}
	
	@Test
	public void testSaveHeader() {
		testGetRecordCount();
		tsManager.saveRootPos(1024);
		tsManager.saveHeader();
		
		try (RandomAccessFile io = new RandomAccessFile("./dummytable.db", "r")) {
			byte[] writtenMagic = new byte[SingleTablespaceHeader.MAGIC_NUMBER_SIZE];
			io.read(writtenMagic);
			
			assertArrayEquals(MAGIC, writtenMagic);
			assertEquals(4, io.readInt());
			assertEquals(10, io.readInt());
			assertEquals(100, io.readLong());
			assertEquals(0, io.readLong());
			assertEquals(1024, io.readLong());
			assertEquals(300, io.readLong());
			assertEquals(500, io.readInt());
		} catch(IOException ioe) {
			fail("io error occurred : " + ioe.getMessage());
		}
	}
	
	@Test
	public void testLoadHeader() {
		try (RandomAccessFile io = new RandomAccessFile("./dummytable.db", "rws")) {
			io.seek(0);
			io.write(MAGIC);
			io.writeInt(4);
			io.writeInt(10);
			io.writeLong(100);
			io.writeLong(0);
			io.writeLong(1024);
			io.writeLong(300);
			io.writeInt(500);
		} catch(IOException ioe) {
			fail("io error occurred : " + ioe.getMessage());
		}
		
		tsManager.loadHeader();
		assertEquals(1024, tsManager.getRootPos());
		assertEquals(500, tsManager.getRecordCount());
	}
}
