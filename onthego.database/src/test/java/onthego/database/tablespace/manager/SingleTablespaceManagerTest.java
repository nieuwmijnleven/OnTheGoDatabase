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

import onthego.database.core.exception.MarginalPayloadSpaceException;
import onthego.database.core.table.meta.ColumnType;
import onthego.database.core.table.meta.Type;
import onthego.database.core.table.meta.TypeConstants;
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
	
	@Test
	public void testCreate() {
		try (RandomAccessFile io = new RandomAccessFile("./dummytable.db", "r")) {
			byte[] writtenMagic = new byte[SingleTablespaceHeader.MAGIC_NUMBER_SIZE];
			io.read(writtenMagic);
			
			assertArrayEquals(MAGIC, writtenMagic);
			assertEquals(io.readInt(), 4);
			assertEquals(io.readInt(), 10);
			assertEquals(io.readLong(), 100);
			assertEquals(io.readLong(), 0);
			assertEquals(io.readLong(), 200);
			assertEquals(io.readLong(), 300);
			assertEquals(io.readLong(), 400);
		} catch(IOException ioe) {
			fail("io error occurred : " + ioe.getMessage());
		}
	}

	@Test
	public void testLoad() {
		TablespaceManager tsManager = null;
		try {
			tsManager = SingleTablespaceManager.load("./dummytable.db");
			tsManager.close();
		} catch(Exception e) {
			fail("could not load a Tablespace");
		} 
		
		TablespaceHeader tsHeader = tsManager.getHeader();
		
		assertArrayEquals(tsHeader.getMagic(), MAGIC);
		assertEquals(tsHeader.getChunkSize(), 4);
		assertEquals(tsHeader.getCrc(), 10);
		assertEquals(tsHeader.getFirstBlockPos(), 100);
		assertEquals(tsHeader.getFirstFreeBlockPos(), 0);
		assertEquals(tsHeader.getTableRootPos(), 200);
		assertEquals(tsHeader.getTableMetaInfoPos(), 300);
		assertEquals(tsHeader.getRecordCount(), 400);
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
				ColumnType column = tableMetaInfo.getColumnList().get(i);
				assertEquals(column.getName(), io.readUTF());
				assertEquals(column.getType().getTypeConstant(), TypeConstants.valueOf(io.readUTF()));
				assertEquals(column.getType().getLength(), io.readInt());
				assertEquals(column.getType().getDecimalLength(), io.readInt());
			}
		} catch(IOException ioe) {
			fail("io error occurred : " + ioe.getMessage());
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
		tsManager.createTableInfoEntry(tableMetaInfo);
		return tableMetaInfo;
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
		assertEquals(tsManager.getRootPos(), 200);
	}
	
	@Test
	public void testIncreaseRecordCount() {
		IntStream.range(0, 200).forEach(i -> tsManager.increaseRecordCount());
		assertEquals(tsManager.getRecordCount(), 600);
	}
	
	@Test
	public void testDecreaseRecordCount() {
		IntStream.range(0, 100).forEach(i -> tsManager.decreaseRecordCount());
		assertEquals(tsManager.getRecordCount(), 300);
	}

	@Test
	public void testGetRecordCount() {
		IntStream.range(0, 200).forEach(i -> tsManager.increaseRecordCount());
		IntStream.range(0, 100).forEach(i -> tsManager.decreaseRecordCount());
		assertEquals(tsManager.getRecordCount(), 500);
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
			} catch(MarginalPayloadSpaceException ioe) {
				throw new TablespaceManagerException(ioe);
			} catch(IOException ioe) {
				throw new TablespaceManagerException(ioe);
			}
		});
		
		IntStream.range(0, 500).forEach(i -> {
			long blockPos = blockPosList.get(i);

			try (ByteArrayInputStream bin = new ByteArrayInputStream(tsManager.readBlock(blockPos));
				 DataInputStream din = new DataInputStream(bin)) {
				assertEquals(din.readInt(), i);
				assertEquals(din.readInt(), i + 1);
				assertEquals(din.readInt(), i + 2);
				assertEquals(din.readLong(), i + 3);
				assertEquals(din.readLong(), i + 4);
				assertEquals(din.readLong(), i + 5);
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
			assertEquals(io.readInt(), 4);
			assertEquals(io.readInt(), 10);
			assertEquals(io.readLong(), 100);
			assertEquals(io.readLong(), 0);
			assertEquals(io.readLong(), 1024);
			assertEquals(io.readLong(), 300);
			assertEquals(io.readLong(), 500);
		} catch(IOException ioe) {
			fail("io error occurred : " + ioe.getMessage());
		}
	}

	@Test
	public void testLoadHeader() {
		try (RandomAccessFile io = new RandomAccessFile("./dummytable.db", "rws")) {
			io.write(MAGIC);
			io.writeInt(4);
			io.writeInt(10);
			io.writeLong(100);
			io.writeLong(0);
			io.writeLong(1024);
			io.writeLong(300);
			io.writeLong(500);
		} catch(IOException ioe) {
			fail("io error occurred : " + ioe.getMessage());
		}
		
		tsManager.loadHeader();
		assertEquals(tsManager.getRootPos(), 1024);
		assertEquals(tsManager.getRecordCount(), 500);
	}
}
