package onthego.database.tablespace.manager;

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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

@TestMethodOrder(MethodOrderer.Alphanumeric.class)
public class SingleTablespaceManagerTest {
	
	private static final byte[] MAGIC = {0x11, 0x10, 0x09, 0x08, 0x07, 0x06, 0x05, 0x04};

    private static final int RECORD_COUNT = 20;

	private static TablespaceManager tsManager;

    private List<Long> blockPosList = new ArrayList<>(RECORD_COUNT);
	
	@BeforeAll
	public static void setUp() throws Exception {
		createSingleTablespace();
	}

	@AfterAll
	public static void tearDown() throws Exception {
		if (tsManager != null) {
			tsManager.close();
		}
		
		removeSingleTablespace();
	}

	private static void createSingleTablespace() throws IOException {
		removeSingleTablespace();
		TablespaceHeader tsHeader = new SingleTablespaceHeader.Builder()
										.magic(MAGIC)
										.chunkSize(4)
										.crc(10)
										.firstBlockPos(100)
										.firstFreeBlockPos(0)
										.tableRootPos(200)
										.tableMetaInfoPos(300)
										.recordCount(0)
										.build();
		
		tsManager = SingleTablespaceManager.create("./dummytable.db", tsHeader);
	}

	private static void removeSingleTablespace() {
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
	public void A_Test_Create() {
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
			assertEquals(0, io.readInt());
		} catch(IOException ioe) {
			fail("io error occurred : " + ioe.getMessage());
		}
	}

	@Test
	public void B_Test_Load() throws IOException {
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
		assertEquals(0, tsHeader.getRecordCount());
	}
	
	@Test
	public void C_Test_CreateTableMetaInfo() {
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
	public void D_Test_LoadTableMetaInfo() {
		TableMetaInfo generatedTableMetaInfo = generateTableMetaInfo();
		
		tsManager.loadTableInfoEntry();
		TableMetaInfo loadedTableMetaInfo = tsManager.getHeader().getTableMetaInfo();

        assertEquals(loadedTableMetaInfo, generatedTableMetaInfo);
	}
	
	@Test
	public void E_Test_SaveRootPos() {
        long rootPos = tsManager.getRootPos();

		tsManager.saveRootPos(Long.MAX_VALUE);
		assertEquals(tsManager.getRootPos(), Long.MAX_VALUE);

        tsManager.saveRootPos(rootPos);
	}
	
	@Test
	public void F_Test_GetRootPos() {
		assertEquals(200, tsManager.getRootPos());
	}
	
	@Test
	public void G_Test_IncreaseRecordCount() {
		IntStream.range(0, RECORD_COUNT).forEach(i -> tsManager.increaseRecordCount());
		assertEquals(RECORD_COUNT, tsManager.getRecordCount());
	}
	
	@Test
	public void H_Test_DecreaseRecordCount() {
		IntStream.range(0, RECORD_COUNT).forEach(i -> tsManager.decreaseRecordCount());
		assertEquals(0, tsManager.getRecordCount());
	}

	@Test
	public void I_Test_Allocate() {
		int size = 3 * (Integer.BYTES + Long.BYTES);
		blockPosList = new ArrayList<>(RECORD_COUNT);
		
		IntStream.range(0, RECORD_COUNT).forEach(i -> {
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
		
		IntStream.range(0, RECORD_COUNT).forEach(i -> {
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
	public void J_Test_Free() {
		Random random = new Random(System.currentTimeMillis());
		// List<Long> blockPosList = new ArrayList<>(RECORD_COUNT);
		
		IntStream.range(0, RECORD_COUNT).forEach(i -> {
			int size = random.nextInt(128) + 1;
			long blockPos = tsManager.allocate(size);
			blockPosList.add(blockPos);
		});
		
		Collections.shuffle(blockPosList);
		IntStream.range(0, RECORD_COUNT).forEach(i -> {
			long blockPos = blockPosList.get(i);
			tsManager.free(blockPos);
			tsManager.printFreeListBlock();
		});
	}
	
	@Test
	public void K_Test_SaveHeader() {
		tsManager.saveRootPos(1024);
		tsManager.saveHeader();
		
		try (RandomAccessFile io = new RandomAccessFile("./dummytable.db", "r")) {
			byte[] writtenMagic = new byte[SingleTablespaceHeader.MAGIC_NUMBER_SIZE];
			io.read(writtenMagic);
			
			assertArrayEquals(tsManager.getHeader().getMagic(), writtenMagic);
			assertEquals(tsManager.getHeader().getChunkSize(), io.readInt());
			assertEquals(tsManager.getHeader().getCrc(), io.readInt());
			assertEquals(tsManager.getHeader().getFirstBlockPos(), io.readLong());
			assertEquals(tsManager.getHeader().getFirstFreeBlockPos(), io.readLong());
			assertEquals(tsManager.getRootPos(), io.readLong());
			assertEquals(tsManager.getHeader().getTableMetaInfoPos(), io.readLong());
			assertEquals(tsManager.getRecordCount(), io.readInt());
		} catch(IOException ioe) {
			fail("io error occurred : " + ioe.getMessage());
		}
	}
	
	@Test
	public void L_Test_LoadHeader() {
        tsManager.loadHeader();

        assertArrayEquals(MAGIC, tsManager.getHeader().getMagic());
        assertEquals(4, tsManager.getHeader().getChunkSize());
        assertEquals(10, tsManager.getHeader().getCrc());
        assertEquals(100, tsManager.getHeader().getFirstBlockPos());
        // assertEquals(80, tsManager.getHeader().getFirstFreeBlockPos());
        assertEquals(1024, tsManager.getRootPos());
        // assertEquals(300, tsManager.getHeader().getTableMetaInfoPos());
        assertEquals(0, tsManager.getRecordCount());
	}
}
