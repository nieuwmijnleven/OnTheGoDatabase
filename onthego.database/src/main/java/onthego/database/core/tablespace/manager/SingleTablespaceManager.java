package onthego.database.core.tablespace.manager;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

import onthego.database.core.exception.MarginalPayloadSpaceException;
import onthego.database.core.index.BTreeIndex;
import onthego.database.core.table.meta.ColumnType;
import onthego.database.core.table.meta.Type;
import onthego.database.core.table.meta.TypeConstants;
import onthego.database.core.tablespace.meta.SingleTablespaceHeader;
import onthego.database.core.tablespace.meta.TableMetaInfo;
import onthego.database.core.tablespace.meta.TablespaceHeader;

public class SingleTablespaceManager implements TablespaceManager {
	
	public static final int BLOCK_HEADER_SIZE = Integer.BYTES;
	
	public static final int BLOCK_FOOTER_SIZE = Integer.BYTES;
	
	public static final int BLOCK_OVERHEAD_SIZE = BLOCK_HEADER_SIZE + BLOCK_FOOTER_SIZE;
	
	public static final int FREE_LIST_NODE_SIZE = 2 * Long.BYTES;
	
	public static final int BLOCK_OVERHEAD_WITH_FREELIST_SIZE = BLOCK_OVERHEAD_SIZE + FREE_LIST_NODE_SIZE;
	
	private RandomAccessFile io;
	
	private TablespaceHeader tsHeader;
	
	static class FreeListNode {
		long prev;
		long next;
		
		FreeListNode(long prev, long next)
		{
			this.prev = prev;
			this.next = next;
		}
	};
	
	public static TablespaceManager create(String tsPath, TablespaceHeader tsHeader) throws IOException {
		return new SingleTablespaceManager(tsPath, tsHeader);
	}
	
	public static TablespaceManager load(String tsPath) throws IOException {
		return new SingleTablespaceManager(tsPath);
	}
	
	private SingleTablespaceManager(String tsPath, TablespaceHeader tsHeader) throws IOException {
		this.io = new RandomAccessFile(tsPath, "rws");
		this.tsHeader = tsHeader; 
		initialize();
		
		if (tsHeader.getTableMetaInfo() != null) {
			createTableInfoEntry(tsHeader.getTableMetaInfo());
		}
	}
	
	private SingleTablespaceManager(String tsPath) throws IOException {
		this.io = new RandomAccessFile(tsPath, "rws");
		loadHeader();
		loadTableInfoEntry();
	}
		
	@Override
	public void loadHeader() {
		try {
			io.seek(0);
			
			byte[] magic = new byte[SingleTablespaceHeader.MAGIC_NUMBER_SIZE];
			io.read(magic);
			
			tsHeader = new SingleTablespaceHeader.Builder()
							.magic(magic)
							.chunkSize(io.readInt())
							.crc(io.readInt())
							.firstBlockPos(io.readLong())
							.firstFreeBlockPos(io.readLong())
							.tableRootPos(io.readLong())
							.tableMetaInfoPos(io.readLong())
							.recordCount(io.readInt())
							.build();
			
		} catch(Exception ioe) {
			throw new TablespaceManagerException(ioe);
		}
	}
	
	@Override
	public void saveHeader() {
		try {
			io.seek(0);
			io.write(tsHeader.getMagic());
			io.writeInt(tsHeader.getChunkSize());
			io.writeInt(tsHeader.getCrc());
			io.writeLong(tsHeader.getFirstBlockPos());
			io.writeLong(tsHeader.getFirstFreeBlockPos());
			io.writeLong(tsHeader.getTableRootPos());
			io.writeLong(tsHeader.getTableMetaInfoPos());
			io.writeLong(tsHeader.getRecordCount());
		} catch(Exception ioe) {
			throw new TablespaceManagerException(ioe);
		}
	}
	
	private void initialize() {
		try {
			//write the header into a tablespace first
			saveHeader();
			//mark a sentinel(dummy) area between tablespace header and data blocks
			io.writeInt(1);
			//set the prolog block which is never freed for coalescing free blocks 
			allocate(0);
		} catch(Exception ioe) {
			ioe.printStackTrace();
			throw new TablespaceManagerException(ioe);
		}
	}

	//<table_name><column_count>
	//<(row_id,type_id,length,decimal_len),(column_name,type_id,length,decimal_len),...>
	public void createTableInfoEntry(TableMetaInfo tableMetaInfo) {
		try (ByteArrayOutputStream baout = new ByteArrayOutputStream();
			 DataOutputStream out = new DataOutputStream(baout)) {
			
			out.writeUTF(tableMetaInfo.getTableName());  //<tableName>
			out.writeInt(tableMetaInfo.getColumnList().size());  //<column_count>
			for (ColumnType column : tableMetaInfo.getColumnList()) {   //<column_meta_info>
				out.writeUTF(column.getName());  //<column_name>
				out.writeUTF(column.getType().getTypeConstant().toString());  //<column_type>
				out.writeInt(column.getType().getLength());  //<column_type_length>
				out.writeInt(column.getType().getDecimalLength());  //<column_decimal_length>
			}
			
			out.flush();
			byte[] tableMetaInfoEntry = baout.toByteArray();
			
			tsHeader.setTableMetaInfoPos(allocate(tableMetaInfoEntry.length));
			io.seek(tsHeader.getTableMetaInfoPos());
			io.write(tableMetaInfoEntry);
			tsHeader.setTableMetaInfo(tableMetaInfo);
			saveHeader();
		} catch (Exception e) {
			throw new TablespaceManagerException(e);
		}
	}

	public void loadTableInfoEntry() {
		try {
			io.seek(tsHeader.getTableMetaInfoPos());
			
			String tableName = io.readUTF();
			int columnCount = io.readInt();
			List<ColumnType> columnList = new ArrayList<>();
			for (int i = 0; i < columnCount; ++i) { 
				String name = io.readUTF();
				TypeConstants typeConstants = TypeConstants.valueOf(io.readUTF());
				int length = io.readInt();
				int decimalLength = io.readInt();
				
				columnList.add(new ColumnType(name, Type.of(typeConstants, length, decimalLength)));
			}
			
			tsHeader.setTableMetaInfo(new TableMetaInfo(tableName, columnList));
		} catch(IOException ioe) {
			throw new TablespaceManagerException(ioe);
		}
	}
	
	// The aligned size is multiplication of chunksize
	private int getChunkAlignedSize(int size) {
		return (size + (tsHeader.getChunkSize() - 1)) & ~(tsHeader.getChunkSize() - 1);
	}

	private long getBlockHeaderPos(long payloadPos) {
		return (payloadPos - BLOCK_HEADER_SIZE);
	}

	private int getBlockHeader(long payloadPos) {
		try {
			io.seek(getBlockHeaderPos(payloadPos));
			return io.readInt();
		} catch(Exception ie) {
			throw new TablespaceManagerException(ie);
		}
	}

	private int getBlockSize(long payloadPos) {
		int blockHeader = getBlockHeader(payloadPos);
		return (blockHeader & ~(tsHeader.getChunkSize() - 1));
	}

	private boolean getBlockAllocationStatus(long payloadPos) {
		int blockHeader = getBlockHeader(payloadPos);
		return (blockHeader & 1) == 1;
	}

	private int pack(int size, int allocStatus) {
		return (size | allocStatus);
	}

	private void putBlockHeader(long payloadPos, int blockHeader) {
		try {
			io.seek(getBlockHeaderPos(payloadPos));
			io.writeInt(blockHeader);
		} catch(Exception ie) {
			throw new TablespaceManagerException(ie);
		}
	}
	
	private long getNextBlockPos(long payloadPos) {
		return (payloadPos + getBlockSize(payloadPos));
	}

	private void putBlockFooter(long payloadPos, int blockFooter) {
		try {
			io.seek(getNextBlockPos(payloadPos) - BLOCK_OVERHEAD_SIZE);
			io.writeInt(blockFooter);
		} catch(Exception ie) {
			throw new TablespaceManagerException(ie);
		}
	}

	private long getPrevBlockPos(long payloadPos) {
		return payloadPos - getBlockSize(payloadPos - BLOCK_HEADER_SIZE);
	}
	
	private FreeListNode getFreeBlock(long payloadPos) {
		try {
			io.seek(payloadPos);
			return new FreeListNode(io.readLong(), io.readLong());
		} catch(Exception ie) {
			throw new TablespaceManagerException(ie);
		}
	}

	private void putFreeBlock(long payloadPos, FreeListNode node) {
		try {
			io.seek(payloadPos);
			io.writeLong(node.prev);
			io.writeLong(node.next);
		} catch(Exception ie) {
			throw new TablespaceManagerException(ie);
		}
	}

	private long getPrevFreeBlockPos(long payloadPos) {
		return getFreeBlock(payloadPos).prev;
	}

	private long getNextFreeBlockPos(long payloadPos) {
		return getFreeBlock(payloadPos).next;
	}
	
	private long increaseFileSize(long size) {   //like sbrk()
		try {
			long oldPos = io.length();
			if (size == 0)
				return oldPos;
	
			long pos = oldPos + size - 1;
			try {
				io.seek(pos);
				io.writeByte(0);
			} catch (IOException ie) {
				throw new TablespaceManagerException(ie);
			}
			
			return oldPos;
		} catch(Exception ioe) {
			throw new TablespaceManagerException(ioe);
		}
	}

	private void initializeBlock(long payloadPos, int size) {
		int remainder = getBlockSize(payloadPos) - size;
		if (remainder >= getChunkAlignedSize(BLOCK_OVERHEAD_WITH_FREELIST_SIZE)) {
			putBlockHeader(payloadPos, pack(size, 1));
			putBlockFooter(payloadPos, pack(size, 1));
			deleteFreeBlock(payloadPos);

			putBlockHeader(getNextBlockPos(payloadPos), pack(remainder, 0));
			putBlockFooter(getNextBlockPos(payloadPos), pack(remainder, 0));
			insertFreeBlock(getNextBlockPos(payloadPos));
		} else {
			putBlockHeader(payloadPos, pack(getBlockSize(payloadPos), 1));
			putBlockFooter(payloadPos, pack(getBlockSize(payloadPos), 1));
			deleteFreeBlock(payloadPos);
		}
	}

	private void extend(int increase) {
		long payloadPos = increaseFileSize(increase);
		putBlockHeader(payloadPos, pack(increase, 0));
		putBlockFooter(payloadPos, pack(increase, 0));
		insertFreeBlock(payloadPos);

		putBlockHeader(getNextBlockPos(payloadPos), pack(0,1));
	}

	//This code will be rewrited using dummy node.
	private void insertFreeBlock(long newFreeBlockPos) {
		long firstFreeBlockPos = tsHeader.getFirstFreeBlockPos();
		
		//in case that there is no free block in free block list
		if (firstFreeBlockPos == 0) {  
			FreeListNode newFreeNode = new FreeListNode(newFreeBlockPos, newFreeBlockPos);
			putFreeBlock(newFreeBlockPos, newFreeNode);
			
			tsHeader.setFirstFreeBlockPos(newFreeBlockPos);
			saveHeader();
			return;
		}

		FreeListNode firstFreeNode = getFreeBlock(firstFreeBlockPos);
		
		//set links of a new free node
		FreeListNode newFreeNode = new FreeListNode(firstFreeNode.prev, firstFreeBlockPos);
		putFreeBlock(newFreeBlockPos, newFreeNode);
		
		//update links of the first free node and its previous free node
		if (firstFreeBlockPos != firstFreeNode.prev) {
			FreeListNode prevFreeNode = getFreeBlock(firstFreeNode.prev);
			prevFreeNode.next = newFreeBlockPos;
			putFreeBlock(firstFreeNode.prev, prevFreeNode);
			
			firstFreeNode.prev = newFreeBlockPos;
			putFreeBlock(firstFreeBlockPos, firstFreeNode);
		} else {
			firstFreeNode.next = newFreeBlockPos;
			firstFreeNode.prev = newFreeBlockPos;
			putFreeBlock(firstFreeBlockPos, firstFreeNode);
		}
		
		//update tablespace header ()
//		tsHeader.setFirstFreeBlockPos(newFreeBlockPos);
//		saveHeader();
	}

	private void deleteFreeBlock(long freeBlockPos) {
		FreeListNode freeNode = getFreeBlock(freeBlockPos);

		 //in case that there is the only one header node. that is, size(list) = 1.
		if (freeNode.prev == freeBlockPos && freeNode.next == freeBlockPos) { 
//			freeNode.prev = 0;
//			freeNode.next = 0;
//			putFreeBlock(freeBlockPos, freeNode);

			tsHeader.setFirstFreeBlockPos(0);
			saveHeader();
			return;
		}

		FreeListNode prevFreeNode = getFreeBlock(freeNode.prev);
		//remove the header node from free list when size(list) >= 2.
		if (freeNode.prev == freeNode.next) {  //size(list) == 2
			prevFreeNode.prev = freeNode.prev;
			prevFreeNode.next = freeNode.prev;
			putFreeBlock(freeNode.prev, prevFreeNode);
		} else { //size(list) > 2
			FreeListNode nextFreeNode = getFreeBlock(freeNode.next);
			prevFreeNode.next = freeNode.next;
			nextFreeNode.prev = freeNode.prev;
			putFreeBlock(freeNode.prev, prevFreeNode);
			putFreeBlock(freeNode.next, nextFreeNode);
		}

		//in case that the removed free node is the first free node
		if (tsHeader.getFirstFreeBlockPos() == freeBlockPos) {
			tsHeader.setFirstFreeBlockPos(freeNode.next);
			saveHeader();
		}
	}
	
	@Override
	public TablespaceHeader getHeader() {
		return new SingleTablespaceHeader(this.tsHeader);
	}

	@Override
	public long getRootPos() {
		return tsHeader.getTableRootPos();
	}

	@Override
	public void saveRootPos(long rootPos) {
		tsHeader.setTableRootPos(rootPos);
		saveHeader();
	}

	@Override
	public int getRecordCount() {
		return tsHeader.getRecordCount();
	}

	@Override
	public int increaseRecordCount() {
		tsHeader.setRecordCount(tsHeader.getRecordCount() + 1);
		saveHeader();
		return tsHeader.getRecordCount();
	}
	
	@Override
	public void decreaseRecordCount() {
		tsHeader.setRecordCount(tsHeader.getRecordCount() - 1);
		saveHeader();
	}

	@Override
	public long allocate(int size)  {
		int alignedSize = 0;
		if (size >= FREE_LIST_NODE_SIZE) {
			alignedSize = getChunkAlignedSize(size + BLOCK_OVERHEAD_SIZE);
		} else {
			alignedSize = getChunkAlignedSize(BLOCK_OVERHEAD_WITH_FREELIST_SIZE);
		}
		
		long bestfitBlockPos = (long)-1;
		long freeBlockPos = tsHeader.getFirstFreeBlockPos();
		long prevFreeBlockPos = 0;
		while(prevFreeBlockPos != freeBlockPos) {
			if (getBlockSize(freeBlockPos) >= alignedSize) {
				if (bestfitBlockPos == -1 || getBlockSize(bestfitBlockPos) > getBlockSize(freeBlockPos)) {
					bestfitBlockPos = freeBlockPos;
				}
			}
			
			prevFreeBlockPos = freeBlockPos;
			freeBlockPos = getNextFreeBlockPos(freeBlockPos);
			
			//in case that the searching returns to the first free node
			if (freeBlockPos == tsHeader.getFirstFreeBlockPos())
				break;
		}

		//  There is no free blocks to use or no a free block larger than the requested size
		if (tsHeader.getFirstFreeBlockPos() == 0 || bestfitBlockPos == -1) {
			bestfitBlockPos = increaseFileSize(0);
			extend(alignedSize);
		}

		initializeBlock(bestfitBlockPos, alignedSize);
		return bestfitBlockPos;
	}

	@Override
	public void free(long blockPos) {
		int size = getBlockSize(blockPos);
		boolean prevBlockAllocationStatus = getBlockAllocationStatus(getPrevBlockPos(blockPos));
		boolean nextBlockAllocationStatus = getBlockAllocationStatus(getNextBlockPos(blockPos));
		
		System.out.println("=======================");
		System.out.println("blockPos = " + blockPos);
		System.out.println("size = " + size);
		System.out.println("prevBlockAllocationStatus = " + prevBlockAllocationStatus);
		System.out.println("nextBlockAllocationStatus = " + nextBlockAllocationStatus);
		
		
		//coalescing adjacent unallocated blocks
		if (prevBlockAllocationStatus && nextBlockAllocationStatus) {
			putBlockHeader(blockPos, pack(size, 0));
			putBlockFooter(blockPos, pack(size, 0));
			insertFreeBlock(blockPos);  // need to integrate PutBlockHeader() and PutBlockFooter() into InsertFreeBlock().
		} else if (!prevBlockAllocationStatus && nextBlockAllocationStatus) {
			long prevBlockPos = getPrevBlockPos(blockPos);
			int prevBlockSize = getBlockSize(prevBlockPos);
			int newSize = prevBlockSize + size;
			
			System.out.println("prevBlockPos = " + prevBlockPos);
			System.out.println("prevBlockSize = " + prevBlockSize);
			System.out.println("newSize = " + newSize);

			putBlockHeader(prevBlockPos, pack(newSize, 0));
			putBlockFooter(prevBlockPos, pack(newSize, 0));
		} else if (prevBlockAllocationStatus && !nextBlockAllocationStatus) {
			long nextBlockPos = getNextBlockPos(blockPos);
			int nextBlockSize = getBlockSize(nextBlockPos);
			int newSize = size + nextBlockSize;
			
			System.out.println("nextBlockPos = " + nextBlockPos);
			System.out.println("nextBlockSize = " + nextBlockSize);
			System.out.println("newSize = " + newSize);

			putBlockHeader(blockPos, pack(newSize, 0));
			putBlockFooter(blockPos, pack(newSize, 0));

			deleteFreeBlock(nextBlockPos);
			insertFreeBlock(blockPos);
		} else { //!prevBlockAllocationStatus && !nextBlockAllocationStatus
			long prevBlockPos = getPrevBlockPos(blockPos);
			int prevBlockSize = getBlockSize(prevBlockPos);
			long nextBlockPos = getNextBlockPos(blockPos);
			int nextBlockSize = getBlockSize(nextBlockPos);
			int newSize = prevBlockSize + size + nextBlockSize;
			
			System.out.println("prevBlockPos = " + prevBlockPos);
			System.out.println("prevBlockSize = " + prevBlockSize);
			System.out.println("nextBlockPos = " + nextBlockPos);
			System.out.println("nextBlockSize = " + nextBlockSize);
			System.out.println("newSize = " + newSize);

			putBlockHeader(prevBlockPos, pack(newSize, 0));
			putBlockFooter(prevBlockPos, pack(newSize, 0));

			deleteFreeBlock(nextBlockPos);
		}
		
		System.out.println("=======================");
	}
	
	public byte[] readBlock(long blockPos) {
		int size = getBlockSize(blockPos) - BLOCK_OVERHEAD_SIZE;
		byte[] payload = new byte[size];
		
		try {
			io.seek(blockPos);
			io.read(payload);
			return payload;
		} catch (IOException ioe) {
			throw new TablespaceManagerException(ioe);
		}
	}
	
	public void writeBlock(long blockPos, byte[] payload) throws MarginalPayloadSpaceException {
		int size = getBlockSize(blockPos) - BLOCK_OVERHEAD_SIZE;
		if (size < payload.length) {
			throw new MarginalPayloadSpaceException("The size(" + payload.length + ") of the payload to be written is larger than that(" +  size + ") of the target block.");
		}
		
		try {
			io.seek(blockPos);
			io.write(payload);
		} catch (IOException ioe) {
			throw new TablespaceManagerException(ioe);
		}
	}
	
	public void close() {
		try {
			io.close();
		} catch (IOException ioe) {
			throw new TablespaceManagerException(ioe);
		}
	}

	@Override
	public void printFreeListBlock() {
		long firstFreeBlockPos = tsHeader.getFirstFreeBlockPos();
		long freeBlockPos = firstFreeBlockPos;
		long prevFreeBlockPos = 0;

		while (freeBlockPos != prevFreeBlockPos) {
			System.out.printf("(offset= %d, size= %d)", freeBlockPos, getBlockSize(freeBlockPos));

			prevFreeBlockPos = freeBlockPos;
			freeBlockPos = getNextFreeBlockPos(freeBlockPos);
//			if (freeBlockPos == -1) {
//				System.out.println("could not read the next free block.");
//				return;
//			}

			if (freeBlockPos == firstFreeBlockPos)
				break;

			System.out.print("->");
		}

		System.out.println();
	}
}
