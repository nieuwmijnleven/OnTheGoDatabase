package onthego.database.core.tablespace.manager;

import onthego.database.core.exception.InsufficientPayloadSpaceException;
import onthego.database.core.table.meta.ColumnMeta;
import onthego.database.core.table.meta.TypeConstants;
import onthego.database.core.table.meta.Types;
import onthego.database.core.tablespace.meta.StandardTablespaceHeader;
import onthego.database.core.tablespace.meta.TableMetaInfo;
import onthego.database.core.tablespace.meta.TablespaceHeader;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

import static onthego.database.util.IOUtils.getByteBuffer;

public class StandardTablespaceManager implements TablespaceManager {

	public static final int BLOCK_HEADER_SIZE = Integer.BYTES;

	public static final int BLOCK_FOOTER_SIZE = Integer.BYTES;

	public static final int BLOCK_OVERHEAD_SIZE = BLOCK_HEADER_SIZE + BLOCK_FOOTER_SIZE;

	public static final int FREE_LIST_NODE_SIZE = 2 * Long.BYTES;

	public static final int BLOCK_OVERHEAD_WITH_FREELIST_SIZE = BLOCK_OVERHEAD_SIZE + FREE_LIST_NODE_SIZE;

    public static final int DEFAULT_BLOCK_SIZE = 4096;

    private final FileChannel channel;

	private TablespaceHeader tsHeader;

	static class FreeListNode {
		long prev;
		long next;

		FreeListNode(long prev, long next)
		{
			this.prev = prev;
			this.next = next;
		}
	}

    public static TablespaceManager create(String tsPath, TablespaceHeader tsHeader) throws IOException {
        return create(Path.of(tsPath), tsHeader);
    }

    public static TablespaceManager create(Path tsPath, TablespaceHeader tsHeader) throws IOException {
		return new StandardTablespaceManager(tsPath, tsHeader);
	}

    public static TablespaceManager load(String tsPath) throws IOException {
        return load(Path.of(tsPath));
    }

	public static TablespaceManager load(Path tsPath) throws IOException {
		return new StandardTablespaceManager(tsPath);
	}

	private StandardTablespaceManager(Path tsPath, TablespaceHeader tsHeader) throws IOException {
		this.channel = FileChannel.open(tsPath, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
		this.tsHeader = tsHeader;
		initialize();

		if (tsHeader.getTableMetaInfo() != null) {
			createTableInfoEntry(tsHeader.getTableMetaInfo());
		}
	}

	private StandardTablespaceManager(Path tsPath) throws IOException {
		this.channel = FileChannel.open(tsPath, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
		loadHeader();
		loadTableInfoEntry();
	}

    private void read(long position, ByteBuffer buffer) {
        try {
            channel.position(position);
            channel.read(buffer);
        } catch (IOException ioe) {
            throw new TablespaceManagerException(ioe);
        }
    }

    private void write(long position, ByteBuffer buffer) {
        try {
            channel.position(position);
            channel.write(buffer);
        } catch (IOException ioe) {
            throw new TablespaceManagerException(ioe);
        }
    }
		
	@Override
	public void loadHeader() {
		try {
            ByteBuffer buffer = getByteBuffer(DEFAULT_BLOCK_SIZE, true, buf -> read(0, buf));

            byte[] magic = new byte[StandardTablespaceHeader.MAGIC_NUMBER_SIZE];
			buffer.get(magic);
			
			tsHeader = new StandardTablespaceHeader.Builder()
							.magic(magic)
							.chunkSize(buffer.getInt())
							.crc(buffer.getInt())
							.firstBlockPos(buffer.getLong())
							.firstFreeBlockPos(buffer.getLong())
							.tableRootPos(buffer.getLong())
							.tableMetaInfoPos(buffer.getLong())
							.recordCount(buffer.getInt())
							.build();
			
		} catch(Exception ioe) {
			throw new TablespaceManagerException(ioe);
		}
	}
	
	@Override
	public void saveHeader() {
		try {
            ByteBuffer buffer = getByteBuffer(DEFAULT_BLOCK_SIZE, true, buf -> {
                buf.put(tsHeader.getMagic());
                buf.putInt(tsHeader.getChunkSize());
                buf.putInt(tsHeader.getCrc());
                buf.putLong(tsHeader.getFirstBlockPos());
                buf.putLong(tsHeader.getFirstFreeBlockPos());
                buf.putLong(tsHeader.getTableRootPos());
                buf.putLong(tsHeader.getTableMetaInfoPos());
                buf.putInt(tsHeader.getRecordCount());
            });

            write(0, buffer);
		} catch(Exception ioe) {
			throw new TablespaceManagerException(ioe);
		}
	}
	
	private void initialize() {
		try {
			//write the header into a tablespace first
			saveHeader();
			//mark a sentinel(dummy) area between tablespace header and data blocks
//            ByteBuffer buffer = ByteBuffer.allocate(DEFAULT_BLOCK_SIZE);
//            buffer.putInt(2);
//            buffer.flip();

            ByteBuffer buffer = getByteBuffer(DEFAULT_BLOCK_SIZE, true, buf -> buf.putInt(2));
            channel.write(buffer);
			//set the prolog block which is never freed for coalescing free blocks 
			allocate(0);
		} catch(Exception ioe) {
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
			for (ColumnMeta column : tableMetaInfo.getColumnList()) {   //<column_meta_info>
				out.writeUTF(column.getName());  //<column_name>
				out.writeUTF(column.getType().getTypeConstant().toString());  //<column_type>
				out.writeInt(column.getType().getLength());  //<column_type_length>
				out.writeInt(column.getType().getDecimalLength());  //<column_decimal_length>
			}
			
			out.flush();
			byte[] tableMetaInfoEntry = baout.toByteArray();
			
			tsHeader.setTableMetaInfoPos(allocate(tableMetaInfoEntry.length));
            write(tsHeader.getTableMetaInfoPos(), ByteBuffer.wrap(tableMetaInfoEntry));
            tsHeader.setTableMetaInfo(tableMetaInfo);
			saveHeader();
		} catch (Exception e) {
			throw new TablespaceManagerException(e);
		}
	}

    public void loadTableInfoEntry() {
        DataInputStream dataBuffer = null;
		try {
            ByteBuffer buffer = ByteBuffer.allocate(DEFAULT_BLOCK_SIZE);
            read(tsHeader.getTableMetaInfoPos(), buffer);

            dataBuffer = new DataInputStream(new ByteArrayInputStream(buffer.array()));

			String tableName = dataBuffer.readUTF();
			int columnCount = dataBuffer.readInt();
			List<ColumnMeta> columnList = new ArrayList<>();
			for (int i = 0; i < columnCount; ++i) { 
				String name = dataBuffer.readUTF();
				TypeConstants typeConstants = TypeConstants.valueOf(dataBuffer.readUTF());
				int length = dataBuffer.readInt();
				int decimalLength = dataBuffer.readInt();
				
				columnList.add(new ColumnMeta(name, Types.of(typeConstants, length, decimalLength)));
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
            ByteBuffer buffer = getByteBuffer(DEFAULT_BLOCK_SIZE, true, buf -> read(getBlockHeaderPos(payloadPos), buf));
            return buffer.getInt();
		} catch(Exception ie) {
            ie.printStackTrace();
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
        ByteBuffer buffer = getByteBuffer(DEFAULT_BLOCK_SIZE, true, buf -> buf.putInt(blockHeader));
        write(getBlockHeaderPos(payloadPos), buffer);
	}
	
	private long getNextBlockPos(long payloadPos) {
		return (payloadPos + getBlockSize(payloadPos));
	}

	private void putBlockFooter(long payloadPos, int blockFooter) {
        ByteBuffer buffer = getByteBuffer(DEFAULT_BLOCK_SIZE, true, buf -> buf.putInt(blockFooter));
        write(getNextBlockPos(payloadPos) - BLOCK_OVERHEAD_SIZE, buffer);
	}

	private long getPrevBlockPos(long payloadPos) {
		return payloadPos - getBlockSize(payloadPos - BLOCK_HEADER_SIZE);
	}
	
	private FreeListNode getFreeBlock(long payloadPos) {
		try {
            ByteBuffer buffer = getByteBuffer(DEFAULT_BLOCK_SIZE, true, buf ->
                    read(payloadPos, buf));
            return new FreeListNode(buffer.getLong(), buffer.getLong());
		} catch(Exception ie) {
			throw new TablespaceManagerException(ie);
		}
	}

    private void putFreeBlock(long payloadPos, FreeListNode node) {
        ByteBuffer buffer = getByteBuffer(DEFAULT_BLOCK_SIZE, true, buf -> {
            buf.putLong(node.prev);
            buf.putLong(node.next);
        });
        write(payloadPos, buffer);
	}

	private long getPrevFreeBlockPos(long payloadPos) {
		return getFreeBlock(payloadPos).prev;
	}

	private long getNextFreeBlockPos(long payloadPos) {
		return getFreeBlock(payloadPos).next;
	}
	
	private long increaseFileSize(long size) {   //like sbrk()
		try {
            long oldSize = channel.size();
			if (size == 0) return oldSize;
            write(oldSize + size - 1, ByteBuffer.wrap(new byte[]{0}));
			return oldSize;
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
		return new StandardTablespaceHeader(this.tsHeader);
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
		
		long bestfitBlockPos = -1;
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
		
//		System.out.println("========= free block info =========");
//		System.out.println("blockPos = " + blockPos);
//		System.out.println("size = " + size);
//		System.out.println("prevBlockAllocationStatus = " + prevBlockAllocationStatus);
//		System.out.println("nextBlockAllocationStatus = " + nextBlockAllocationStatus);
		
		//coalescing adjacent unallocated blocks
		if (prevBlockAllocationStatus && nextBlockAllocationStatus) {
			putBlockHeader(blockPos, pack(size, 0));
			putBlockFooter(blockPos, pack(size, 0));
			insertFreeBlock(blockPos);  // need to integrate PutBlockHeader() and PutBlockFooter() into InsertFreeBlock().
		} else if (!prevBlockAllocationStatus && nextBlockAllocationStatus) {
			long prevBlockPos = getPrevBlockPos(blockPos);
			int prevBlockSize = getBlockSize(prevBlockPos);
			int newSize = prevBlockSize + size;
			
//			System.out.println("prevBlockPos = " + prevBlockPos);
//			System.out.println("prevBlockSize = " + prevBlockSize);
//			System.out.println("newSize = " + newSize);

			putBlockHeader(prevBlockPos, pack(newSize, 0));
			putBlockFooter(prevBlockPos, pack(newSize, 0));
		} else if (prevBlockAllocationStatus && !nextBlockAllocationStatus) {
			long nextBlockPos = getNextBlockPos(blockPos);
			int nextBlockSize = getBlockSize(nextBlockPos);
			int newSize = size + nextBlockSize;
			
//			System.out.println("nextBlockPos = " + nextBlockPos);
//			System.out.println("nextBlockSize = " + nextBlockSize);
//			System.out.println("newSize = " + newSize);

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
			
//			System.out.println("prevBlockPos = " + prevBlockPos);
//			System.out.println("prevBlockSize = " + prevBlockSize);
//			System.out.println("nextBlockPos = " + nextBlockPos);
//			System.out.println("nextBlockSize = " + nextBlockSize);
//			System.out.println("newSize = " + newSize);

			putBlockHeader(prevBlockPos, pack(newSize, 0));
			putBlockFooter(prevBlockPos, pack(newSize, 0));

			deleteFreeBlock(nextBlockPos);
		}
		
//		System.out.println("=======================");
	}
	
	public byte[] readBlock(long blockPos) {
		int size = getBlockSize(blockPos) - BLOCK_OVERHEAD_SIZE;
		byte[] payload = new byte[size];

        ByteBuffer buffer = getByteBuffer(size, true, buf -> read(blockPos, buf));
        buffer.get(payload);
        return payload;
	}
	
	public void writeBlock(long blockPos, byte[] payload) throws InsufficientPayloadSpaceException {
		int size = getBlockSize(blockPos) - BLOCK_OVERHEAD_SIZE;
		if (size < payload.length) {
			throw new InsufficientPayloadSpaceException("The size(" + payload.length + ") of the payload to be written is larger than that(" +  size + ") of the target block.");
		}

        write(blockPos, ByteBuffer.wrap(payload));
	}
	
	public void close() {
		try {
			channel.close();
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
