package onthego.database.core.tablespace.meta;

public class SingleTablespaceHeader implements TablespaceHeader {
	
	public static final int MAGIC_NUMBER_SIZE = 8;
	
	private byte[] magic;
	
	private int chunkSize;
	
	private int crc;
	
	private long firstBlockPos;  
	
	private long firstFreeBlockPos;
	
	//btree root, node type => <rowid, record_bp, (len, primary_key_value)>
	private long tableRootPos; 
	
	private long tableMetaInfoPos;  
	
	private TableMetaInfo tableMetaInfo;
	
	private int recordCount;
	
	public static class Builder {
		
		private byte[] magic = {0x01, 0x02, 0x03, 0x04, 0x0A, 0x0B, 0x0C, 0x0D};
		
		private int chunkSize = 4;
		
		private int crc = 0;
		
		private long firstBlockPos = 0;  
		
		private long firstFreeBlockPos = 0;
		
		private long tableRootPos = 0; 
		
		private long tableMetaInfoPos = 0;
		
		private TableMetaInfo tableMetaInfo;
		
		private int recordCount = 0;
		
		public Builder() {}
		
		public Builder magic(byte[] magic) {
			this.magic = magic;
			return this;
		}

		public Builder chunkSize(int chunkSize) {
			this.chunkSize = chunkSize;
			return this;
		}

		public Builder crc(int crc) {
			this.crc = crc;
			return this;
		}

		public Builder firstBlockPos(long firstBlockPos) {
			this.firstBlockPos = firstBlockPos;
			return this;
		}

		public Builder firstFreeBlockPos(long firstFreeBlockPos) {
			this.firstFreeBlockPos = firstFreeBlockPos;
			return this;
		}

		public Builder tableRootPos(long tableRootPos) {
			this.tableRootPos = tableRootPos;
			return this;
		}

		public Builder tableMetaInfoPos(long tableMetaInfoPos) {
			this.tableMetaInfoPos = tableMetaInfoPos;
			return this;
		}
		
		public Builder tableMetaInfo(TableMetaInfo tableMetaInfo) {
			this.tableMetaInfo = tableMetaInfo;
			return this;
		}

		public Builder recordCount(int recordCount) {
			this.recordCount = recordCount;
			return this;
		}
		
		public TablespaceHeader build() {
			return new SingleTablespaceHeader(this);
		}
	}
	
	protected SingleTablespaceHeader(Builder builder) {
		this.magic = builder.magic;
		this.chunkSize = builder.chunkSize;
		this.crc = builder.crc;
		this.firstBlockPos = builder.firstBlockPos;
		this.firstFreeBlockPos = builder.firstFreeBlockPos;
		this.tableRootPos = builder.tableRootPos;
		this.tableMetaInfoPos = builder.tableMetaInfoPos;
		this.tableMetaInfo = builder.tableMetaInfo;
		this.recordCount = builder.recordCount;
	}
	
	//copy constructor
	public SingleTablespaceHeader(TablespaceHeader tsHeader) {
		this.magic = tsHeader.getMagic();
		this.chunkSize = tsHeader.getChunkSize();
		this.crc = tsHeader.getCrc();
		this.firstBlockPos = tsHeader.getFirstBlockPos();
		this.firstFreeBlockPos = tsHeader.getFirstFreeBlockPos();
		this.tableRootPos = tsHeader.getTableRootPos();
		this.tableMetaInfoPos = tsHeader.getTableMetaInfoPos();
		this.tableMetaInfo = tsHeader.getTableMetaInfo();
		this.recordCount = tsHeader.getRecordCount();
	}

	@Override
	public byte[] getMagic() {
		return magic;
	}

	@Override
	public void setMagic(byte[] magic) {
		this.magic = magic;
	}

	@Override
	public int getChunkSize() {
		return chunkSize;
	}

	@Override
	public void setChunkSize(int chunkSize) {
		this.chunkSize = chunkSize;
	}

	@Override
	public int getCrc() {
		return crc;
	}

	@Override
	public void setCrc(int crc) {
		this.crc = crc;
	}

	@Override
	public long getFirstBlockPos() {
		return firstBlockPos;
	}

	@Override
	public void setFirstBlockPos(long firstBlockPos) {
		this.firstBlockPos = firstBlockPos;
	}

	@Override
	public long getFirstFreeBlockPos() {
		return firstFreeBlockPos;
	}

	@Override
	public void setFirstFreeBlockPos(long firstFreeBlockPos) {
		this.firstFreeBlockPos = firstFreeBlockPos;
	}

	@Override
	public long getTableRootPos() {
		return tableRootPos;
	}

	@Override
	public void setTableRootPos(long tableRootPos) {
		this.tableRootPos = tableRootPos;
	}

	@Override
	public long getTableMetaInfoPos() {
		return tableMetaInfoPos;
	}

	@Override
	public void setTableMetaInfoPos(long tableMetaInfoPos) {
		this.tableMetaInfoPos = tableMetaInfoPos;
	}
	
	@Override
	public TableMetaInfo getTableMetaInfo() {
		return this.tableMetaInfo;
	}
	
	@Override
	public void setTableMetaInfo(TableMetaInfo tableMetaInfo) {
		this.tableMetaInfo = tableMetaInfo;
	}

	@Override
	public int getRecordCount() {
		return recordCount;
	}

	@Override
	public void setRecordCount(int recordCount) {
		this.recordCount = recordCount;
	}
}
