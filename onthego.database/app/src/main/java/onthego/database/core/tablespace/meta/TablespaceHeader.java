package onthego.database.core.tablespace.meta;

public interface TablespaceHeader {

	byte[] getMagic();

	void setMagic(byte[] magic);

	int getChunkSize();

	void setChunkSize(int chunkSize);

	int getCrc();

	void setCrc(int crc);

	long getFirstBlockPos();

	void setFirstBlockPos(long firstBlockPos);

	long getFirstFreeBlockPos();

	void setFirstFreeBlockPos(long firstFreeBlockPos);

	long getTableRootPos();

	void setTableRootPos(long tableRootPos);

	long getTableMetaInfoPos();

	void setTableMetaInfoPos(long tableMetaInfoPos);
	
	TableMetaInfo getTableMetaInfo();
	
	void setTableMetaInfo(TableMetaInfo tableMetaInfo);

	int getRecordCount();

	void setRecordCount(int recordCount);

}