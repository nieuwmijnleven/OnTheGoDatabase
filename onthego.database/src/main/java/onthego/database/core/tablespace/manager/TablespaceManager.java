package onthego.database.core.tablespace.manager;

import java.io.IOException;

import onthego.database.core.exception.MarginalPayloadSpaceException;
import onthego.database.core.tablespace.meta.TableMetaInfo;
import onthego.database.core.tablespace.meta.TablespaceHeader;

public interface TablespaceManager {
	
	void createTableInfoEntry(TableMetaInfo tableMetaInfo);
	
	void loadTableInfoEntry();

	void loadHeader();

	void saveHeader();
	
	TablespaceHeader getHeader();

	long getRootPos();

	void saveRootPos(long rootPos);

	int getRecordCount();

	int increaseRecordCount();

	void decreaseRecordCount();

	long allocate(int size);

	void free(long blockPos);
	
	public byte[] readBlock(long blockPos);
	
	public void writeBlock(long blockPos, byte[] payload) throws MarginalPayloadSpaceException;
	
	void close();

	void printFreeListBlock();

}