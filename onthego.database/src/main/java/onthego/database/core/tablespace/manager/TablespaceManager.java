package onthego.database.core.tablespace.manager;

import java.io.IOException;

import onthego.database.core.tablespace.meta.TablespaceHeader;

public interface TablespaceManager {
	
	/*enum ManagerType {
		SingleTablespaceManager
	}
	
	static TablespaceManager of(ManagerType type, String tsPath, TablespaceHeader tsHeader) throws IOException {
		if (type == ManagerType.SingleTablespaceManager) {
			return SingleTablespaceManager.create(tsPath, tsHeader);
		}	
		
		throw new IllegalArgumentException("Unsupported TablespaceManager type");
	}*/

	void loadHeader();

	void saveHeader();
	
	TablespaceHeader getHeader();

	long getRootPos();

	void saveRootPos(long rootPos);

	long getRecordCount();

	void increaseRecordCount();

	void decreaseRecordCount();

	long allocate(int size);

	void free(long blockPos);
	
	public byte[] readBlock(long blockPos) throws IOException;
	
	public void writeBlock(long blockPos, byte[] payload) throws IOException;
	
	void close();

	void printFreeListBlock();

}