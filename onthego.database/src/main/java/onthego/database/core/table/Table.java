package onthego.database.core.table;

import java.util.List;
import java.util.Map;

import onthego.database.core.table.meta.ColumnType;
import onthego.database.core.tablespace.manager.TablespaceManager;

public interface Table {

	void begin();

	void rollback(boolean all);

	void commit(boolean all);
	
	Table select(Filtration filtration);

	long insert(Map<ColumnType, String> values);

	Cursor getCursor();

	int update(Filtration filtration);

	int delete(Filtration filtration);

	String getTableName();

	TablespaceManager getTablespaceManager();

	List<ColumnType> getColumnList();

	int getColumnCount();

	void close();
}