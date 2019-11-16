package onthego.database.core.table;

import java.util.List;
import java.util.Map;

import onthego.database.core.table.meta.ColumnMeta;
import onthego.database.core.tablespace.manager.TablespaceManager;

public interface Table {

	void begin();

	void rollback(boolean all);

	void commit(boolean all);
	
	Table select(List<ColumnMeta> selectColumn, Filtration filtration);
	
//	Table select(Filtration filtration);

	long insert(Map<ColumnMeta, String> values);

	Cursor getCursor();
	
	Cursor getCursor(List<ColumnMeta> selectColumn);

	int update(Filtration filtration);

	int delete(Filtration filtration);

	String getTableName();

	TablespaceManager getTablespaceManager();

	List<ColumnMeta> getColumnList();

	int getColumnCount();

	void close();
}