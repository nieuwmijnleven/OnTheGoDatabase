package onthego.database.core.tablespace.meta;

import java.util.List;

import onthego.database.core.table.meta.Column;

public class TableMetaInfo {
	
	private String tableName;
	
	private List<Column> columnList;
	
	public TableMetaInfo(String tableName, List<Column> columnList) {
		this.tableName = tableName;
		this.columnList = columnList;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public List<Column> getColumnList() {
		return columnList;
	}

	public void setColumnList(List<Column> columnList) {
		this.columnList = columnList;
	}
}
