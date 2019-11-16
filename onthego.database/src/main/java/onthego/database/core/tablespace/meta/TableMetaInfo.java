package onthego.database.core.tablespace.meta;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import onthego.database.core.table.meta.ColumnMeta;

public class TableMetaInfo {
	
	private String tableName;
	
	private List<ColumnMeta> columnList;
	
	private Map<String,Integer> columnIndexMap;
	
	public TableMetaInfo(String tableName, List<ColumnMeta> columnList) {
		this.tableName = tableName;
		this.columnList = columnList;
		this.columnIndexMap = new HashMap<>();
		initializeColumnIndexMap();
	}
	
	//copy constructor
	public TableMetaInfo(TableMetaInfo tableMetaInfo) {
		this.tableName = tableMetaInfo.tableName;
		this.columnList = new ArrayList<>(tableMetaInfo.columnList);
		this.columnIndexMap = tableMetaInfo.columnIndexMap;
	}
	
	private void initializeColumnIndexMap() {
		for (int i = 0; i < columnList.size(); ++i) {
			columnIndexMap.put(columnList.get(i).getName(), i);
		}
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public List<ColumnMeta> getColumnList() {
		return Collections.unmodifiableList(this.columnList);
	}

	public void setColumnList(List<ColumnMeta> columnList) {
		this.columnList = new ArrayList<>(columnList);
	}
	
	public void addColumn(ColumnMeta column) {
		this.columnList.add(column);
		this.columnIndexMap.put(column.getName(), columnList.size());
	}
	
	public int getColumnCount() {
		return this.columnList.size();
	}
	
	public int getColumnIndex(String name) {
		return columnIndexMap.getOrDefault(name, -1);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((columnList == null) ? 0 : columnList.hashCode());
		result = prime * result + ((tableName == null) ? 0 : tableName.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		
		TableMetaInfo other = (TableMetaInfo) obj;
		if (columnList == null) {
			if (other.columnList != null)
				return false;
		} else if (!columnList.equals(other.columnList))
			return false;
		if (tableName == null) {
			if (other.tableName != null)
				return false;
		} else if (!tableName.equals(other.tableName))
			return false;
		return true;
	}
	
//	@Override
//	public boolean equals(Object obj) {
//		if (!(obj instanceof TableMetaInfo)) {
//			return false;
//		}
//		
//		TableMetaInfo rhs = (TableMetaInfo)obj;
//		return this.tableName.equals(rhs.tableName) 
//			&& this.columnList.equals(rhs.columnList);
//	}
}
