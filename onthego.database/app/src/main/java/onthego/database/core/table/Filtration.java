package onthego.database.core.table;

import onthego.database.core.database.DatabaseException;

public interface Filtration {
	
	boolean filter(Cursor[] cursor) throws DatabaseException;
	
	void update(Cursor cursor) throws DatabaseException;

	class DefaultFilter implements Filtration {
		@Override
		public boolean filter(Cursor[] cursor) throws DatabaseException {
			return true;
		}

		@Override
		public void update(Cursor cursor) throws DatabaseException {
			throw new UnsupportedOperationException();
		}
	}
	
	Filtration ALL = new DefaultFilter();
}


