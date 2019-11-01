package onthego.database.core.table;

public interface Filtration {
	
	boolean filter(Cursor[] cursor);
	
	void update(Cursor cursor);

	public static class DefaultFilter implements Filtration {
		@Override
		public boolean filter(Cursor[] cursor) {
			return true;
		}

		@Override
		public void update(Cursor cursor) {
			throw new UnsupportedOperationException();
		}
	}
	
	public static final Filtration ALL = new DefaultFilter();
}


