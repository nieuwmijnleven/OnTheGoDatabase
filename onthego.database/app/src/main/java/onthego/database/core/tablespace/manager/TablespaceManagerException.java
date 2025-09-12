package onthego.database.core.tablespace.manager;

public class TablespaceManagerException extends RuntimeException {

	public TablespaceManagerException() {}

	public TablespaceManagerException(String message) {
		super(message);
	}

	public TablespaceManagerException(Throwable cause) {
		super(cause);
	}

	public TablespaceManagerException(String message, Throwable cause) {
		super(message, cause);
	}

	public TablespaceManagerException(String message, Throwable cause, boolean enableSuppression,
			boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

}
