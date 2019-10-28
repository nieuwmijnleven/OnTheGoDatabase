package onthego.database.core.index;

public class BTreeIndexException extends RuntimeException {

	public BTreeIndexException() {}

	public BTreeIndexException(String message) {
		super(message);
	}

	public BTreeIndexException(Throwable cause) {
		super(cause);
	}

	public BTreeIndexException(String message, Throwable cause) {
		super(message, cause);
	}

	public BTreeIndexException(String message, Throwable cause, boolean enableSuppression,
			boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

}
