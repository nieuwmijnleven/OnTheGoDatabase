package onthego.database.core.index;

public class BPlusTreeIndexException extends RuntimeException {

	public BPlusTreeIndexException() {}

	public BPlusTreeIndexException(String message) {
		super(message);
	}

	public BPlusTreeIndexException(Throwable cause) {
		super(cause);
	}

	public BPlusTreeIndexException(String message, Throwable cause) {
		super(message, cause);
	}

	public BPlusTreeIndexException(String message, Throwable cause, boolean enableSuppression,
                                   boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

}
