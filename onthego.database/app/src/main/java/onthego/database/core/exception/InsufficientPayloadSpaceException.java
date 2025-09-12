package onthego.database.core.exception;

public class InsufficientPayloadSpaceException extends RuntimeException {

	public InsufficientPayloadSpaceException() {}

	public InsufficientPayloadSpaceException(String message) {
		super(message);
	}

	public InsufficientPayloadSpaceException(Throwable cause) {
		super(cause);
	}

	public InsufficientPayloadSpaceException(String message, Throwable cause) {
		super(message, cause);
	}

	public InsufficientPayloadSpaceException(String message, Throwable cause, boolean enableSuppression,
			boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

}
