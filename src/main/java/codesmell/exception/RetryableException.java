package codesmell.exception;

@SuppressWarnings("serial")
public class RetryableException extends RuntimeException {
    public RetryableException(String msg) {
        super(msg);
    }

    public RetryableException(String msg, Throwable cause) {
        super(msg, cause);
    }
}