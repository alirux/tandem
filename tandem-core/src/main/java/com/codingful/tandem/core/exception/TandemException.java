package com.codingful.tandem.core.exception;

/**
 * Base type for every exception raised by Tandem. Unchecked (LLD-core §3): the write-side
 * insert already runs inside the caller's transaction, so a checked exception would only add
 * ceremony without giving the caller a meaningful recovery path beyond catching it.
 */
public class TandemException extends RuntimeException {

    private static final long serialVersionUID = 5559838824161988047L;

    public TandemException(String message) {
        super(message);
    }

    public TandemException(String message, Throwable cause) {
        super(message, cause);
    }
}
