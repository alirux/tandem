package com.codingful.tandem.core.exception;

/** A {@code PayloadSerializer} failed to turn an object into bytes (LLD-core §2.4, §3). */
public class PayloadSerializationException extends TandemException {

    private static final long serialVersionUID = 7313339292310773083L;

    public PayloadSerializationException(String message) {
        super(message);
    }

    public PayloadSerializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
