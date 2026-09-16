package io.flowforge.worker;

public final class TaskExecutionException extends RuntimeException {
    private final boolean retryable;

    public TaskExecutionException(String message, boolean retryable) {
        super(message);
        this.retryable = retryable;
    }

    public boolean retryable() {
        return retryable;
    }
}
