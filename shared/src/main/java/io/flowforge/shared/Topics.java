package io.flowforge.shared;

public final class Topics {
    private Topics() {}
    public static final String READY = "flowforge.tasks.ready";
    public static final String RESULTS = "flowforge.tasks.results";
    public static final String RETRY = "flowforge.tasks.retry";
    public static final String DEAD_LETTER = "flowforge.tasks.dead-letter";
}
