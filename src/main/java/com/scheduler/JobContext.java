package com.scheduler;

public final class JobContext {
    private volatile boolean lost;

    public void checkpoint() {
        if (lost) {
            throw new LeaseLostException();
        }
    }

    public boolean lost() {
        return lost;
    }

    void markLost() {
        lost = true;
    }

    public static final class LeaseLostException extends RuntimeException {
        public LeaseLostException() {
            super("lease lost");
        }
    }
}
