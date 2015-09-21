package com.anishek;

class Result {

    public final long elapsedMillis;
    public final long passed;
    public final long failed;
    public final String threadName;
    public final long timeout;
    public final long otherException;

    public Result(long passed, long failed, long timeout, long otherException, long elapsedMillis) {
        this.timeout = timeout;
        this.otherException = otherException;
        threadName = Thread.currentThread().getName();
        this.elapsedMillis = elapsedMillis;
        this.passed = passed;
        this.failed = failed;
    }

    @Override
    public String toString() {
        return "thread Name: " + threadName + "\n"
                + "passed: " + passed + "\n"
                + "failed: " + failed + "\n"
                + "timeout: " + timeout + "\n"
                + "other Exception: " + otherException + "\n";
    }
}
