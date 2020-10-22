package com.amazonaws.services.kinesisanalytics.payloads;

public class ConfigSettings {
    private int sampleCount;
    private int threshold;
    private long tokenIssueTimestamp;

    public int getSampleCount() {
        return sampleCount;
    }

    public void setSampleCount(int sampleCount) {
        this.sampleCount = sampleCount;
    }

    public int getThreshold() {
        return threshold;
    }

    public void setThreshold(int threshold) {
        this.threshold = threshold;
    }

    public long getTokenIssueTimestamp() {
        return tokenIssueTimestamp;
    }

    public void setTokenIssueTimestamp(long tokenIssueTimestamp) {
        this.tokenIssueTimestamp = tokenIssueTimestamp;
    }
}