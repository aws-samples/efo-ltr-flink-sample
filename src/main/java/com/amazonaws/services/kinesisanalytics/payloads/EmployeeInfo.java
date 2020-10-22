package com.amazonaws.services.kinesisanalytics.payloads;

public class EmployeeInfo {
    private long companyid;
    private long employeeid;
    private String name;
    private String dob;
    private long eventtimestamp;

    public long getCompanyid() {
        return companyid;
    }

    public void setCompanyid(long companyid) {
        this.companyid = companyid;
    }

    public long getEmployeeid() {
        return employeeid;
    }

    public void setEmployeeid(long employeeid) {
        this.employeeid = employeeid;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDob() {
        return dob;
    }

    public void setDob(String dob) {
        this.dob = dob;
    }

    public long getEventtimestamp() {
        return eventtimestamp;
    }

    public void setEventtimestamp(long eventtimestamp) {
        this.eventtimestamp = eventtimestamp;
    }
}