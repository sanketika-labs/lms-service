package org.sunbird.models.activity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import java.io.Serializable;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ActivityUserEnrolment implements Serializable {

    private static final long serialVersionUID = 1L;

    private String userid;
    private String activityid;
    private String activitytype;
    private String batchid;
    private boolean active;
    private String addedby;
    private Timestamp completedon;
    private Map<String, Integer> statusmap;
    private Timestamp datetime;
    private Timestamp enrolled_date;
    private List<Map<String, String>> issued_certificates;
    private int progress;
    private int status;

    public String getUserid() {
        return userid;
    }

    public void setUserid(String userid) {
        this.userid = userid;
    }

    public String getActivityid() {
        return activityid;
    }

    public void setActivityid(String activityid) {
        this.activityid = activityid;
    }

    public String getActivitytype() {
        return activitytype;
    }

    public void setActivitytype(String activitytype) {
        this.activitytype = activitytype;
    }

    public String getBatchid() {
        return batchid;
    }

    public void setBatchid(String batchid) {
        this.batchid = batchid;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public String getAddedby() {
        return addedby;
    }

    public void setAddedby(String addedby) {
        this.addedby = addedby;
    }

    public Timestamp getCompletedon() {
        return completedon;
    }

    public void setCompletedon(Timestamp completedon) {
        this.completedon = completedon;
    }

    public Map<String, Integer> getStatusmap() {
        return statusmap;
    }

    public void setStatusmap(Map<String, Integer> statusmap) {
        this.statusmap = statusmap;
    }

    public Timestamp getDatetime() {
        return datetime;
    }

    public void setDatetime(Timestamp datetime) {
        this.datetime = datetime;
    }

    public Timestamp getEnrolled_date() {
        return enrolled_date;
    }

    public void setEnrolled_date(Timestamp enrolled_date) {
        this.enrolled_date = enrolled_date;
    }

    public List<Map<String, String>> getIssued_certificates() {
        return issued_certificates;
    }

    public void setIssued_certificates(List<Map<String, String>> issued_certificates) {
        this.issued_certificates = issued_certificates;
    }

    public int getProgress() {
        return progress;
    }

    public void setProgress(int progress) {
        this.progress = progress;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }
}
