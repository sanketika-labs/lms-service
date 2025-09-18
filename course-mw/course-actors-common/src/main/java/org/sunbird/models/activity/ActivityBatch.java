package org.sunbird.models.activity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import java.io.Serializable;
import java.util.Date;
import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ActivityBatch implements Serializable {

    private static final long serialVersionUID = 1L;
    private String batchId;
    private String activityId;
    private String activityType;
    private Map<String, Object> certTemplates;
    private Date createdDate;
    private String createdBy;
    private List<String> createdFor;
    private String description;
    private Date endDate;
    private Date enrollmentEndDate;
    private String enrollmentType;
    private String name;
    private Date startDate;
    private Integer status;
    private Date updatedDate;

    // Getters and Setters
    public String getBatchId() { return batchId; }
    public void setBatchId(String batchId) { this.batchId = batchId; }

    public String getActivityId() { return activityId; }
    public void setActivityId(String activityId) { this.activityId = activityId; }

    public String getActivityType() { return activityType; }
    public void setActivityType(String activityType) { this.activityType = activityType; }

    public Map<String, Object> getCertTemplates() { return certTemplates; }
    public void setCertTemplates(Map<String, Object> certTemplates) { this.certTemplates = certTemplates; }

    public Date getCreatedDate() { return createdDate; }
    public void setCreatedDate(Date createdDate) { this.createdDate = createdDate; }

    public String getCreatedBy() { return createdBy; }
    public void setCreatedBy(String createdBy) { this.createdBy = createdBy; }

    public List<String> getCreatedFor() { return createdFor; }
    public void setCreatedFor(List<String> createdFor) { this.createdFor = createdFor; }

    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }

    public Date getEndDate() { return endDate; }
    public void setEndDate(Date endDate) { this.endDate = endDate; }

    public Date getEnrollmentEndDate() { return enrollmentEndDate; }
    public void setEnrollmentEndDate(Date enrollmentEndDate) { this.enrollmentEndDate = enrollmentEndDate; }

    public String getEnrollmentType() { return enrollmentType; }
    public void setEnrollmentType(String enrollmentType) { this.enrollmentType = enrollmentType; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public Date getStartDate() { return startDate; }
    public void setStartDate(Date startDate) { this.startDate = startDate; }

    public int getStatus() { return status; }
    public void setStatus(int status) { this.status = status; }

    public Date getUpdatedDate() { return updatedDate; }
    public void setUpdatedDate(Date updatedDate) { this.updatedDate = updatedDate; }
}

