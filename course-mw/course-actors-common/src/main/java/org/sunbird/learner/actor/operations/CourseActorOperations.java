package org.sunbird.learner.actor.operations;

public enum CourseActorOperations {
  ISSUE_CERTIFICATE("issueCertificate"),
  ADD_BATCH_CERTIFICATE("addCertificateToCourseBatch"),
  DELETE_BATCH_CERTIFICATE("removeCertificateFromCourseBatch"),
  ADD_ACTIVITY_BATCH_CERTIFICATE("addCertificateToActivityBatch");

  private String value;

  private CourseActorOperations(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }
}
