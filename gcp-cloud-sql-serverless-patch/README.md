# Google Cloud SQL Connector patch

This is a patch for CloudSqlInstance.java that fixes a problem in
`com.google.cloud.sql:jdbc-socket-factory-core:1.7.2` when running in a serverless environment.

Original file copied from https://github.com/GoogleCloudPlatform/cloud-sql-jdbc-socket-factory/blob/v1.7.2/core/src/main/java/com/google/cloud/sql/core/CloudSqlInstance.java

## Background

The `CloudSqlInstance` class is used in the Cloud SQL connector. It is responsible for fetching and
refreshing certificates that are used in the communication with the Cloud SQL instance.

This class is creating a background job that periodically fetches a new certificate, when the current
certificate is about to expire.

This class is used both in the `jdbc` and the `r2dbc` connectors.

## Problem

When running serverless containers such as Cloud Functions or Cloud Run with CPU throttling, you are
not allowed to keep background processes.

This causes the background process to fail to refresh the certificate now and then.

```
java.lang.RuntimeException: Failed to update metadata for Cloud SQL instance.
	at com.google.cloud.sql.core.CloudSqlInstance.addExceptionContext(CloudSqlInstance.java:598)
	at com.google.cloud.sql.core.CloudSqlInstance.fetchMetadata(CloudSqlInstance.java:505)
	at com.google.common.util.concurrent.TrustedListenableFutureTask$TrustedFutureInterruptibleTask.runInterruptibly(TrustedListenableFutureTask.java:131)
	at com.google.common.util.concurrent.InterruptibleTask.run(InterruptibleTask.java:74)
	at com.google.common.util.concurrent.TrustedListenableFutureTask.run(TrustedListenableFutureTask.java:82)
	at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:515)
	at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:264)
	at java.base/java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.run(ScheduledThreadPoolExecutor.java:304)
	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
	at java.base/java.lang.Thread.run(Thread.java:829)
Caused by: java.net.SocketException: Unexpected end of file from server
	at java.base/sun.net.www.http.HttpClient.parseHTTPHeader(HttpClient.java:866)
```

## Solution

This patch changes the `CloudSqlInstance` class to refresh the certificate when it is needed instead of
as a background job.

## Usage

To use the patch, place this jar file on the class path, before the `jdbc-socket-factory-core` jar file.

This patch is based on version `1.6.3` of `jdbc-socket-factory-core`, and it might not work with any other
version.
