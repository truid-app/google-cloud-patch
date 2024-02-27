/*
 * Copyright 2016 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.sql.core;

import com.google.cloud.sql.AuthType;
import com.google.cloud.sql.CredentialFactory;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.RateLimiter;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import java.io.IOException;
import java.security.KeyPair;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.net.ssl.SSLSocket;

/**
 * This class manages information on and creates connections to a Cloud SQL instance using the Cloud
 * SQL Admin API. The operations to retrieve information with the API are largely done
 * asynchronously, and this class should be considered threadsafe.
 */
class CloudSqlInstance {

  private static final Logger logger = Logger.getLogger(CloudSqlInstance.class.getName());

  private final ListeningScheduledExecutorService executor;
  private final InstanceDataSupplier instanceDataSupplier;
  private final AuthType authType;
  private final AccessTokenSupplier accessTokenSupplier;
  private final CloudSqlInstanceName instanceName;
  private final ListenableFuture<KeyPair> keyPair;
  private final Object instanceDataGuard = new Object();

  private static final Duration DEFAULT_REFRESH_BUFFER = Duration.ofMinutes(4);

  @SuppressWarnings("UnstableApiUsage")
  private final RateLimiter forcedRenewRateLimiter;

  @GuardedBy("instanceDataGuard")
  private ListenableFuture<InstanceData> currentInstanceData;

  @GuardedBy("instanceDataGuard")
  private Throwable currentRefreshFailure;

  /**
   * Initializes a new Cloud SQL instance based on the given connection name.
   *
   * @param connectionName instance connection name in the format "PROJECT_ID:REGION_ID:INSTANCE_ID"
   * @param instanceDataSupplier Service class for interacting with the Cloud SQL Admin API
   * @param executor executor used to schedule asynchronous tasks
   * @param keyPair public/private key pair used to authenticate connections
   */
  CloudSqlInstance(
      String connectionName,
      InstanceDataSupplier instanceDataSupplier,
      AuthType authType,
      CredentialFactory tokenSourceFactory,
      ListeningScheduledExecutorService executor,
      ListenableFuture<KeyPair> keyPair,
      @SuppressWarnings("UnstableApiUsage") RateLimiter forcedRenewRateLimiter) {
    this.instanceName = new CloudSqlInstanceName(connectionName);
    this.instanceDataSupplier = instanceDataSupplier;
    this.authType = authType;
    this.executor = executor;
    this.keyPair = keyPair;
    this.forcedRenewRateLimiter = forcedRenewRateLimiter;

    if (authType == AuthType.IAM) {
      this.accessTokenSupplier = new DefaultAccessTokenSupplier(tokenSourceFactory);
    } else {
      this.accessTokenSupplier = Optional::empty;
    }

    synchronized (instanceDataGuard) {
      this.currentInstanceData = this.startRefreshAttempt();
    }
  }

  /**
   * Returns the current data related to the instance from {@link #startRefreshAttempt()}. May block
   * if no valid data is currently available. This method is called by an application thread when it
   * is trying to create a new connection to the database. (It is not called by a
   * ListeningScheduledExecutorService task.) So it is OK to block waiting for a future to complete.
   *
   * <p>When no refresh attempt is in progress, this returns immediately. Otherwise, it waits up to
   * timeoutMs milliseconds. If a refresh attempt succeeds, returns immediately at the end of that
   * successful attempt. If no attempts succeed within the timeout, throws a RuntimeException with
   * the exception from the last failed refresh attempt as the cause.
   */
  private InstanceData getInstanceData(long timeoutMs) {
    ListenableFuture<InstanceData> instanceDataFuture;
    synchronized (instanceDataGuard) {
      instanceDataFuture = currentInstanceData;
    }

    try {
      return instanceDataFuture.get(timeoutMs, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      synchronized (instanceDataGuard) {
        if (currentRefreshFailure != null) {
          throw new RuntimeException(
              String.format(
                      "Unable to get valid instance data within %d ms."
                          + " Last refresh attempt failed:",
                      timeoutMs)
                  + currentRefreshFailure.getMessage(),
              currentRefreshFailure);
        }
      }
      throw new RuntimeException(
          String.format(
              "Unable to get valid instance data within %d ms. No refresh has completed.",
              timeoutMs),
          e);
    } catch (ExecutionException | InterruptedException ex) {
      Throwable cause = ex.getCause();
      Throwables.throwIfUnchecked(cause);
      throw new RuntimeException(cause);
    }
  }

  /**
   * Returns an unconnected {@link SSLSocket} using the SSLContext associated with the instance. May
   * block until required instance data is available.
   */
  SSLSocket createSslSocket(long timeoutMs) throws IOException {
    refreshIfExpired();
    return (SSLSocket) getInstanceData(timeoutMs).getSslContext().getSocketFactory().createSocket();
  }

  /**
   * Returns the first IP address for the instance, in order of the preference supplied by
   * preferredTypes.
   *
   * @param preferredTypes Preferred instance IP types to use. Valid IP types include "Public" and
   *     "Private".
   * @return returns a string representing the IP address for the instance
   * @throws IllegalArgumentException If the instance has no IP addresses matching the provided
   *     preferences.
   */
  String getPreferredIp(List<String> preferredTypes, long timeoutMs) {
    Map<String, String> ipAddrs = getInstanceData(timeoutMs).getIpAddrs();
    for (String ipType : preferredTypes) {
      String preferredIp = ipAddrs.get(ipType);
      if (preferredIp != null) {
        return preferredIp;
      }
    }
    throw new IllegalArgumentException(
        String.format(
            "[%s] Cloud SQL instance  does not have any IP addresses matching preferences (%s)",
            instanceName.getConnectionName(), String.join(", ", preferredTypes)));
  }

  /**
   * Attempts to force a new refresh of the instance data if the current data is close to expiry.
   * May fail if called too frequently or if new refresh is already in progress. If successful,
   * other methods will block until refresh has been completed.
   */
  private void refreshIfExpired() {
    synchronized (instanceDataGuard) {
      if (currentInstanceData.isDone()) {
        try {
          InstanceData instanceData = null;
          try {
            instanceData = currentInstanceData.get();
          } catch (Exception e) {
            // needs refresh
          }
          if (instanceData == null || needRefresh(instanceData)) {
            currentInstanceData = startRefreshAttempt();
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  private boolean needRefresh(InstanceData instanceData) {
    Instant expiration = instanceData.getExpiration();
    Duration timeUntilRefresh = Duration.between(Instant.now(), expiration)
        .minus(DEFAULT_REFRESH_BUFFER);

    return timeUntilRefresh.isNegative();
  }

  /**
   * Attempts to force a new refresh of the instance data. May fail if called too frequently or if a
   * new refresh is already in progress. If successful, other methods will block until refresh has
   * been completed.
   */
  void forceRefresh() {
    synchronized (instanceDataGuard) {
      // Don't force a refresh until the current refresh operation
      // has produced a successful refresh.
      if (currentInstanceData.isDone()) {
        currentInstanceData = startRefreshAttempt();
      }
    }
  }

  /**
   * Triggers an update of internal information obtained from the Cloud SQL Admin API, returning a
   * future that resolves once a valid InstanceData has been acquired. This sets up a chain of
   * futures that will 1. Acquire a rate limiter. 2. Attempt to fetch instance data. 3. Schedule the
   * next attempt to get instance data based on the success/failure of this attempt.
   */
  private ListenableFuture<InstanceData> startRefreshAttempt() {
    // To avoid unreasonable SQL Admin API usage, use a rate limit to throttle our usage.
    ListenableFuture<?> rateLimit =
        executor.submit(
            () -> {
              logger.fine(
                  String.format(
                      "[%s] Refresh Operation: Acquiring rate limiter permit.", instanceName));
              //noinspection UnstableApiUsage
              forcedRenewRateLimiter.acquire();
              logger.fine(
                  String.format(
                      "[%s] Refresh Operation: Acquired rate limiter permit. Starting refresh...",
                      instanceName));
            },
            executor);

    // Once rate limiter is done, attempt to getInstanceData.
    ListenableFuture<InstanceData> dataFuture =
        Futures.whenAllComplete(rateLimit)
            .callAsync(
                () ->
                    instanceDataSupplier.getInstanceData(
                        this.instanceName,
                        this.accessTokenSupplier,
                        this.authType,
                        executor,
                        keyPair),
                executor);

    // Finally, reschedule refresh after getInstanceData is complete.
    return Futures.whenAllComplete(dataFuture)
        .callAsync(() -> {
          try {
            // This does not block, because it only gets called when dataFuture has completed.
            // This will throw an exception if the refresh attempt has failed.
            InstanceData data = dataFuture.get();

            logger.fine(
                String.format(
                    "[%s] Refresh Operation: Completed refresh with new certificate expiration at %s.",
                    instanceName, data.getExpiration().toString()));

            return dataFuture;
          } catch (ExecutionException | InterruptedException e) {
            logger.log(
                Level.FINE,
                String.format(
                    "[%s] Refresh Operation: Failed! Starting next refresh operation immediately.",
                    instanceName),
                e);

            // Once rate limiter is done, attempt to getInstanceData.
            ListenableFuture<InstanceData> dataFuture2 =
                Futures.whenAllComplete(rateLimit)
                    .callAsync(
                        () ->
                            instanceDataSupplier.getInstanceData(
                                this.instanceName,
                                this.accessTokenSupplier,
                                this.authType,
                                executor,
                                keyPair),
                        executor);

            return Futures.whenAllComplete(dataFuture2)
                .callAsync(() -> {
                      try {
                        // This does not block, because it only gets called when dataFuture has completed.
                        // This will throw an exception if the refresh attempt has failed.
                        InstanceData data = dataFuture2.get();

                        logger.fine(
                            String.format(
                                "[%s] Refresh Operation: Completed refresh with new certificate expiration at %s.",
                                instanceName, data.getExpiration().toString()));

                        return dataFuture2;
                      } catch (ExecutionException | InterruptedException e2) {
                        logger.log(
                            Level.FINE,
                            String.format(
                                "[%s] Refresh Operation: Failed!",
                                instanceName),
                            e2);
                        throw e2;
                      }
                    },
                    executor);
          }
        },
        executor);
  }

  SslData getSslData(long timeoutMs) {
    refreshIfExpired();
    return getInstanceData(timeoutMs).getSslData();
  }

  ListenableFuture<InstanceData> getNext() {
    synchronized (instanceDataGuard) {
      return this.currentInstanceData;
    }
  }

  ListenableFuture<InstanceData> getCurrent() {
    synchronized (instanceDataGuard) {
      return this.currentInstanceData;
    }
  }

  public CloudSqlInstanceName getInstanceName() {
    return instanceName;
  }
}
