package com.google.cloud.sql.core;

import com.google.api.client.auth.oauth2.BearerToken;
import com.google.api.client.auth.oauth2.Credential;
import com.google.cloud.sql.AuthType;
import com.google.cloud.sql.CredentialFactory;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.testing.TestingExecutors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.security.KeyPair;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class CloudSqlInstanceTest {
    private final String connectionName = "project:region:instance";

    @Mock
    private InstanceDataSupplier instanceDataSupplier;

    @Mock
    private CredentialFactory tokenSourceFactory;

    private ListeningScheduledExecutorService executor = TestingExecutors.sameThreadScheduledExecutor();

    @Mock
    private ListenableFuture<KeyPair> keyPair;

    @Mock
    private RateLimiter rateLimiter;

    @BeforeEach
    public void setup() {
        when(tokenSourceFactory.create()).thenReturn(
            new Credential.Builder(BearerToken.authorizationHeaderAccessMethod()).build()
        );
        when(rateLimiter.acquire()).thenReturn(0.0);
    }

    @Test
    public void shouldBeAbleToFetchData() throws Exception {
        SslData sslData = createSslData();
        when(instanceDataSupplier.getInstanceData(any(), any(), eq(AuthType.IAM), any(), any())).thenReturn(
            Futures.immediateFuture(validInstanceData(sslData))
        );

        CloudSqlInstance inst = new CloudSqlInstance(connectionName, instanceDataSupplier, AuthType.IAM, tokenSourceFactory, executor, keyPair, rateLimiter);
        SslData result = inst.getSslData(0);
        assertEquals(sslData, result);
        verify(instanceDataSupplier, times(1)).getInstanceData(any(), any(), eq(AuthType.IAM), any(), any());
    }

    @Test
    public void failOnStartupShouldRetryOnce() throws Exception {
        SslData sslData = createSslData();
        when(instanceDataSupplier.getInstanceData(any(), any(), eq(AuthType.IAM), any(), any()))
            .thenThrow(new RuntimeException("knas"))
            .thenReturn(Futures.immediateFuture(validInstanceData(sslData)));

        CloudSqlInstance inst = new CloudSqlInstance(connectionName, instanceDataSupplier, AuthType.IAM, tokenSourceFactory, executor, keyPair, rateLimiter);
        SslData result = inst.getSslData(0);
        assertEquals(sslData, result);
        verify(instanceDataSupplier, times(2)).getInstanceData(any(), any(), eq(AuthType.IAM), any(), any());
    }

    @Test
    public void failOnRefreshShouldRetryOnce() throws Exception {
        SslData sslData = createSslData();
        when(instanceDataSupplier.getInstanceData(any(), any(), eq(AuthType.IAM), any(), any()))
            .thenReturn(Futures.immediateFuture(expiredInstanceData()))
            .thenThrow(new RuntimeException("knas"))
            .thenThrow(new RuntimeException("knas"))
            .thenReturn(Futures.immediateFuture(validInstanceData(sslData)));

        CloudSqlInstance inst = new CloudSqlInstance(connectionName, instanceDataSupplier, AuthType.IAM, tokenSourceFactory, executor, keyPair, rateLimiter);
        assertThrows(RuntimeException.class, () -> inst.getSslData(0));
        SslData result = inst.getSslData(0);
        assertEquals(sslData, result);
        verify(instanceDataSupplier, times(4)).getInstanceData(any(), any(), eq(AuthType.IAM), any(), any());
    }

    @Test
    public void failTwiceOnStartupShouldRetryOnNextRefresh() throws Exception {
        SslData sslData = createSslData();
        when(instanceDataSupplier.getInstanceData(any(), any(), eq(AuthType.IAM), any(), any()))
            .thenThrow(new RuntimeException("knas"))
            .thenThrow(new RuntimeException("knas"))
            .thenReturn(Futures.immediateFuture(validInstanceData(sslData)));

        CloudSqlInstance inst = new CloudSqlInstance(connectionName, instanceDataSupplier, AuthType.IAM, tokenSourceFactory, executor, keyPair, rateLimiter);
        SslData result = inst.getSslData(0);
        assertEquals(sslData, result);
        verify(instanceDataSupplier, times(3)).getInstanceData(any(), any(), eq(AuthType.IAM), any(), any());
    }

    @Test
    public void failTwiceOnRefreshShouldRetryOnNextRefresh() throws Exception {
        SslData sslData = createSslData();
        when(instanceDataSupplier.getInstanceData(any(), any(), eq(AuthType.IAM), any(), any()))
            .thenReturn(Futures.immediateFuture(expiredInstanceData()))
            .thenThrow(new RuntimeException("knas"))
            .thenThrow(new RuntimeException("knas"))
            .thenReturn(Futures.immediateFuture(validInstanceData(sslData)));

        CloudSqlInstance inst = new CloudSqlInstance(connectionName, instanceDataSupplier, AuthType.IAM, tokenSourceFactory, executor, keyPair, rateLimiter);
        assertThrows(RuntimeException.class, () -> inst.getSslData(0));
        SslData result = inst.getSslData(0);
        assertEquals(sslData, result);
        verify(instanceDataSupplier, times(4)).getInstanceData(any(), any(), eq(AuthType.IAM), any(), any());
    }

    static class ResolvedFuture extends AbstractFuture<InstanceData> {
        public ResolvedFuture(InstanceData data) {
            set(data);
        }
    }

    static class RejectedFuture extends AbstractFuture<InstanceData> {
        public RejectedFuture(Throwable t) {
            setException(t);
        }
    }

    SslData createSslData() {
        return new SslData(null, null, null);
    }

    InstanceData validInstanceData(SslData sslData) {
        return new InstanceData(null, sslData, Instant.now().plus(Duration.ofHours(1)));
    }

    InstanceData expiredInstanceData() {
        return new InstanceData(null, createSslData(), Instant.now().minus(Duration.ofSeconds(1)));
    }
}
