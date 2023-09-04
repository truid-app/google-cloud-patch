package com.google.cloud.sql.core;

import com.google.api.client.auth.oauth2.BearerToken;
import com.google.api.client.auth.oauth2.Credential;
import com.google.cloud.sql.AuthType;
import com.google.cloud.sql.CredentialFactory;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.security.KeyPair;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class CloudSqlInstanceTest {
    private final String connectionName = "project:region:instance";

    @Mock
    private SqlAdminApiFetcher apiFetcher;

    @Mock
    private CredentialFactory tokenSourceFactory;

    @Mock
    private ListeningScheduledExecutorService executor;

    @Mock
    private ListenableFuture<KeyPair> keyPair;

    @BeforeEach
    public void setup() {
        when(tokenSourceFactory.create()).thenReturn(
            new Credential.Builder(BearerToken.authorizationHeaderAccessMethod()).build()
        );
    }

    @Test
    public void shouldBeAbleToFetchData() throws Exception {
        SslData sslData = createSslData();
        when(apiFetcher.getInstanceData(any(), any(), eq(AuthType.IAM), any(), any())).thenReturn(
            new ResolvedFuture(validInstanceData(sslData))
        );

        CloudSqlInstance inst = new CloudSqlInstance(connectionName, apiFetcher, AuthType.IAM, tokenSourceFactory, executor, keyPair);
        SslData result = inst.getSslData();
        assertEquals(sslData, result);
        verify(apiFetcher, times(1)).getInstanceData(any(), any(), eq(AuthType.IAM), any(), any());
    }

    @Test
    public void failOnStartupShouldRetryOnce() throws Exception {
        SslData sslData = createSslData();
        when(apiFetcher.getInstanceData(any(), any(), eq(AuthType.IAM), any(), any()))
            .thenReturn(new RejectedFuture(new RuntimeException("knas")))
            .thenReturn(new ResolvedFuture(validInstanceData(sslData)));

        CloudSqlInstance inst = new CloudSqlInstance(connectionName, apiFetcher, AuthType.IAM, tokenSourceFactory, executor, keyPair);
        SslData result = inst.getSslData();
        assertEquals(sslData, result);
        verify(apiFetcher, times(2)).getInstanceData(any(), any(), eq(AuthType.IAM), any(), any());
    }

    @Test
    public void failOnRefreshShouldRetryOnce() throws Exception {
        SslData sslData = createSslData();
        when(apiFetcher.getInstanceData(any(), any(), eq(AuthType.IAM), any(), any()))
            .thenReturn(new ResolvedFuture(expiredInstanceData()))
            .thenReturn(new RejectedFuture(new RuntimeException("knas")))
            .thenReturn(new RejectedFuture(new RuntimeException("knas")))
            .thenReturn(new ResolvedFuture(validInstanceData(sslData)));

        CloudSqlInstance inst = new CloudSqlInstance(connectionName, apiFetcher, AuthType.IAM, tokenSourceFactory, executor, keyPair);
        assertThrows(RuntimeException.class, inst::getSslData);
        SslData result = inst.getSslData();
        assertEquals(sslData, result);
        verify(apiFetcher, times(4)).getInstanceData(any(), any(), eq(AuthType.IAM), any(), any());
    }

    @Test
    public void failTwiceOnStartupShouldRetryOnNextRefresh() throws Exception {
        SslData sslData = createSslData();
        when(apiFetcher.getInstanceData(any(), any(), eq(AuthType.IAM), any(), any()))
            .thenReturn(new RejectedFuture(new RuntimeException("knas")))
            .thenReturn(new RejectedFuture(new RuntimeException("knas")))
            .thenReturn(new ResolvedFuture(validInstanceData(sslData)));

        CloudSqlInstance inst = new CloudSqlInstance(connectionName, apiFetcher, AuthType.IAM, tokenSourceFactory, executor, keyPair);
        SslData result = inst.getSslData();
        assertEquals(sslData, result);
        verify(apiFetcher, times(3)).getInstanceData(any(), any(), eq(AuthType.IAM), any(), any());
    }

    @Test
    public void failTwiceOnRefreshShouldRetryOnNextRefresh() throws Exception {
        SslData sslData = createSslData();
        when(apiFetcher.getInstanceData(any(), any(), eq(AuthType.IAM), any(), any()))
            .thenReturn(new ResolvedFuture(expiredInstanceData()))
            .thenReturn(new RejectedFuture(new RuntimeException("knas")))
            .thenReturn(new RejectedFuture(new RuntimeException("knas")))
            .thenReturn(new ResolvedFuture(validInstanceData(sslData)));

        CloudSqlInstance inst = new CloudSqlInstance(connectionName, apiFetcher, AuthType.IAM, tokenSourceFactory, executor, keyPair);
        assertThrows(RuntimeException.class, inst::getSslData);
        SslData result = inst.getSslData();
        assertEquals(sslData, result);
        verify(apiFetcher, times(4)).getInstanceData(any(), any(), eq(AuthType.IAM), any(), any());
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
        return new InstanceData(null, sslData, new Date(System.currentTimeMillis() + 3600000));
    }

    InstanceData expiredInstanceData() {
        return new InstanceData(null, createSslData(), new Date(System.currentTimeMillis() - 1000));
    }
}
