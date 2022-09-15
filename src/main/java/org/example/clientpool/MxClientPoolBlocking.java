package org.example.clientpool;

import cn.ymatrix.apiclient.MxClient;
import cn.ymatrix.builder.ConnectionListener;
import cn.ymatrix.builder.MxBuilder;
import cn.ymatrix.builder.RequestType;
import org.example.logger.MxLogger;
import org.example.utils.StrUtils;
import org.slf4j.Logger;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

public class MxClientPoolBlocking {
    private static final String TAG = MxClientPoolBlocking.class.getName();
    private static final Logger l = MxLogger.init(MxClientPoolBlocking.class);
    private static int builderCacheCapacity;
    private static boolean builderDropAll;
    private static RequestType builderRequestType;
    private static int builderCacheEnqueueTimeout;
    private static int builderSDKConcurrency;
    private static int builderRequestTimeout;
    private static int builderMaxRetryAttempts;
    private static int builderRetryWaitDurationMillis;
    private static int clientsPoolSize;
    private final Map<String, LinkedBlockingDeque<MxClient>> clientsPoolMap;

    public static void prepareBuilder(int cacheCapacity, RequestType type, int cacheEnqueueTimeout,
                               int sdkConcurrency, int requestTimeout, int maxRetryAttempts, int retryWaitDurationMillis) {
        builderCacheCapacity = cacheCapacity;
        builderRequestType = type;
        builderCacheEnqueueTimeout = cacheEnqueueTimeout;
        builderSDKConcurrency = sdkConcurrency;
        builderRequestTimeout = requestTimeout;
        builderMaxRetryAttempts = maxRetryAttempts;
        builderRetryWaitDurationMillis = retryWaitDurationMillis;
    }

    public static void prepareClientPool(int size) {
        clientsPoolSize = size;
    }

    private final MxBuilder builder;

    public static MxClientPoolBlocking getInstance() {
        return innerClass.instance;
    }

    private MxClientPoolBlocking() {
        builder = MxBuilder.newBuilder()
                .withCacheCapacity(builderCacheCapacity)
                .withDropAll(builderDropAll)
                .withRequestType(builderRequestType)
                .withCacheEnqueueTimeout(builderCacheEnqueueTimeout)
                .withConcurrency(builderSDKConcurrency)
                .withRequestTimeoutMillis(builderRequestTimeout)
                .withMaxRetryAttempts(builderMaxRetryAttempts)
                .withRetryWaitDurationMillis(builderRetryWaitDurationMillis)
                .build();
        this.clientsPoolMap = new ConcurrentHashMap<>();
    }

    public synchronized String generateMxClients(String dataSendingHost, String grpcHost, String schema, String table) {
        String key = clientPoolKey(dataSendingHost, grpcHost, schema, table);
        if (this.clientsPoolMap.get(key) == null) {
            this.clientsPoolMap.put(key, generateClientPool(clientsPoolSize));
        }
        Queue<MxClient> q = this.clientsPoolMap.get(key);
        if (q == null) {
            l.error("{} Get null clients queue from clientsPoolMap", TAG);
            return "";
        }
        for (int i = 0; i < clientsPoolSize; i++) {
            MxClient client = getMxClientInstance(dataSendingHost, grpcHost, schema, table);
            q.add(client);
            l.info("{} Generate MxClient successfully for key = {} with sequence = {}", TAG, key, i);
        }
        return key;
    }

    private LinkedBlockingDeque<MxClient> generateClientPool(int size) {
        return new LinkedBlockingDeque<>(size);
    }

    private static class innerClass {
        private static final MxClientPoolBlocking instance = new MxClientPoolBlocking();
    }

    public MxClient getMxClient(String clientKey) {
        LinkedBlockingDeque<MxClient> q = this.clientsPoolMap.get(clientKey);
        if (q == null) {
            l.error("{} Could not get client queue from clients map for key {}", TAG, clientKey);
            return null;
        }
        try {
            return q.take();
        } catch (InterruptedException e) {
            l.error("{} Take MxClient from clients queue exception: ", TAG, e);
        }
        return null;
    }

    public void giveBack(MxClient client, String key) {
        if (client == null) {
            l.error("{} Client which key = {} is null to giveBack.", TAG, key);
            return;
        }
        if (this.clientsPoolMap.get(key) == null) {
            l.error("{} Can not give the client back, no clients map for key {}", TAG, key);
            return;
        }
        try {
            this.clientsPoolMap.get(key).put(client);
        } catch (InterruptedException e) {
            l.error("{} Client map give back exception: ", TAG, e);
        }
    }

    private MxClient getMxClientInstance(String dataSendingHost, String grpcHost, String schema, String table) {
        CountDownLatch latch = new CountDownLatch(1);
        final MxClient[] mxClient = new MxClient[1];
        try {
            builder.connect(dataSendingHost, grpcHost, schema, table, new ConnectionListener() {
                @Override
                public void onSuccess(MxClient client) {
                    if (client != null) {
                        mxClient[0] = client;
                    } else {
                        l.error("{} Get null MxClient from MxBuilder connect.", TAG);
                    }
                    latch.countDown();
                }
                @Override
                public void onFailure(String failureMsg) {
                    l.error("{} MxClient init error onFailure: {}", TAG, failureMsg);
                }
            });

            if (latch.await(3, TimeUnit.SECONDS)) {
                return mxClient[0];
            }
            return null;
        } catch (Exception e) {
            l.info("MxClient init error onException: " + e.getMessage(), e);
        }
        return null;
    }

    private static String clientPoolKey(String dataSendingHost, String grpcHost, String schema, String table) {
        return StrUtils.connect(dataSendingHost, grpcHost, schema, table);
    }


}
