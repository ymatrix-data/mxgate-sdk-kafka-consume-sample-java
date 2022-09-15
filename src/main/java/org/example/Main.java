package org.example;

import cn.ymatrix.apiclient.DataPostListener;
import cn.ymatrix.apiclient.MxClient;
import cn.ymatrix.apiclient.Result;
import cn.ymatrix.builder.MxBuilder;
import cn.ymatrix.builder.RequestType;
import cn.ymatrix.concurrencycontrol.WorkerPool;
import cn.ymatrix.concurrencycontrol.WorkerPoolFactory;
import cn.ymatrix.data.Tuple;
import cn.ymatrix.exception.AllTuplesFailException;
import cn.ymatrix.exception.PartiallyTuplesFailException;
import com.alibaba.fastjson2.JSON;
import org.example.consume.KafkaMsgConsumeCallback;
import org.example.kafka.KafkaConsumerWrapper;
import org.example.logger.MxLogger;

import org.example.model.TestTable;
import org.slf4j.Logger;


public class Main {
    private static final Logger l = MxLogger.init(Main.class);

    private static final String dataSendingHost = "http://loaclhost:8086";
    private static final String gRPCHost = "localhost:8087";
    private static final String schema = "public";
    private static final String table = "test_table";
    private static MxBuilder builder;
    private static final String kafkaBootStrap = "localhost:9092";
    private static final String kafkaTopic = "topic_kafka";
    private static final String kafkaConsumerGroup = "consumer_group_1";

    public static void main(String[] args) {
        // 第一步：初始化 MxBuilder
        // 1. MxBuilder 是全局唯一的单例，在一个 Java 进程中只需要初始化一次；
        initBuilder();

        // 第二步：从 Kafka 中获取数据并写入到 SDK。
        consumeKafka(10);
    }

    public static void initBuilder() {
        builder = MxBuilder.newBuilder()
                .withCacheCapacity(100000)
                .withRequestType(RequestType.WithHTTP) // 使用 HTTP 的方式还是 gRPC 的方式向 mxgate server 发送数据
                .withCacheEnqueueTimeout(3000)
                .withConcurrency(10)
                .withRequestTimeoutMillis(5000)
                .withMaxRetryAttempts(3)
                .withRetryWaitDurationMillis(1000)
                .build();
    }

    public static void consumeKafka(int consumers) {
        WorkerPool consumerPool = WorkerPoolFactory.initFixedSizeWorkerPool(consumers);
        for (int i = 0; i < consumers; i++) {
            int finalI = i;
            Runnable runnable = new Runnable() {
                @Override
                public void run() {
                    KafkaConsumerWrapper consumer = new KafkaConsumerWrapper(kafkaBootStrap, kafkaTopic, finalI, kafkaConsumerGroup);
                    // MxClient 是线程安全的，但是为了提升效率，
                    // 每个消费者线程独站一个 MxClient 是比较好的实现方式。
                    // 通过 MxBuilder 获取 MxClient 实例的方式有两种：
                    // 方式1，同步阻塞的方式：
                    final MxClient client = builder.connect(dataSendingHost, gRPCHost, schema, table);
                    // 方式2，异步非阻塞的方式，通过回调函数获取 MxClient 实例。
//                    builder.connect(dataSendingHost, gRPCHost, schema, table, new ConnectionListener() {
//                        @Override
//                        public void onSuccess(MxClient mxClient) {
//
//                        }
//
//                        @Override
//                        public void onFailure(String s) {
//
//                        }
//                    });
                    // 设置累积满多少字节的 Tuples 一并发送到 mxgate server。
                    client.withEnoughBytesToFlush(4000000);
                    // 设置自动发送数据的定时任务的时间间隔，不管是否满足足够的字节数。
                    client.withIntervalToFlushMillis(2000);
                    client.registerDataPostListener(new DataPostListener() {
                        @Override
                        public void onSuccess(Result result) {
                            // 数据发送成功回调
                        }

                        @Override
                        public void onFailure(Result result) {
                            // 数据发送失败回调
                        }
                    });

                    consumer.consumeLoop(5000, new KafkaMsgConsumeCallback() {
                        @Override
                        public void convertMsgValueToObj(String value, int num) {
                            // 第三步：通过 MxClient 发送数据
                            TestTable tt = JSON.parseObject(value, TestTable.class);

                            // 从 MxClient 获取一个空的 Tuple。
                            Tuple tuple = client.generateEmptyTuple();

                            // 向 Tuple 中添加对应的表字段。
                            tuple.addColumn("ts", tt.getTs());
                            tuple.addColumn("tag", tt.getTag());
                            tuple.addColumn("c1", tt.getC1());
                            tuple.addColumn("c2", tt.getC2());
                            tuple.addColumn("c3", tt.getC3());
                            tuple.addColumn("c4", tt.getC4());
                            tuple.addColumn("c5", tt.getC5());
                            tuple.addColumn("c6", tt.getC6());
                            tuple.addColumn("c7", tt.getC7());
                            tuple.addColumn("c8", tt.getC8());

                            // MxClient 提供了两种方式向 mxgate server 发送数据：
                            // 方式1：同步发送数据，调用 appendTupleBlocking 会返回 boolean 类型的返回值
                            // true：表示已经满足设定的 Tuples 字节数，然后调用 flushBlocking API，即可手动
                            // 向 mxgate server 发送数据。
                            // flushBlocking 会抛出两个异常：
                            // AllTuplesFailException：所有一批 Tuples 都发生错误，无法写入数据库；
                            // PartiallyTuplesFailException：一批 Tuples 有若干 Tuple 发生了错误。
                            // 可以通过 Exception 返回的 Result 获取到对应的错误 Tuple 的详细信息。
                            try {
                                if (client.appendTupleBlocking(tuple)) {
                                    client.flushBlocking();
                                }
                            } catch (AllTuplesFailException e) {
                                Result result = e.getResult();
                                result.getErrorTuplesMap();
                            } catch (PartiallyTuplesFailException e) {
                                Result result = e.getResult();
                                result.getErrorTuplesMap();
                            }

                            // 方式2：异步发送，只需要将 Tuple append 到 MxClient，这样吞吐量会更高
                            // MxClient 会根据设置的字节数大小和自动发送的时间间隔异步地发送数据。
                            // 数据发送成功还是失败，都会通过 registerDataPostListener 注册的回调函数返回。
                            client.appendTuple(tuple);

                        }
                    });
                }
            };
            consumerPool.join(runnable);
        }
    }



}