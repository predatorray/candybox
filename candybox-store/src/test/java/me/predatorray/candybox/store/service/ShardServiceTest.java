/*
 * Copyright (c) 2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package me.predatorray.candybox.store.service;

import com.google.common.collect.Lists;
import com.google.common.primitives.Bytes;
import com.google.protobuf.ByteString;
import io.grpc.ServerInterceptors;
import io.grpc.stub.StreamObserver;
import io.grpc.util.TransmitStatusRuntimeExceptionInterceptor;
import me.predatorray.candybox.proto.CommonProtos;
import me.predatorray.candybox.proto.CommonProtos.ObjectDeleteRequest;
import me.predatorray.candybox.proto.CommonProtos.ObjectDeleteResponse;
import me.predatorray.candybox.proto.CommonProtos.ObjectKey;
import me.predatorray.candybox.proto.CommonProtos.Shard;
import me.predatorray.candybox.proto.ShardServiceGrpc;
import me.predatorray.candybox.store.SingleDataDirLocalShardManager;
import me.predatorray.candybox.store.config.DefaultConfiguration;
import me.predatorray.candybox.store.util.CompletableStreamObserver;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ShardServiceTest extends AbstractGrpcService {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void setUpGrpcService() throws Exception {
        File dataDir = temporaryFolder.newFolder();
        Path dataDirPath = dataDir.toPath();

        ShardService srv = new ShardService(new SingleDataDirLocalShardManager(new DefaultConfiguration() {
            @Override
            public List<Path> getDataDirectoryPaths() {
                return Collections.singletonList(dataDirPath);
            }
        }));
        grpcServerRule.getServiceRegistry().addService(ServerInterceptors.intercept(srv,
            TransmitStatusRuntimeExceptionInterceptor.instance()));
    }

    private ShardServiceGrpc.ShardServiceBlockingStub newBlockingStub() {
        return ShardServiceGrpc.newBlockingStub(grpcServerRule.getChannel());
    }

    private ShardServiceGrpc.ShardServiceStub newStub() {
        return ShardServiceGrpc.newStub(grpcServerRule.getChannel());
    }

    @Test
    public void fetchNotExistedShard() {
        ShardServiceGrpc.ShardServiceBlockingStub sut = newBlockingStub();
        Iterator<CommonProtos.ObjectFetchResponse> response = sut.fetch(CommonProtos.ObjectFetchRequest.newBuilder()
                .setShard(CommonProtos.Shard.newBuilder()
                        .setBoxName("not-found").setOffset(0))
                .setObjectKey(CommonProtos.ObjectKey.newBuilder()
                        .setValue("not-found"))
                .build());
        assertNotFoundStatusThrown(() -> {
            while (response.hasNext()) {
                response.next();
            }
        });
    }

    @Test
    public void appendToNotExistedShard() throws Exception {
        ShardServiceGrpc.ShardServiceStub sut = newStub();

        CompletableStreamObserver<CommonProtos.ObjectAppendResponse> observer = new CompletableStreamObserver<>();
        StreamObserver<CommonProtos.ObjectAppendRequest> request = sut.append(observer);

        final String objectKey = "foobar";
        final byte[] data = new byte[] {1, 2, 3};
        request.onNext(CommonProtos.ObjectAppendRequest.newBuilder()
                .setShard(CommonProtos.Shard.newBuilder()
                        .setBoxName("not-found").setOffset(0))
                .setObjectKey(CommonProtos.ObjectKey.newBuilder().setValue(objectKey))
                .setBody(CommonProtos.Chunk.newBuilder().setData(ByteString.copyFrom(data)))
                .build());
        request.onCompleted();

        CompletableFuture<List<CommonProtos.ObjectAppendResponse>> cf = observer.getCompletableFuture();
        assertNotFoundStatusThrown(() -> {
            try {
                cf.get();
            } catch (ExecutionException e) {
                throw (Exception) e.getCause();
            }
        });
    }

    @Test
    public void fetchEmptyShard() {
        final String boxName = "foobar";
        final int offset = 0;
        ShardServiceGrpc.ShardServiceBlockingStub sut = newBlockingStub();
        CommonProtos.ShardInitializeResponse initializeResponse = sut.initialize(
                CommonProtos.ShardInitializeRequest.newBuilder()
                        .setShard(CommonProtos.Shard.newBuilder().setBoxName(boxName).setOffset(offset))
                        .build());
        assertEquals(CommonProtos.ShardInitializeResponse.Status.INITIALIZED, initializeResponse.getStatus());

        Iterator<CommonProtos.ObjectFetchResponse> fetchResponse = sut.fetch(
                CommonProtos.ObjectFetchRequest.newBuilder()
                        .setShard(CommonProtos.Shard.newBuilder()
                                .setBoxName("not-found").setOffset(0))
                        .setObjectKey(CommonProtos.ObjectKey.newBuilder()
                                .setValue("not-found"))
                        .build());
        assertNotFoundStatusThrown(() -> {
            while (fetchResponse.hasNext()) {
                fetchResponse.next();
            }
        });
    }

    @Test
    public void initializeAppendAndFetch() throws Exception {
        ShardServiceGrpc.ShardServiceStub sut = newStub();
        ShardServiceGrpc.ShardServiceBlockingStub blockingSut = newBlockingStub();
        final String boxName = "foobar";
        final int offset = 0;
        final String objectKey = "foobar";
        final byte[] data = new byte[] {1, 2, 3};

        // initialize
        CommonProtos.ShardInitializeResponse initializeResponse = blockingSut.initialize(
                CommonProtos.ShardInitializeRequest.newBuilder()
                        .setShard(CommonProtos.Shard.newBuilder().setBoxName(boxName).setOffset(offset))
                        .build());
        assertEquals(CommonProtos.ShardInitializeResponse.Status.INITIALIZED, initializeResponse.getStatus());

        // append
        CompletableStreamObserver<CommonProtos.ObjectAppendResponse> observer = new CompletableStreamObserver<>();
        StreamObserver<CommonProtos.ObjectAppendRequest> request = sut.append(observer);
        request.onNext(CommonProtos.ObjectAppendRequest.newBuilder()
                .setShard(CommonProtos.Shard.newBuilder()
                        .setBoxName(boxName).setOffset(offset))
                .setObjectKey(CommonProtos.ObjectKey.newBuilder().setValue(objectKey))
                .setBody(CommonProtos.Chunk.newBuilder().setData(ByteString.copyFrom(data)))
                .build());
        request.onCompleted();
        CompletableFuture<List<CommonProtos.ObjectAppendResponse>> cf = observer.getCompletableFuture();
        List<CommonProtos.ObjectAppendResponse> appendResponses = cf.get();
        assertEquals(1, appendResponses.size());
        CommonProtos.ObjectAppendResponse appendResponse = appendResponses.get(0);
        assertEquals(CommonProtos.ObjectAppendResponse.Status.APPENDED, appendResponse.getStatus());

        // fetch
        Iterator<CommonProtos.ObjectFetchResponse> fetchResponseIt = blockingSut.fetch(
                CommonProtos.ObjectFetchRequest.newBuilder()
                        .setShard(CommonProtos.Shard.newBuilder()
                                .setBoxName(boxName).setOffset(offset))
                        .setObjectKey(CommonProtos.ObjectKey.newBuilder()
                                .setValue(objectKey))
                        .build());
        ArrayList<CommonProtos.ObjectFetchResponse> fetchResponses = Lists.newArrayList(fetchResponseIt);
        assertTrue(fetchResponses.size() > 0);

        CommonProtos.ObjectFetchResponse firstFetchResponse = fetchResponses.get(0);
        assertEquals(CommonProtos.ObjectFetchResponse.Status.OK, firstFetchResponse.getStatus());
        byte[] fetchedData = Bytes.concat(fetchResponses.stream()
                .map(res -> res.getBody().getData().toByteArray())
                .toArray(byte[][]::new));
        assertArrayEquals(data, fetchedData);
    }

    @Test
    public void initializeAppendDeleteAndFetch() throws Exception {
        ShardServiceGrpc.ShardServiceStub sut = newStub();
        ShardServiceGrpc.ShardServiceBlockingStub blockingSut = newBlockingStub();
        final String boxName = "foobar";
        final int offset = 0;
        final String objectKey = "foobar";
        final byte[] data = new byte[] {1, 2, 3};

        // initialize
        blockingSut.initialize(
            CommonProtos.ShardInitializeRequest.newBuilder()
                .setShard(CommonProtos.Shard.newBuilder().setBoxName(boxName).setOffset(offset))
                .build());

        // append
        CompletableStreamObserver<CommonProtos.ObjectAppendResponse> observer = new CompletableStreamObserver<>();
        StreamObserver<CommonProtos.ObjectAppendRequest> request = sut.append(observer);
        request.onNext(CommonProtos.ObjectAppendRequest.newBuilder()
            .setShard(CommonProtos.Shard.newBuilder()
                .setBoxName(boxName).setOffset(offset))
            .setObjectKey(CommonProtos.ObjectKey.newBuilder().setValue(objectKey))
            .setBody(CommonProtos.Chunk.newBuilder().setData(ByteString.copyFrom(data)))
            .build());
        request.onCompleted();
        CompletableFuture<List<CommonProtos.ObjectAppendResponse>> cf = observer.getCompletableFuture();
        cf.get();

        // delete
        ObjectDeleteResponse deleteResponse = blockingSut.delete(ObjectDeleteRequest.newBuilder()
            .setObjectKey(ObjectKey.newBuilder().setValue(objectKey))
            .setShard(Shard.newBuilder().setBoxName(boxName).setOffset(offset))
            .build());
        assertEquals(ObjectDeleteResponse.Status.DELETED, deleteResponse.getStatus());

        // fetch
        Iterator<CommonProtos.ObjectFetchResponse> fetchResponseIt = blockingSut.fetch(
            CommonProtos.ObjectFetchRequest.newBuilder()
                .setShard(CommonProtos.Shard.newBuilder()
                    .setBoxName(boxName).setOffset(offset))
                .setObjectKey(CommonProtos.ObjectKey.newBuilder()
                    .setValue(objectKey))
                .build());
        assertNotFoundStatusThrown(() -> {
            while (fetchResponseIt.hasNext()) {
                fetchResponseIt.next();
            }
        });
    }
}
