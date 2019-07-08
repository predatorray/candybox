/*
 * Copyright (c) 2018 the original author or authors.
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

import com.google.protobuf.UnsafeByteOperations;
import io.grpc.stub.StreamObserver;
import me.predatorray.candybox.ObjectKey;
import me.predatorray.candybox.proto.CommonProtos;
import me.predatorray.candybox.proto.ShardServiceGrpc;
import me.predatorray.candybox.store.BlockLocation;
import me.predatorray.candybox.store.CandyBlock;
import me.predatorray.candybox.store.CandyBlockIOException;
import me.predatorray.candybox.store.LocalShard;
import me.predatorray.candybox.store.LocalShardManager;
import me.predatorray.candybox.store.ShardNotFoundException;
import me.predatorray.candybox.store.SuperBlock;
import me.predatorray.candybox.store.SuperBlockIndex;
import me.predatorray.candybox.store.util.GrpcUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

public class ShardService extends ShardServiceGrpc.ShardServiceImplBase {

    private static final Logger logger = LoggerFactory.getLogger(ShardService.class);

    private LocalShardManager localShardManager;

    public ShardService(LocalShardManager localShardManager) {
        this.localShardManager = Objects.requireNonNull(localShardManager, "localShardManager must not be null");
    }

    @Override
    public void initialize(CommonProtos.ShardInitializeRequest request,
                           StreamObserver<CommonProtos.ShardInitializeResponse> responseObserver) {
        CommonProtos.Shard shardProto = request.getShard();
        String boxName = shardProto.getBoxName();
        int offset = shardProto.getOffset();
        try {
            localShardManager.initialize(boxName, offset);
        } catch (CandyBlockIOException e) {
            String errorMessage = "Shard " + boxName + "[" + offset + "] initialization failed.";
            logger.error(errorMessage, e);
            GrpcUtils.sendInternalError(responseObserver, errorMessage, e);
            return;
        }

        responseObserver.onNext(CommonProtos.ShardInitializeResponse.newBuilder()
                .setStatus(CommonProtos.ShardInitializeResponse.Status.INITIALIZED)
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void fetch(CommonProtos.ObjectFetchRequest request,
                      StreamObserver<CommonProtos.ObjectFetchResponse> responseObserver) {
        CommonProtos.Shard shardProto = request.getShard();
        CommonProtos.ObjectKey objectKeyProto = request.getObjectKey();
        ObjectKey objectKey = new ObjectKey(objectKeyProto.getValue());

        dealWithLocalShard(shardProto, responseObserver, localShardManager, objectKey, localShard -> {
            try (LocalShard.Snapshot snapshot = localShard.takeSnapshot()) {
                SuperBlockIndex idx = snapshot.index();
                BlockLocation location = idx.queryLocation(objectKey);
                if (location == null) {
                    GrpcUtils.sendNotFoundError(responseObserver, "Object named " + objectKey + " is not found.", null);
                    return;
                }

                SuperBlock block = snapshot.block();
                CandyBlock candyBlock;
                try {
                    candyBlock = block.getCandyBlockAt(location);
                } catch (IOException e) {
                    String errorMessage = "Fetching candy block at " + location + " for object " + objectKey +
                            " failed.";
                    logger.error(errorMessage, e);
                    GrpcUtils.sendInternalError(responseObserver, errorMessage, e);
                    return;
                }

                List<ByteBuffer> dataMaps = candyBlock.getObjectDataMaps();
                for (ByteBuffer dataMap : dataMaps) {
                    CommonProtos.Chunk chunk = CommonProtos.Chunk.newBuilder()
                            .setData(UnsafeByteOperations.unsafeWrap(dataMap))
                            .build();
                    CommonProtos.ObjectFetchResponse response = CommonProtos.ObjectFetchResponse.newBuilder()
                            .setBody(chunk)
                            .build();
                    responseObserver.onNext(response);
                }

                responseObserver.onCompleted();
            }
        });
    }

    @Override
    public StreamObserver<CommonProtos.ObjectAppendRequest> append(
                       StreamObserver<CommonProtos.ObjectAppendResponse> responseObserver) {
        return new ShardServiceAppendObserver(responseObserver, localShardManager);
    }

    static void dealWithLocalShard(CommonProtos.Shard shardProto, StreamObserver<?> observer,
                                   LocalShardManager localShardManager,
                                   ObjectKey objectKey,
                                   Consumer<LocalShard> consumer) {
        String boxName = shardProto.getBoxName();
        int offset = shardProto.getOffset();

        LocalShard localShard;
        try {
            localShard = localShardManager.find(boxName, offset);
        } catch (ShardNotFoundException notFound) {
            logger.error(notFound.getMessage(), notFound);
            GrpcUtils.sendNotFoundError(observer, null, notFound);
            return;
        } catch (CandyBlockIOException e) {
            String errorMessage = "Finding shard " + boxName + "[" + offset + "] for object " + objectKey + " failed.";
            logger.error(errorMessage, e);
            GrpcUtils.sendInternalError(observer, errorMessage, e);
            return;
        }

        try {
            consumer.accept(localShard);
        } catch (Exception e) {
            GrpcUtils.sendInternalError(observer, "unexpected error occurred while dealing with the local shard", e);
        }
    }
}
