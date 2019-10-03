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

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import me.predatorray.candybox.ObjectKey;
import me.predatorray.candybox.proto.CommonProtos;
import me.predatorray.candybox.store.BlockLocation;
import me.predatorray.candybox.store.CandyBlock;
import me.predatorray.candybox.store.LocalShard;
import me.predatorray.candybox.store.LocalShardManager;
import me.predatorray.candybox.store.util.GrpcUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

class ShardServiceAppendObserver implements StreamObserver<CommonProtos.ObjectAppendRequest> {

    private static final Logger logger = LoggerFactory.getLogger(ShardServiceAppendObserver.class);

    private final StreamObserver<CommonProtos.ObjectAppendResponse> responseObserver;
    private final LocalShardManager localShardManager;

    private ObjectKey objectKey;
    private LocalShard localShard;
    private LinkedList<ByteBuffer> dataBufferList = new LinkedList<>();

    public ShardServiceAppendObserver(StreamObserver<CommonProtos.ObjectAppendResponse> responseObserver,
                                      LocalShardManager localShardManager) {
        this.responseObserver = responseObserver;
        this.localShardManager = localShardManager;
    }

    @Override
    public void onNext(CommonProtos.ObjectAppendRequest request) {
        CommonProtos.Shard shardProto = request.getShard();
        CommonProtos.ObjectKey objectKeyProto = request.getObjectKey();
        this.objectKey = new ObjectKey(objectKeyProto.getValue());

        if (this.localShard == null) {
            ShardServiceAppendObserver.this.localShard = ShardService.getLocalShard(localShardManager, shardProto);
        }

        CommonProtos.Chunk body = request.getBody();
        if (body == null) {
            return;
        }
        ByteString bodyData = body.getData();
        List<ByteBuffer> partialData = bodyData.asReadOnlyByteBufferList();
        dataBufferList.addAll(partialData);
    }

    @Override
    public void onError(Throwable t) {
        String errorMessage = "unexpected exception error occurred while appending the shard";
        logger.error(errorMessage, t);
        GrpcUtils.sendInternalError(responseObserver, errorMessage, t);
    }

    @Override
    public void onCompleted() {
        CandyBlock candyBlock = new CandyBlock(objectKey, dataBufferList);
        localShard.append(candyBlock, new LocalShard.AppendCallback() {
            @Override
            public void onCompleted(BlockLocation location) {
                responseObserver.onNext(CommonProtos.ObjectAppendResponse.newBuilder()
                        .setStatus(CommonProtos.ObjectAppendResponse.Status.APPENDED)
                        .setLocation(CommonProtos.BlockLocation.newBuilder()
                                .setOffset(location.getOffset())
                                .setLength(location.getLength())
                                .build())
                        .build());
                responseObserver.onCompleted();
            }

            @Override
            public void onError(Throwable t) {
                ShardServiceAppendObserver.this.onError(t);
            }
        });
    }
}
