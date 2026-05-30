/*
 * Copyright (c) 2026 the original author or authors.
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
package me.predatorray.candybox.s3;

import java.util.List;
import java.util.Map;
import me.predatorray.candybox.client.CandyboxClient;
import me.predatorray.candybox.client.CandyboxClient.CandyInfo;
import me.predatorray.candybox.client.CandyboxClient.Listing;

/**
 * The production {@link CandyStore}: delegates to a cluster-aware {@link CandyboxClient}. Object writes
 * pass a {@code null} idempotency token (S3 PUT has no such concept). The gateway owns the client's
 * lifecycle and closes it on shutdown.
 */
final class CandyboxClientStore implements CandyStore, AutoCloseable {

    private final CandyboxClient client;

    CandyboxClientStore(CandyboxClient client) {
        this.client = client;
    }

    @Override
    public void createBox(String box) {
        client.createBox(box);
    }

    @Override
    public void deleteBox(String box) {
        client.deleteBox(box, false);
    }

    @Override
    public boolean headBox(String box) {
        return client.headBox(box);
    }

    @Override
    public List<String> listBoxes() {
        return client.listBoxes();
    }

    @Override
    public void putCandy(String box, String key, byte[] data, String contentType,
                         Map<String, String> userMetadata) {
        client.putCandy(box, key, data, contentType, userMetadata, null);
    }

    @Override
    public byte[] getCandy(String box, String key) {
        return client.getCandy(box, key);
    }

    @Override
    public CandyInfo headCandy(String box, String key) {
        return client.headCandy(box, key);
    }

    @Override
    public void deleteCandy(String box, String key) {
        client.deleteCandy(box, key);
    }

    @Override
    public CandyInfo copyCandy(String box, String srcKey, String dstKey) {
        return client.copyCandy(box, srcKey, dstKey, null);
    }

    @Override
    public Listing listCandies(String box, String prefix, String startAfter, int maxKeys) {
        return client.listCandies(box, prefix, startAfter, maxKeys);
    }

    @Override
    public void close() {
        client.close();
    }
}
