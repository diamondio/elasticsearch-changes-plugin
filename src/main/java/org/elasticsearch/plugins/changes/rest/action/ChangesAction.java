/*
 * Copyright 2012 Thomas Peuss
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.elasticsearch.plugins.changes.rest.action;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestStatus.OK;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.index.IndexableField;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.json.JSONObject;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesLifecycle.Listener;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.changes.beans.Change;
import org.elasticsearch.plugins.changes.beans.IndexChangeWatcher;
import org.elasticsearch.plugins.changes.beans.IndexChanges;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

public class ChangesAction extends BaseRestHandler {
    private static final ESLogger LOG = Loggers.getLogger(ChangesAction.class);
    private static final String SETTING_HISTORY_SIZE = "changes.history.size";
    private IndicesService indicesService;
    private Map<String, IndexChanges> changes;

    @Inject
    public ChangesAction(Settings settings, Client client, RestController controller, IndicesService indicesService) {
        super(settings, controller, client);
        this.indicesService = indicesService;
        this.changes = new ConcurrentHashMap<String, IndexChanges>();
        controller.registerHandler(GET, "/_changes", this);
        controller.registerHandler(GET, "/{index}/_changes", this);

        registerLifecycleHandler();
    }

    private void registerLifecycleHandler() {
        indicesService.indicesLifecycle().addListener(new Listener() {
            @Override
            public void afterIndexShardStarted(IndexShard indexShard) {
                if (indexShard.routingEntry().primary()) {
                    IndexChanges indexChanges;
                    synchronized (changes) {
                        indexChanges = changes.get(indexShard.shardId().index().name());
                        if (indexChanges == null) {
                            indexChanges = new IndexChanges(indexShard.shardId().index().name(), settings.getAsInt(SETTING_HISTORY_SIZE, 100));
                            changes.put(indexShard.shardId().index().name(), indexChanges);
                        }
                    }
                    indexChanges.addShard();
                    indexShard.indexingService().addListener(indexChanges);
                }
            }

        });
    }
    
    @Override
    protected void handleRequest(RestRequest request, RestChannel channel, Client client) throws Exception {
        logger.debug("Request");

        List<String> indices = Arrays.asList(splitIndices(request.param("index")));

        if (indices.isEmpty()) {
            indices = new ArrayList<String>(changes.keySet());
        }

        boolean wait = request.paramAsBoolean("wait", Boolean.FALSE);
        long timeout = request.paramAsLong("timeout", 15 * 60 * 1000);

        // Wait for trigger
        if (wait) {
            IndexChangeWatcher watcher = addWatcher(indices, timeout);
            boolean changeDetected = watcher.aquire();
            removeWatcher(indices);
            if (!changeDetected) {
                channel.sendResponse(new BytesRestResponse(RestStatus.NOT_FOUND, "No change detected during timeout interval"));
                return;
            }
            if (watcher.getChange() == null || watcher.getChange().getType() == null) {
                channel.sendResponse(new BytesRestResponse(RestStatus.NOT_FOUND, "No more shards available to trigger waiting watch"));
                return;
            }
        }

        XContentBuilder builder = createXContentBuilderForChanges(request, indices);
        channel.sendResponse(new BytesRestResponse(OK, builder));

    }

    private void setObject(String name, Object value, JSONObject context) {
        if (!name.contains(".")) {
            context.put(name, value);
            return;
        }

        ArrayList<String> parts = new ArrayList<String>(Arrays.asList(name.split("\\.")));
        String p = parts.remove(parts.size() - 1);
        String j;
        for (int i = 0; context != null && i < parts.size() && ((j = parts.get(i)) != null); i++) {
            if (context.has(j)) {
                context = context.getJSONObject(j);
            } else {
                context.put(j, new JSONObject());
            }
        }
        if (context != null && p != null) {
            context.put(p, value);
        }
    }

    private void buildObject(XContentBuilder builder, JSONObject jObject, String objectKey) throws IOException {
        if (objectKey != null) builder.startObject(objectKey);
        else builder.startObject();

        Iterator<?> keys = jObject.keys();
        while (keys.hasNext()) {
            String key = (String)keys.next();
            if (jObject.get(key) instanceof JSONObject) {
                buildObject(builder, jObject.getJSONObject(key), key);
            } else if (jObject.get(key) instanceof Double) {
                builder.field(key, jObject.getDouble(key));
            } else if (jObject.get(key) instanceof Integer) {
                builder.field(key, jObject.getInt(key));
            } else if (jObject.get(key) instanceof String) {
                builder.field(key, jObject.getString(key));
            }
        }

        builder.endObject();
    }

    private XContentBuilder createXContentBuilderForChanges(final RestRequest request, List<String> indices) throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        try {
            long since = request.paramAsLong("since", Long.MIN_VALUE);
            builder.startObject();
            for (String indexName : indices) {
                IndexChanges indexChanges = changes.get(indexName);
                if (indexChanges == null) {
                    continue;
                }
                builder.startObject(indexName);
                builder.field("lastChange", indexChanges.getLastChange());
                List<Change> changesList = indexChanges.getChanges();
                builder.startArray("changes");

                for (Change change : changesList) {
                    if (change.getTimestamp() > since) {
                        builder.startObject();

                        builder.field("type", change.getType().toString());
                        builder.field("id", change.getId());
                        builder.field("timestamp", change.getTimestamp());
                        builder.field("version", change.getVersion());

                        builder.startArray("sources");
                        for (Document doc : change.getDocs()) {
                            JSONObject source = new JSONObject();
                            List<IndexableField> fields = doc.getFields();
                            for (IndexableField field : fields) {
                                if (field.numericValue() != null) {
                                    setObject(field.name(), field.numericValue(), source);
                                } else if (field.stringValue() != null) {
                                    setObject(field.name(), field.stringValue(), source);
                                }
                            }

                            buildObject(builder, source, null);
                        }
                        builder.endArray();

                        builder.endObject();
                    }
                }

                builder.endArray();
                builder.endObject();
            }
            builder.endObject();
        } catch (Exception e) {
            LOG.error("Error while setting XContentBuilder from response", e);
        }
        return builder;
    }

    IndexChangeWatcher addWatcher(List<String> indices, long timeout) {
        IndexChangeWatcher watcher = new IndexChangeWatcher(timeout);

        for (String index : indices) {
            IndexChanges change = changes.get(index);
            if (change != null) {
                change.addWatcher(watcher);
            }
        }

        return watcher;
    }

    void removeWatcher(List<String> indices) {
        IndexChangeWatcher watcher = new IndexChangeWatcher();

        for (String index : indices) {
            IndexChanges change = changes.get(index);
            if (change != null) {
                change.removeWatcher(watcher);
            }
        }
    }

    public static String[] splitIndices(String indices) {
        if (indices == null) {
            return Strings.EMPTY_ARRAY;
        }
        return Strings.splitStringByCommaToArray(indices);
    }
}
