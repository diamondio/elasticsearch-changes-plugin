/*
   Copyright 2012 Thomas Peuss

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */
package org.elasticsearch.plugins.changes.beans;

import java.util.List;
import org.elasticsearch.index.mapper.ParseContext.Document;

public class Change implements Comparable<Change> {
    long timestamp;
    Type type;
    String id;
    long version;
    java.util.List<Document> docs;
    
    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public java.util.List<Document> getDocs() {
        return docs;
    }

    public void setDocs(java.util.List<Document> docs) {
        this.docs = docs;
    }

    public enum Type {
        INDEX,CREATE,DELETE;
    }

    @Override
    public int compareTo(Change o) {
        if (this.timestamp<o.timestamp) {
            return -1;
        } else if (this.timestamp>o.timestamp) {
            return 1;
        } else {
            return 0;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Change)) {
            return false;
        }

        Change other = (Change)o;
        return this.timestamp == other.timestamp;
    }

    @Override
    public int hashCode() {
        return this.hashCode();
    }
}
