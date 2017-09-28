package org.springframework.session.hazelcast.entryprocessor;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SessionState implements IdentifiedDataSerializable {

    private final Map<String, Data> attributes = new HashMap<String, Data>(1);

    @Override
    public int getFactoryId() {
        return FactoryIdHelper.getFactoryId(FactoryIdHelper.WEB_DS_FACTORY, DataSerializerHook.F_ID_OFFSET_WEBMODULE);
    }

    @Override
    public int getId() {
        return 6;
    }

    public Map<String, Data> getAttributes() {
        return attributes;
    }

    public void setAttribute(String key, Data value) {
        attributes.put(key, value);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(attributes.size());
        for (Map.Entry<String, Data> entry : attributes.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeData(entry.getValue());
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int attCount = in.readInt();
        for (int i = 0; i < attCount; i++) {
            attributes.put(in.readUTF(), in.readData());
        }
    }

    public void set(Map<String, Data> attributes) {
        this.attributes.putAll(attributes);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("SessionState {");
        sb.append(", attributes=" + ((attributes == null) ? 0 : attributes.size()));
        if (attributes != null) {
            for (Map.Entry<String, Data> entry : attributes.entrySet()) {
                Data data = entry.getValue();
                int len = (data == null) ? 0 : data.dataSize();
                sb.append("\n\t");
                sb.append(entry.getKey() + "[" + len + "]");
            }
        }
        sb.append("\n}");
        return sb.toString();
    }
}
