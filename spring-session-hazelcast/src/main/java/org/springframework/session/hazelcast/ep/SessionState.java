package org.springframework.session.hazelcast.ep;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public class SessionState implements IdentifiedDataSerializable {
	
	
	private Map<String, Data> attributes = new HashMap<>(1);

	public SessionState() {
		this.attributes = new HashMap<>();
	}

	public SessionState(Map<String, Data> attributes) {
		this.attributes = attributes;
	}
    
    @Override
    public int getFactoryId() {
		return SpringSessionDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return SpringSessionDataSerializerHook.SESSION_STATE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(getAttributes().size());
        for (Map.Entry<String, Data> entry : getAttributes().entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeData(entry.getValue());
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int attCount = in.readInt();
        for (int i = 0; i < attCount; i++) {
            getAttributes().put(in.readUTF(), in.readData());
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("SessionState {");
        sb.append(", attributes=" + ((getAttributes() == null) ? 0 : getAttributes().size()));
        if (getAttributes() != null) {
            for (Map.Entry<String, Data> entry : getAttributes().entrySet()) {
                Data data = entry.getValue();
                int len = (data == null) ? 0 : data.dataSize();
                sb.append("\n\t");
                sb.append(entry.getKey() + "[" + len + "]");
            }
        }
        sb.append("\n}");
        return sb.toString();
    }


	/**
	 * @return the attributes
	 */
	public Map<String, Data> getAttributes() {
		return attributes;
	}
	
	public Data getAttribute(String attributeName) {
		return attributes.get(attributeName);
	}
	
	public void setAttribute(String attributeName, Data attributeValue) {
		attributes.put(attributeName, attributeValue);
	}
}
