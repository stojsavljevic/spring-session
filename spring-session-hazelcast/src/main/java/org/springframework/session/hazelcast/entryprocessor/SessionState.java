package org.springframework.session.hazelcast.entryprocessor;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public class SessionState implements IdentifiedDataSerializable {
	
	

	private Instant lastAccessedTime;
	private Instant creationTime;
	private Duration maxInactiveInterval;
    private final Map<String, Data> attributes = new HashMap<>(1);
    
    @Override
    public int getFactoryId() {
        return FactoryIdHelper.getFactoryId(FactoryIdHelper.WEB_DS_FACTORY, DataSerializerHook.F_ID_OFFSET_WEBMODULE);
    }

    @Override
    public int getId() {
        return SpringSessionDataSerializerHook.SESSION_STATE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(getAttributes().size());
        out.writeObject(lastAccessedTime);
        out.writeObject(getCreationTime());
        out.writeObject(getMaxInactiveInterval());
        for (Map.Entry<String, Data> entry : getAttributes().entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeData(entry.getValue());
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int attCount = in.readInt();
        this.lastAccessedTime = in.readObject();
        this.setCreationTime(in.readObject());
        this.setMaxInactiveInterval(in.readObject());
        for (int i = 0; i < attCount; i++) {
            getAttributes().put(in.readUTF(), in.readData());
        }
    }

//    public void set(Map<String, Data> attributes) {
//        this.attributes.putAll(attributes);
//    }

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
	 * @return the lastAccessdTime
	 */
	public Instant getLastAccessedTime() {
		return lastAccessedTime;
	}

	/**
	 * @param lastAccessdTime the lastAccessdTime to set
	 */
	public void setLastAccessedTime(Instant lastAccessedTime) {
		this.lastAccessedTime = lastAccessedTime;
	}

	/**
	 * @return the creationTime
	 */
	public Instant getCreationTime() {
		return creationTime;
	}

	/**
	 * @param creationTime the creationTime to set
	 */
	public void setCreationTime(Instant creationTime) {
		this.creationTime = creationTime;
	}

	/**
	 * @return the maxInactiveInterval
	 */
	public Duration getMaxInactiveInterval() {
		return maxInactiveInterval;
	}

	/**
	 * @param maxInactiveInterval the maxInactiveInterval to set
	 */
	public void setMaxInactiveInterval(Duration maxInactiveInterval) {
		this.maxInactiveInterval = maxInactiveInterval;
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
