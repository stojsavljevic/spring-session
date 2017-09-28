package org.springframework.session.hazelcast.entryprocessor;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public class UpdateAttributeEntryProcessor implements EntryProcessor<String, SessionState>,
		EntryBackupProcessor<String, SessionState>, IdentifiedDataSerializable {

	private static final long serialVersionUID = -6386424630862009224L;
	
	private Map<String, Data> attributes;

	public UpdateAttributeEntryProcessor(int size) {
		this.attributes = new HashMap<String, Data>(size);
	}

	public UpdateAttributeEntryProcessor(String key, Data value) {
		attributes = new HashMap<String, Data>(1);
		attributes.put(key, value);
	}

	public UpdateAttributeEntryProcessor() {
		attributes = Collections.emptyMap();
	}

	public Map<String, Data> getAttributes() {
		return attributes;
	}

	@Override
	public int getFactoryId() {
		return FactoryIdHelper.getFactoryId(FactoryIdHelper.WEB_DS_FACTORY, DataSerializerHook.F_ID_OFFSET_WEBMODULE);
	}

	@Override
	public int getId() {
		return 1;
	}

	@Override
	public Object process(Map.Entry<String, SessionState> entry) {
		SessionState sessionState = entry.getValue();
		if (sessionState == null) {
			sessionState = new SessionState();
		}
		for (Map.Entry<String, Data> attribute : attributes.entrySet()) {
			String name = attribute.getKey();
			Data value = attribute.getValue();
			if (value == null) {
				sessionState.getAttributes().remove(name);
			} else {
				sessionState.getAttributes().put(name, value);
			}
		}
		entry.setValue(sessionState);
		return Boolean.TRUE;
	}

	@Override
	public EntryBackupProcessor<String, SessionState> getBackupProcessor() {
		return this;
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
		attributes = new HashMap<String, Data>(attCount);
		for (int i = 0; i < attCount; i++) {
			attributes.put(in.readUTF(), in.readData());
		}
	}

	@Override
	public void processBackup(Map.Entry<String, SessionState> entry) {
		process(entry);
	}
}
