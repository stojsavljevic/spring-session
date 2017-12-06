package org.springframework.session.hazelcast.ep;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public class SessionUpdateEntryProcessor implements EntryProcessor<String, SessionState>,
		EntryBackupProcessor<String, SessionState>, IdentifiedDataSerializable {

	private static final long serialVersionUID = 9073171388557840680L;

	private SessionState sessionState;

	public SessionUpdateEntryProcessor(SessionState sessionState) {
		this.sessionState = sessionState;
	}

	public SessionUpdateEntryProcessor() {
		this.sessionState = new SessionState();
	}

	@Override
	public Object process(Map.Entry<String, SessionState> entry) {
		SessionState value = entry.getValue();
		if (value == null) {
			value = new SessionState();
		}
		for (final Map.Entry<String, Data> attribute : this.sessionState.getAttributes()
				.entrySet()) {
			if (attribute.getValue() != null) {
				value.setAttribute(attribute.getKey(), attribute.getValue());
			} else {
				value.getAttributes().remove(attribute.getKey());
			}
		}
		entry.setValue(value);
		return value;
	}

	@Override
	public void writeData(ObjectDataOutput out) throws IOException {
		sessionState.writeData(out);
	}

	@Override
	public void readData(ObjectDataInput in) throws IOException {
		sessionState.readData(in);
	}

	@Override
	public int getFactoryId() {
		return SpringSessionDataSerializerHook.F_ID;
	}

	@Override
	public int getId() {
		return SpringSessionDataSerializerHook.SESSION_UPDATE_EP;
	}

	@Override
	public void processBackup(Entry<String, SessionState> entry) {
		process(entry);
	}

	@Override
	public EntryBackupProcessor<String, SessionState> getBackupProcessor() {
		return this;
	}

}
