package org.springframework.session.hazelcast.entryprocessor;

import java.io.IOException;
import java.util.Map;

import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public final class GetSessionEntryProcessor implements EntryProcessor<String, SessionState>,
        IdentifiedDataSerializable {

	private static final long serialVersionUID = 1367205300417577625L;

	public GetSessionEntryProcessor() {
    }

    @Override
    public int getFactoryId() {
        return SpringSessionDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return SpringSessionDataSerializerHook.GET_SESSION;
    }

    @Override
    public Object process(Map.Entry<String, SessionState> entry) {
        SessionState sessionState = entry.getValue();
        if (sessionState == null) {
            return null;
        }
        entry.setValue(sessionState);
        return sessionState;
    }

    @Override
    public EntryBackupProcessor<String, SessionState> getBackupProcessor() {
        return null;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
    }
}
