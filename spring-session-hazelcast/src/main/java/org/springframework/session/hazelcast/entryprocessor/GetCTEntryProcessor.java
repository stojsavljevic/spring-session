package org.springframework.session.hazelcast.entryprocessor;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;

import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public class GetCTEntryProcessor implements EntryProcessor<String, SessionState>, IdentifiedDataSerializable {

	private static final long serialVersionUID = -4279090762028535172L;

	public GetCTEntryProcessor() {
    }

    @Override
    public int getFactoryId() {
        return SpringSessionDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return SpringSessionDataSerializerHook.GET_CT;
    }

    @Override
    public Instant process(Map.Entry<String, SessionState> entry) {
        SessionState sessionState = entry.getValue();
        if (sessionState == null) {
            return null;
        }
        entry.setValue(sessionState);
        return sessionState.getCreationTime();
    }

    @Override
    public EntryBackupProcessor<String, SessionState> getBackupProcessor() {
        return null;
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
    }
}
