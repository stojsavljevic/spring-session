package org.springframework.session.hazelcast.entryprocessor;

import java.io.IOException;
import java.util.Map;

import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public final class DeleteSessionEntryProcessor
        implements EntryProcessor<String, SessionState>,
        EntryBackupProcessor<String, SessionState>, IdentifiedDataSerializable {

	private static final long serialVersionUID = 9115108723309182129L;
	
    public DeleteSessionEntryProcessor() {
    }

    @Override
    public int getFactoryId() {
    	return SpringSessionDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return SpringSessionDataSerializerHook.SESSION_DELETE;
    }

    @Override
    public Object process(Map.Entry<String, SessionState> entry) {
        SessionState sessionState = entry.getValue();
        if (sessionState == null) {
            return Boolean.FALSE;
        }

        entry.setValue(null);
        return Boolean.TRUE;
    }

    @Override
    public EntryBackupProcessor<String, SessionState> getBackupProcessor() {
        return this;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
    }

    @Override
    public void processBackup(Map.Entry<String, SessionState> entry) {
    	process(entry);
    }
}
