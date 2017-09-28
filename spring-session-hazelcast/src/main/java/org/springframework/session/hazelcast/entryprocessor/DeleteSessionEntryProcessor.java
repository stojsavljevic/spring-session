package org.springframework.session.hazelcast.entryprocessor;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.Map;

public final class DeleteSessionEntryProcessor
        implements EntryProcessor<String, SessionState>,
        EntryBackupProcessor<String, SessionState>, IdentifiedDataSerializable {

	private static final long serialVersionUID = 9115108723309182129L;
	
	private boolean invalidate;
    private boolean removed;

    public DeleteSessionEntryProcessor(boolean invalidate) {
        this.invalidate = invalidate;
    }

    public DeleteSessionEntryProcessor() {
    }

    @Override
    public int getFactoryId() {
    	return FactoryIdHelper.getFactoryId(FactoryIdHelper.WEB_DS_FACTORY, DataSerializerHook.F_ID_OFFSET_WEBMODULE);
    }

    @Override
    public int getId() {
        return 2;
    }

    @Override
    public Object process(Map.Entry<String, SessionState> entry) {
        SessionState sessionState = entry.getValue();
        if (sessionState == null) {
            return Boolean.FALSE;
        }

        if (invalidate) {
            entry.setValue(null);
            removed = true;
        } else {
            entry.setValue(sessionState);
        }
        return Boolean.TRUE;
    }

    @Override
    public EntryBackupProcessor<String, SessionState> getBackupProcessor() {
        return (removed) ? this : null;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeBoolean(invalidate);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        invalidate = in.readBoolean();
    }

    @Override
    public void processBackup(Map.Entry<String, SessionState> entry) {
        SessionState sessionState = entry.getValue();
        if (sessionState != null) {
            entry.setValue(null);
        }
    }
}
