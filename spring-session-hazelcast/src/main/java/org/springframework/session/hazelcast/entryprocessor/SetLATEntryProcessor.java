package org.springframework.session.hazelcast.entryprocessor;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.Map.Entry;

import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public class SetLATEntryProcessor implements EntryProcessor<String, SessionState>, EntryBackupProcessor<String, SessionState>, IdentifiedDataSerializable {
	
	private static final long serialVersionUID = 1356725895004395219L;
	
	private Instant lastAccessedTime;
	
	public SetLATEntryProcessor() {
    	this(null);
    }

    public SetLATEntryProcessor(Instant lastAccessedTime) {
    	this.lastAccessedTime = lastAccessedTime;
    }


    @Override
    public int getFactoryId() {
        return SpringSessionDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return SpringSessionDataSerializerHook.SET_LAT;
    }

    @Override
    public Boolean process(Map.Entry<String, SessionState> entry) {
        SessionState sessionState = entry.getValue();
        if (sessionState == null) {
            sessionState = new SessionState();
        }
        sessionState.setLastAccessedTime(this.lastAccessedTime);
        entry.setValue(sessionState);
        return Boolean.TRUE;
    }

    @Override
    public EntryBackupProcessor<String, SessionState> getBackupProcessor() {
        return this;
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
    	this.lastAccessedTime = in.readObject();
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
    	out.writeObject(lastAccessedTime);
    }

	@Override
	public void processBackup(Entry<String, SessionState> entry) {
		process(entry);
	}
}
