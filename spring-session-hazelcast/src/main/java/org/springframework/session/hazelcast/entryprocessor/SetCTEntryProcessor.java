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

public class SetCTEntryProcessor implements EntryProcessor<String, SessionState>, EntryBackupProcessor<String, SessionState>, IdentifiedDataSerializable {
	
	private static final long serialVersionUID = 1356725895004395219L;
	
	private Instant creationTime;
	
	public SetCTEntryProcessor() {
    	this(null);
    }

    public SetCTEntryProcessor(Instant creationTime) {
    	this.creationTime = creationTime;
    }


    @Override
    public int getFactoryId() {
        return SpringSessionDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return SpringSessionDataSerializerHook.SET_CT;
    }

    @Override
    public Boolean process(Map.Entry<String, SessionState> entry) {
        SessionState sessionState = entry.getValue();
        if (sessionState == null) {
        	sessionState = new SessionState();
        }
        sessionState.setCreationTime(this.creationTime);
        entry.setValue(sessionState);
        return Boolean.TRUE;
    }

    @Override
    public EntryBackupProcessor<String, SessionState> getBackupProcessor() {
        return this;
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
    	this.creationTime = in.readObject();
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
    	out.writeObject(creationTime);
    }

	@Override
	public void processBackup(Entry<String, SessionState> entry) {
		process(entry);
	}
}
