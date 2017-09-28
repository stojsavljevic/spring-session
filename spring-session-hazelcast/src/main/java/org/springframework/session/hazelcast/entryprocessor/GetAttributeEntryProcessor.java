package org.springframework.session.hazelcast.entryprocessor;

import java.io.IOException;
import java.util.Map;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public class GetAttributeEntryProcessor implements EntryProcessor<String, SessionState>, IdentifiedDataSerializable {

	private static final long serialVersionUID = -504363582246966762L;
	
	String attributeName;

    public GetAttributeEntryProcessor(String attributeName) {
        this.attributeName = attributeName;
    }

    public GetAttributeEntryProcessor() {
        this(null);
    }

    @Override
    public int getFactoryId() {
        return FactoryIdHelper.getFactoryId(FactoryIdHelper.WEB_DS_FACTORY, DataSerializerHook.F_ID_OFFSET_WEBMODULE);
    }

    @Override
    public int getId() {
        return 3;
    }

    @Override
    public Data process(Map.Entry<String, SessionState> entry) {
        SessionState sessionState = entry.getValue();
        if (sessionState == null) {
            return null;
        }
        entry.setValue(sessionState);
        return sessionState.getAttributes().get(attributeName);
    }

    @Override
    public EntryBackupProcessor<String, SessionState> getBackupProcessor() {
        return null;
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        attributeName = in.readUTF();
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(attributeName);
    }
}
