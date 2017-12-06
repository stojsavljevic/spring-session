package org.springframework.session.hazelcast.ep;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public class SpringSessionDataSerializerHook implements DataSerializerHook {

    /**
     * The constant F_ID.
     */
	public static final int F_ID = FactoryIdHelper.getFactoryId("spring.session.hazlecast", 789);

    /**
     * The constant SESSION_UPDATE.
     */
	public static final int SESSION_UPDATE_EP = 1;
	public static final int SESSION_STATE = 2;
	public static final int GET_SESSION_STATE = 3;

    @Override
    public DataSerializableFactory createFactory() {
        return new DataSerializableFactory() {
            @Override
            public IdentifiedDataSerializable create(final int typeId) {
                return getIdentifiedDataSerializable(typeId);
            }
        };
    }

    private IdentifiedDataSerializable getIdentifiedDataSerializable(int typeId) {
        IdentifiedDataSerializable dataSerializable;
        switch (typeId) {
			case SESSION_UPDATE_EP:
				dataSerializable = new SessionUpdateEntryProcessor();
                break;
			case SESSION_STATE:
				dataSerializable = new SessionState();
                break;
			case GET_SESSION_STATE:
				dataSerializable = new GetSessionEntryProcessor();
				break;
            default:
                dataSerializable = null;
        }
        return dataSerializable;
    }

    @Override
    public int getFactoryId() {
        return F_ID;
    }
}
