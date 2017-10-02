package org.springframework.session.hazelcast.entryprocessor;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public class SpringSessionDataSerializerHook implements DataSerializerHook {

    /**
     * The constant F_ID.
     */
    public static final int F_ID = FactoryIdHelper.getFactoryId("spring.session.hazlecast", F_ID_OFFSET_WEBMODULE);

    /**
     * The constant SESSION_UPDATE.
     */
    public static final int SESSION_UPDATE = 1;
    /**
     * The constant SESSION_DELETE.
     */
    public static final int SESSION_DELETE = 2;
    /**
     * The constant GET_ATTRIBUTE.
     */
    public static final int GET_ATTRIBUTE = 3;
    /**
     * The constant GET_ATTRIBUTE_NAMES.
     */
    public static final int GET_ATTRIBUTE_NAMES = 4;
    /**
     * The constant GET_SESSION_STATE.
     */
    public static final int GET_SESSION = 5;
    /**
     * The constant SESSION_STATE.
     */
    public static final int SESSION_STATE = 6;
    
    public static final int GET_LAT = 7;
    public static final int SET_LAT = 8;
    public static final int GET_CT = 9;
    public static final int SET_CT = 10;
    public static final int SET_SESSION = 11;
    public static final int DOES_SESSION_EXIST = 12;

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
            case SESSION_UPDATE:
                dataSerializable = new UpdateAttributeEntryProcessor();
                break;
            case SESSION_DELETE:
                dataSerializable = new DeleteSessionEntryProcessor();
                break;
            case GET_ATTRIBUTE:
                dataSerializable = new GetAttributeEntryProcessor();
                break;
            case GET_ATTRIBUTE_NAMES:
                dataSerializable = new GetAttributeNamesEntryProcessor();
                break;
            case GET_SESSION:
                dataSerializable = new GetSessionEntryProcessor();
                break;
            case SESSION_STATE:
                dataSerializable =  new SessionState();
                break;
            case GET_LAT:
                dataSerializable =  new GetLATEntryProcessor();
                break;
            case SET_LAT:
                dataSerializable =  new SetLATEntryProcessor();
                break;
            case GET_CT:
                dataSerializable =  new GetCTEntryProcessor();
                break;
            case SET_CT:
                dataSerializable =  new SetCTEntryProcessor();
                break;
            case SET_SESSION:
                dataSerializable =  new SetSessionEntryProcessor();
                break;
            case DOES_SESSION_EXIST:
                dataSerializable =  new DoesSessionExistEntryProcessor();
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
