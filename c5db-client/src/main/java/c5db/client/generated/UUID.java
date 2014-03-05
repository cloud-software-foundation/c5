// Generated by http://code.google.com/p/protostuff/ ... DO NOT EDIT!
// Generated from resources

package c5db.client.generated;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.dyuproject.protostuff.GraphIOUtil;
import com.dyuproject.protostuff.Input;
import com.dyuproject.protostuff.Message;
import com.dyuproject.protostuff.Output;
import com.dyuproject.protostuff.Schema;
import com.dyuproject.protostuff.UninitializedMessageException;

public final class UUID implements Externalizable, Message<UUID>, Schema<UUID>
{

    public static Schema<UUID> getSchema()
    {
        return DEFAULT_INSTANCE;
    }

    public static UUID getDefaultInstance()
    {
        return DEFAULT_INSTANCE;
    }

    static final UUID DEFAULT_INSTANCE = new UUID();


    private Long leastSigBits;
    private Long mostSigBits;

    public UUID()
    {

    }

    public UUID(
        Long leastSigBits,
        Long mostSigBits
    )
    {
        this.leastSigBits = leastSigBits;
        this.mostSigBits = mostSigBits;
    }

    @Override
    public String toString() {
        return "UUID{" +
                    "leastSigBits=" + leastSigBits +
                    ", mostSigBits=" + mostSigBits +
                '}';
    }
    // getters and setters

    // leastSigBits

    public Long getLeastSigBits()
    {
        return leastSigBits;
    }


    public UUID setLeastSigBits(Long leastSigBits)
    {
        this.leastSigBits = leastSigBits;
        return this;
    }

    // mostSigBits

    public Long getMostSigBits()
    {
        return mostSigBits;
    }


    public UUID setMostSigBits(Long mostSigBits)
    {
        this.mostSigBits = mostSigBits;
        return this;
    }

    // java serialization

    public void readExternal(ObjectInput in) throws IOException
    {
        GraphIOUtil.mergeDelimitedFrom(in, this, this);
    }

    public void writeExternal(ObjectOutput out) throws IOException
    {
        GraphIOUtil.writeDelimitedTo(out, this, this);
    }

    // message method

    public Schema<UUID> cachedSchema()
    {
        return DEFAULT_INSTANCE;
    }

    // schema methods

    public UUID newMessage()
    {
        return new UUID();
    }

    public Class<UUID> typeClass()
    {
        return UUID.class;
    }

    public String messageName()
    {
        return UUID.class.getSimpleName();
    }

    public String messageFullName()
    {
        return UUID.class.getName();
    }

    public boolean isInitialized(UUID message)
    {
        return
            message.leastSigBits != null 
            && message.mostSigBits != null;
    }

    public void mergeFrom(Input input, UUID message) throws IOException
    {
        for(int number = input.readFieldNumber(this);; number = input.readFieldNumber(this))
        {
            switch(number)
            {
                case 0:
                    return;
                case 1:
                    message.leastSigBits = input.readUInt64();
                    break;
                case 2:
                    message.mostSigBits = input.readUInt64();
                    break;
                default:
                    input.handleUnknownField(number, this);
            }
        }
    }


    public void writeTo(Output output, UUID message) throws IOException
    {
        if(message.leastSigBits == null)
            throw new UninitializedMessageException(message);
        output.writeUInt64(1, message.leastSigBits, false);

        if(message.mostSigBits == null)
            throw new UninitializedMessageException(message);
        output.writeUInt64(2, message.mostSigBits, false);
    }

    public String getFieldName(int number)
    {
        switch(number)
        {
            case 1: return "leastSigBits";
            case 2: return "mostSigBits";
            default: return null;
        }
    }

    public int getFieldNumber(String name)
    {
        final Integer number = __fieldMap.get(name);
        return number == null ? 0 : number.intValue();
    }

    private static final java.util.HashMap<String,Integer> __fieldMap = new java.util.HashMap<String,Integer>();
    static
    {
        __fieldMap.put("leastSigBits", 1);
        __fieldMap.put("mostSigBits", 2);
    }

}
