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

public final class BinaryComparator implements Externalizable, Message<BinaryComparator>, Schema<BinaryComparator>
{

    public static Schema<BinaryComparator> getSchema()
    {
        return DEFAULT_INSTANCE;
    }

    public static BinaryComparator getDefaultInstance()
    {
        return DEFAULT_INSTANCE;
    }

    static final BinaryComparator DEFAULT_INSTANCE = new BinaryComparator();


    private ByteArrayComparable comparable;

    public BinaryComparator()
    {

    }

    public BinaryComparator(
        ByteArrayComparable comparable
    )
    {
        this.comparable = comparable;
    }

    @Override
    public String toString() {
        return "BinaryComparator{" +
                    "comparable=" + comparable +
                '}';
    }
    // getters and setters

    // comparable

    public ByteArrayComparable getComparable()
    {
        return comparable;
    }


    public BinaryComparator setComparable(ByteArrayComparable comparable)
    {
        this.comparable = comparable;
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

    public Schema<BinaryComparator> cachedSchema()
    {
        return DEFAULT_INSTANCE;
    }

    // schema methods

    public BinaryComparator newMessage()
    {
        return new BinaryComparator();
    }

    public Class<BinaryComparator> typeClass()
    {
        return BinaryComparator.class;
    }

    public String messageName()
    {
        return BinaryComparator.class.getSimpleName();
    }

    public String messageFullName()
    {
        return BinaryComparator.class.getName();
    }

    public boolean isInitialized(BinaryComparator message)
    {
        return
            message.comparable != null;
    }

    public void mergeFrom(Input input, BinaryComparator message) throws IOException
    {
        for(int number = input.readFieldNumber(this);; number = input.readFieldNumber(this))
        {
            switch(number)
            {
                case 0:
                    return;
                case 1:
                    message.comparable = input.mergeObject(message.comparable, ByteArrayComparable.getSchema());
                    break;

                default:
                    input.handleUnknownField(number, this);
            }
        }
    }


    public void writeTo(Output output, BinaryComparator message) throws IOException
    {
        if(message.comparable == null)
            throw new UninitializedMessageException(message);
        output.writeObject(1, message.comparable, ByteArrayComparable.getSchema(), false);

    }

    public String getFieldName(int number)
    {
        switch(number)
        {
            case 1: return "comparable";
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
        __fieldMap.put("comparable", 1);
    }

}
