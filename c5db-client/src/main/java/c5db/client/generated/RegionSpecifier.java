// Generated by http://code.google.com/p/protostuff/ ... DO NOT EDIT!
// Generated from resources

package c5db.client.generated;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.dyuproject.protostuff.ByteString;import com.dyuproject.protostuff.GraphIOUtil;
import com.dyuproject.protostuff.Input;
import com.dyuproject.protostuff.Message;
import com.dyuproject.protostuff.Output;
import com.dyuproject.protostuff.Schema;
import com.dyuproject.protostuff.UninitializedMessageException;

public final class RegionSpecifier implements Externalizable, Message<RegionSpecifier>, Schema<RegionSpecifier>
{
    public enum RegionSpecifierType implements com.dyuproject.protostuff.EnumLite<RegionSpecifierType>
    {
        REGION_NAME(1),
        ENCODED_REGION_NAME(2);

        public final int number;

        private RegionSpecifierType (int number)
        {
            this.number = number;
        }

        public int getNumber()
        {
            return number;
        }

        public static RegionSpecifierType valueOf(int number)
        {
            switch(number)
            {
                case 1: return REGION_NAME;
                case 2: return ENCODED_REGION_NAME;
                default: return null;
            }
        }
    }


    public static Schema<RegionSpecifier> getSchema()
    {
        return DEFAULT_INSTANCE;
    }

    public static RegionSpecifier getDefaultInstance()
    {
        return DEFAULT_INSTANCE;
    }

    static final RegionSpecifier DEFAULT_INSTANCE = new RegionSpecifier();


    private RegionSpecifierType type;
    private ByteString value;

    public RegionSpecifier()
    {

    }

    public RegionSpecifier(
        RegionSpecifierType type,
        ByteString value
    )
    {
        this.type = type;
        this.value = value;
    }

    @Override
    public String toString() {
        return "RegionSpecifier{" +
                    "type=" + type +
                    ", value=" + value +
                '}';
    }
    // getters and setters

    // type

    public RegionSpecifierType getType()
    {
        return type;
    }


    public RegionSpecifier setType(RegionSpecifierType type)
    {
        this.type = type;
        return this;
    }

    // value

    public ByteString getValue()
    {
        return value;
    }


    public RegionSpecifier setValue(ByteString value)
    {
        this.value = value;
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

    public Schema<RegionSpecifier> cachedSchema()
    {
        return DEFAULT_INSTANCE;
    }

    // schema methods

    public RegionSpecifier newMessage()
    {
        return new RegionSpecifier();
    }

    public Class<RegionSpecifier> typeClass()
    {
        return RegionSpecifier.class;
    }

    public String messageName()
    {
        return RegionSpecifier.class.getSimpleName();
    }

    public String messageFullName()
    {
        return RegionSpecifier.class.getName();
    }

    public boolean isInitialized(RegionSpecifier message)
    {
        return
            message.type != null 
            && message.value != null;
    }

    public void mergeFrom(Input input, RegionSpecifier message) throws IOException
    {
        for(int number = input.readFieldNumber(this);; number = input.readFieldNumber(this))
        {
            switch(number)
            {
                case 0:
                    return;
                case 1:
                    message.type = RegionSpecifierType.valueOf(input.readEnum());
                    break;

                case 2:
                    message.value = input.readBytes();
                    break;
                default:
                    input.handleUnknownField(number, this);
            }
        }
    }


    public void writeTo(Output output, RegionSpecifier message) throws IOException
    {
        if(message.type == null)
            throw new UninitializedMessageException(message);
        output.writeEnum(1, message.type.number, false);

        if(message.value == null)
            throw new UninitializedMessageException(message);
        output.writeBytes(2, message.value, false);
    }

    public String getFieldName(int number)
    {
        switch(number)
        {
            case 1: return "type";
            case 2: return "value";
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
        __fieldMap.put("type", 1);
        __fieldMap.put("value", 2);
    }

}
