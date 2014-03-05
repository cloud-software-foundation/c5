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

public final class FamilyFilter implements Externalizable, Message<FamilyFilter>, Schema<FamilyFilter>
{

    public static Schema<FamilyFilter> getSchema()
    {
        return DEFAULT_INSTANCE;
    }

    public static FamilyFilter getDefaultInstance()
    {
        return DEFAULT_INSTANCE;
    }

    static final FamilyFilter DEFAULT_INSTANCE = new FamilyFilter();


    private CompareFilter compareFilter;

    public FamilyFilter()
    {

    }

    public FamilyFilter(
        CompareFilter compareFilter
    )
    {
        this.compareFilter = compareFilter;
    }

    @Override
    public String toString() {
        return "FamilyFilter{" +
                    "compareFilter=" + compareFilter +
                '}';
    }
    // getters and setters

    // compareFilter

    public CompareFilter getCompareFilter()
    {
        return compareFilter;
    }


    public FamilyFilter setCompareFilter(CompareFilter compareFilter)
    {
        this.compareFilter = compareFilter;
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

    public Schema<FamilyFilter> cachedSchema()
    {
        return DEFAULT_INSTANCE;
    }

    // schema methods

    public FamilyFilter newMessage()
    {
        return new FamilyFilter();
    }

    public Class<FamilyFilter> typeClass()
    {
        return FamilyFilter.class;
    }

    public String messageName()
    {
        return FamilyFilter.class.getSimpleName();
    }

    public String messageFullName()
    {
        return FamilyFilter.class.getName();
    }

    public boolean isInitialized(FamilyFilter message)
    {
        return
            message.compareFilter != null;
    }

    public void mergeFrom(Input input, FamilyFilter message) throws IOException
    {
        for(int number = input.readFieldNumber(this);; number = input.readFieldNumber(this))
        {
            switch(number)
            {
                case 0:
                    return;
                case 1:
                    message.compareFilter = input.mergeObject(message.compareFilter, CompareFilter.getSchema());
                    break;

                default:
                    input.handleUnknownField(number, this);
            }
        }
    }


    public void writeTo(Output output, FamilyFilter message) throws IOException
    {
        if(message.compareFilter == null)
            throw new UninitializedMessageException(message);
        output.writeObject(1, message.compareFilter, CompareFilter.getSchema(), false);

    }

    public String getFieldName(int number)
    {
        switch(number)
        {
            case 1: return "compareFilter";
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
        __fieldMap.put("compareFilter", 1);
    }

}
