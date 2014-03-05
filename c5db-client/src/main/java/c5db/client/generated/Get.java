// Generated by http://code.google.com/p/protostuff/ ... DO NOT EDIT!
// Generated from resources

package c5db.client.generated;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

import com.dyuproject.protostuff.ByteString;import com.dyuproject.protostuff.GraphIOUtil;
import com.dyuproject.protostuff.Input;
import com.dyuproject.protostuff.Message;
import com.dyuproject.protostuff.Output;
import com.dyuproject.protostuff.Schema;
import com.dyuproject.protostuff.UninitializedMessageException;

public final class Get implements Externalizable, Message<Get>, Schema<Get>
{

    public static Schema<Get> getSchema()
    {
        return DEFAULT_INSTANCE;
    }

    public static Get getDefaultInstance()
    {
        return DEFAULT_INSTANCE;
    }

    static final Get DEFAULT_INSTANCE = new Get();

    static final Integer DEFAULT_MAX_VERSIONS = new Integer(1);
    static final Boolean DEFAULT_CACHE_BLOCKS = new Boolean(true);
    static final Boolean DEFAULT_EXISTENCE_ONLY = new Boolean(false);
    static final Boolean DEFAULT_CLOSEST_ROW_BEFORE = new Boolean(false);

    private ByteString row;
    private List<Column> column;
    private List<NameBytesPair> attribute;
    private Filter filter;
    private TimeRange timeRange;
    private Integer maxVersions = DEFAULT_MAX_VERSIONS;
    private Boolean cacheBlocks = DEFAULT_CACHE_BLOCKS;
    private Integer storeLimit;
    private Integer storeOffset;
    private Boolean existenceOnly = DEFAULT_EXISTENCE_ONLY;
    private Boolean closestRowBefore = DEFAULT_CLOSEST_ROW_BEFORE;

    public Get()
    {

    }

    public Get(
        ByteString row
    )
    {
        this.row = row;
    }

    @Override
    public String toString() {
        return "Get{" +
                    "row=" + row +
                    ", column=" + column +
                    ", attribute=" + attribute +
                    ", filter=" + filter +
                    ", timeRange=" + timeRange +
                    ", maxVersions=" + maxVersions +
                    ", cacheBlocks=" + cacheBlocks +
                    ", storeLimit=" + storeLimit +
                    ", storeOffset=" + storeOffset +
                    ", existenceOnly=" + existenceOnly +
                    ", closestRowBefore=" + closestRowBefore +
                '}';
    }
    // getters and setters

    // row

    public ByteString getRow()
    {
        return row;
    }


    public Get setRow(ByteString row)
    {
        this.row = row;
        return this;
    }

    // column

    public List<Column> getColumnList()
    {
        return column;
    }


    public Get setColumnList(List<Column> column)
    {
        this.column = column;
        return this;
    }

    // attribute

    public List<NameBytesPair> getAttributeList()
    {
        return attribute;
    }


    public Get setAttributeList(List<NameBytesPair> attribute)
    {
        this.attribute = attribute;
        return this;
    }

    // filter

    public Filter getFilter()
    {
        return filter;
    }


    public Get setFilter(Filter filter)
    {
        this.filter = filter;
        return this;
    }

    // timeRange

    public TimeRange getTimeRange()
    {
        return timeRange;
    }


    public Get setTimeRange(TimeRange timeRange)
    {
        this.timeRange = timeRange;
        return this;
    }

    // maxVersions

    public Integer getMaxVersions()
    {
        return maxVersions;
    }


    public Get setMaxVersions(Integer maxVersions)
    {
        this.maxVersions = maxVersions;
        return this;
    }

    // cacheBlocks

    public Boolean getCacheBlocks()
    {
        return cacheBlocks;
    }


    public Get setCacheBlocks(Boolean cacheBlocks)
    {
        this.cacheBlocks = cacheBlocks;
        return this;
    }

    // storeLimit

    public Integer getStoreLimit()
    {
        return storeLimit;
    }


    public Get setStoreLimit(Integer storeLimit)
    {
        this.storeLimit = storeLimit;
        return this;
    }

    // storeOffset

    public Integer getStoreOffset()
    {
        return storeOffset;
    }


    public Get setStoreOffset(Integer storeOffset)
    {
        this.storeOffset = storeOffset;
        return this;
    }

    // existenceOnly

    public Boolean getExistenceOnly()
    {
        return existenceOnly;
    }


    public Get setExistenceOnly(Boolean existenceOnly)
    {
        this.existenceOnly = existenceOnly;
        return this;
    }

    // closestRowBefore

    public Boolean getClosestRowBefore()
    {
        return closestRowBefore;
    }


    public Get setClosestRowBefore(Boolean closestRowBefore)
    {
        this.closestRowBefore = closestRowBefore;
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

    public Schema<Get> cachedSchema()
    {
        return DEFAULT_INSTANCE;
    }

    // schema methods

    public Get newMessage()
    {
        return new Get();
    }

    public Class<Get> typeClass()
    {
        return Get.class;
    }

    public String messageName()
    {
        return Get.class.getSimpleName();
    }

    public String messageFullName()
    {
        return Get.class.getName();
    }

    public boolean isInitialized(Get message)
    {
        return
            message.row != null;
    }

    public void mergeFrom(Input input, Get message) throws IOException
    {
        try {
        for(int number = input.readFieldNumber(this);; number = input.readFieldNumber(this))
        {
            switch(number)
            {
                case 0:
                    return;
                case 1:
                    message.row = input.readBytes();
                    break;
                case 2:
                    if(message.column == null)
                        message.column = new ArrayList<Column>();
                    message.column.add(input.mergeObject(null, Column.getSchema()));
                    break;

                case 3:
                    if(message.attribute == null)
                        message.attribute = new ArrayList<NameBytesPair>();
                    message.attribute.add(input.mergeObject(null, NameBytesPair.getSchema()));
                    break;

                case 4:
                    message.filter = input.mergeObject(message.filter, Filter.getSchema());
                    break;

                case 5:
                    message.timeRange = input.mergeObject(message.timeRange, TimeRange.getSchema());
                    break;

                case 6:
                    message.maxVersions = input.readUInt32();
                    break;
                case 7:
                    message.cacheBlocks = input.readBool();
                    break;
                case 8:
                    message.storeLimit = input.readUInt32();
                    break;
                case 9:
                    message.storeOffset = input.readUInt32();
                    break;
                case 10:
                    message.existenceOnly = input.readBool();
                    break;
                case 11:
                    message.closestRowBefore = input.readBool();
                    break;
                default:
                    input.handleUnknownField(number, this);
            }
        }
        } finally {
        if (message.column != null)
            message.column = java.util.Collections.unmodifiableList(message.column);
        else
            message.column = java.util.Collections.emptyList();
        if (message.attribute != null)
            message.attribute = java.util.Collections.unmodifiableList(message.attribute);
        else
            message.attribute = java.util.Collections.emptyList();
        }
    }


    public void writeTo(Output output, Get message) throws IOException
    {
        if(message.row == null)
            throw new UninitializedMessageException(message);
        output.writeBytes(1, message.row, false);

        if(message.column != null)
        {
            for(Column column : message.column)
            {
                if(column != null)
                    output.writeObject(2, column, Column.getSchema(), true);
            }
        }


        if(message.attribute != null)
        {
            for(NameBytesPair attribute : message.attribute)
            {
                if(attribute != null)
                    output.writeObject(3, attribute, NameBytesPair.getSchema(), true);
            }
        }


        if(message.filter != null)
             output.writeObject(4, message.filter, Filter.getSchema(), false);


        if(message.timeRange != null)
             output.writeObject(5, message.timeRange, TimeRange.getSchema(), false);


        if(message.maxVersions != null && message.maxVersions != DEFAULT_MAX_VERSIONS)
            output.writeUInt32(6, message.maxVersions, false);

        if(message.cacheBlocks != null && message.cacheBlocks != DEFAULT_CACHE_BLOCKS)
            output.writeBool(7, message.cacheBlocks, false);

        if(message.storeLimit != null)
            output.writeUInt32(8, message.storeLimit, false);

        if(message.storeOffset != null)
            output.writeUInt32(9, message.storeOffset, false);

        if(message.existenceOnly != null && message.existenceOnly != DEFAULT_EXISTENCE_ONLY)
            output.writeBool(10, message.existenceOnly, false);

        if(message.closestRowBefore != null && message.closestRowBefore != DEFAULT_CLOSEST_ROW_BEFORE)
            output.writeBool(11, message.closestRowBefore, false);
    }

    public String getFieldName(int number)
    {
        switch(number)
        {
            case 1: return "row";
            case 2: return "column";
            case 3: return "attribute";
            case 4: return "filter";
            case 5: return "timeRange";
            case 6: return "maxVersions";
            case 7: return "cacheBlocks";
            case 8: return "storeLimit";
            case 9: return "storeOffset";
            case 10: return "existenceOnly";
            case 11: return "closestRowBefore";
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
        __fieldMap.put("row", 1);
        __fieldMap.put("column", 2);
        __fieldMap.put("attribute", 3);
        __fieldMap.put("filter", 4);
        __fieldMap.put("timeRange", 5);
        __fieldMap.put("maxVersions", 6);
        __fieldMap.put("cacheBlocks", 7);
        __fieldMap.put("storeLimit", 8);
        __fieldMap.put("storeOffset", 9);
        __fieldMap.put("existenceOnly", 10);
        __fieldMap.put("closestRowBefore", 11);
    }

}
