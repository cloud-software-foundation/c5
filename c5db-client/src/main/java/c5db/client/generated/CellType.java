// Generated by http://code.google.com/p/protostuff/ ... DO NOT EDIT!
// Generated from resources

package c5db.client.generated;

public enum CellType implements com.dyuproject.protostuff.EnumLite<CellType>
{
    MINIMUM(0),
    PUT(4),
    DELETE(8),
    DELETE_COLUMN(12),
    DELETE_FAMILY(14),
    MAXIMUM(255);

    public final int number;

    private CellType (int number)
    {
        this.number = number;
    }

    public int getNumber()
    {
        return number;
    }

    public static CellType valueOf(int number)
    {
        switch(number)
        {
            case 0: return MINIMUM;
            case 4: return PUT;
            case 8: return DELETE;
            case 12: return DELETE_COLUMN;
            case 14: return DELETE_FAMILY;
            case 255: return MAXIMUM;
            default: return null;
        }
    }
}
