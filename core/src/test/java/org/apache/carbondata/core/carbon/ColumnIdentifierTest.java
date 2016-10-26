package org.apache.carbondata.core.carbon;

/**
 * Created by rahul on 26/10/16.
 */

import org.apache.carbondata.core.carbon.metadata.datatype.DataType;
import org.junit.*;

import java.util.HashMap;
import java.util.Map;


public class ColumnIdentifierTest {

    static ColumnIdentifier columnIdentifier;
    static Map<String, String> columnProperties;

    @BeforeClass
    public static void setup() {
        columnProperties = new HashMap<String, String>();
        columnProperties.put("key","value");
        columnIdentifier = new ColumnIdentifier("columnId", columnProperties, DataType.INT);
    }

    @Test
    public void hashCodeTest() {
        int res = columnIdentifier.hashCode();
        assert (res == -623419600);
    }

    @Test
    public void equalsTestwithSameObject() {
        Boolean res = columnIdentifier.equals(columnIdentifier);
        assert (res == true);
    }

    @Test
    public void equalsTestwithSimilarObject() {
        ColumnIdentifier columnIdentifierTest = new ColumnIdentifier("columnId", columnProperties, DataType.INT);
        Boolean res = columnIdentifier.equals(columnIdentifierTest);
        assert (res == true);
    }

    @Test
    public void equalsTestwithNullObject() {
        Boolean res = columnIdentifier.equals(null);
        assert (res == false);
    }

    @Test
    public void equalsTestwithStringObject() {
        Boolean res = columnIdentifier.equals("String Object");
        assert (res == false);
    }

    @Test
    public void equalsTestwithNullColumnId() {
        ColumnIdentifier columnIdentifierTest = new ColumnIdentifier(null, columnProperties, DataType.INT);
        Boolean res = columnIdentifierTest.equals(columnIdentifier);
        assert (res == false);
    }

    @Test
    public void equalsTestwithDiffColumnId() {
        ColumnIdentifier columnIdentifierTest = new ColumnIdentifier("diffColumnId", columnProperties, DataType.INT);
        Boolean res = columnIdentifierTest.equals(columnIdentifier);
        assert (res == false);
    }

    @Test
    public void toStringTest(){
        String res = columnIdentifier.toString();
        assert (res.equals("ColumnIdentifier [columnId=columnId]"));
    }

    @Test
    public void getColumnPropertyTest(){
        ColumnIdentifier columnIdentifierTest = new ColumnIdentifier("diffColumnId", null, DataType.INT);
        String res = columnIdentifierTest.getColumnProperty("key");
        assert (res == null);
    }

    @Test
    public void getColumnPropertyTestwithNull(){
        assert (columnIdentifier.getColumnProperty("key").equals("value"));
    }
}
