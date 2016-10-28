package org.apache.carbondata.core.carbon;

/**
 * Created by rahul on 25/10/16.
 */

import org.junit.*;

public class AbsoluteTableIdentifierTest {
    static AbsoluteTableIdentifier absoluteTableIdentifier;
    static AbsoluteTableIdentifier absoluteTableIdentifier1;
    static AbsoluteTableIdentifier absoluteTableIdentifier2;
    static AbsoluteTableIdentifier absoluteTableIdentifier3;
    static AbsoluteTableIdentifier absoluteTableIdentifier4;

    @BeforeClass
    public static void setup() {
        absoluteTableIdentifier = new AbsoluteTableIdentifier("storePath", new CarbonTableIdentifier("databaseName", "tableName", "tableId"));
        absoluteTableIdentifier1 = new AbsoluteTableIdentifier("dummy", null);
        absoluteTableIdentifier2 = new AbsoluteTableIdentifier("dumgfhmy", null);
        absoluteTableIdentifier3 = new AbsoluteTableIdentifier("duhgmmy", new CarbonTableIdentifier("dummy", "dumy", "dmy"));
        absoluteTableIdentifier4 = new AbsoluteTableIdentifier("storePath", new CarbonTableIdentifier("databaseName", "tableName", "tableId"));
    }

    @Test
    public void equalsTestWithSameInstance() {
        Boolean res = absoluteTableIdentifier.equals("wrong data");
        assert (!res);
    }

    @Test
    public void equalsTestWithNullObject() {
        Boolean res = absoluteTableIdentifier.equals(null);
        assert (!res);
    }

    @Test
    public void equalsTestWithotherObject() {
        Boolean res = absoluteTableIdentifier1.equals(absoluteTableIdentifier);
        assert (!res);
    }

    @Test
    public void equalsTestWithSameObj() {
        Boolean res = absoluteTableIdentifier.equals(absoluteTableIdentifier);
        assert (res);
    }

    @Test
    public void equalsTestWithNullColumnIdentifier() {
        Boolean res = absoluteTableIdentifier1.equals(absoluteTableIdentifier2);
        assert (!res);
    }

    @Test
    public void equalsTestWithEqualColumnIdentifier() {
        Boolean res = absoluteTableIdentifier3.equals(absoluteTableIdentifier4);
        assert (!res);
    }

    @Test
    public void equalsTestWithEqualAbsoluteTableIdentifier() {
        Boolean res = absoluteTableIdentifier.equals(absoluteTableIdentifier4);
        assert (res);
    }

    @Test
    public void hashCodeTest() {
        int res = absoluteTableIdentifier4.hashCode();
        assert (res == 804398706);
    }

    @Test
    public void gettablePathTest(){
        String res = absoluteTableIdentifier4.getTablePath();
        assert (res.equals("storePath/databaseName/tableName"));
    }

    @Test
    public void fromTablePathTest(){
       AbsoluteTableIdentifier absoluteTableIdentifierTest = AbsoluteTableIdentifier.fromTablePath("storePath/databaseName/tableName");
        assert (absoluteTableIdentifierTest.getStorePath().equals(absoluteTableIdentifier4.getStorePath()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void fromTablePathWithExceptionTest(){
        AbsoluteTableIdentifier absoluteTableIdentifierTest = AbsoluteTableIdentifier.fromTablePath("storePath/databaseName");
    }
}
