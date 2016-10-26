package org.apache.carbondata.core.carbon;

/**
 * Created by rahul on 26/10/16.
 */

import org.junit.BeforeClass;
import org.junit.Test;

public class CarbonTableIdentifierTest {

    static CarbonTableIdentifier carbonTableIdentifier;
    static CarbonTableIdentifier carbonTableIdentifier2;

    @BeforeClass
    public static void setup() {
        carbonTableIdentifier = new CarbonTableIdentifier("DatabseName", "tableName", "tableId");

    }

    @Test
    public void equalsTestWithSameObject() {
        Boolean res = carbonTableIdentifier.equals(carbonTableIdentifier);
        assert (res == true);
    }

    @Test
    public void equalsTestWithSimilarObject() {
        CarbonTableIdentifier carbonTableIdentifierTest = new CarbonTableIdentifier("DatabseName", "tableName", "tableId");
        Boolean res = carbonTableIdentifier.equals(carbonTableIdentifierTest);
        assert (res == true);
    }

    @Test
    public void equalsTestWithNullrObject() {
        Boolean res = carbonTableIdentifier.equals(carbonTableIdentifier2);
        assert (res == false);
    }

    @Test
    public void equalsTestWithStringrObject() {
        Boolean res = carbonTableIdentifier.equals("different class object");
        assert (res == false);
    }

    @Test
    public void equalsTestWithoutDatabaseName() {
        CarbonTableIdentifier carbonTableIdentifierTest = new CarbonTableIdentifier(null, "tableName", "tableId");
        Boolean res = carbonTableIdentifierTest.equals(carbonTableIdentifier);
        assert (res == false);
    }

    @Test
    public void equalsTestWithoutTableId() {
        CarbonTableIdentifier carbonTableIdentifierTest = new CarbonTableIdentifier("DatabseName", "tableName", null);
        Boolean res = carbonTableIdentifierTest.equals(carbonTableIdentifier);
        assert (res == false);
    }

    @Test
    public void equalsTestWithDifferentTableId() {
        CarbonTableIdentifier carbonTableIdentifierTest = new CarbonTableIdentifier("DatabseName", "tableName", "diffTableId");
        Boolean res = carbonTableIdentifierTest.equals(carbonTableIdentifier);
        assert (res == false);
    }

    @Test
    public void equalsTestWithNullTableName() {
        CarbonTableIdentifier carbonTableIdentifierTest = new CarbonTableIdentifier("DatabseName", null, "tableId");
        Boolean res = carbonTableIdentifierTest.equals(carbonTableIdentifier);
        assert (res == false);
    }

    @Test
    public void equalsTestWithDifferentTableName() {
        CarbonTableIdentifier carbonTableIdentifierTest = new CarbonTableIdentifier("DatabseName", "diffTableName", "tableId");
        Boolean res = carbonTableIdentifierTest.equals(carbonTableIdentifier);
        assert (res == false);
    }

    @Test
    public void toStringTest(){
        String res = carbonTableIdentifier.toString();
        System.out.printf("sfdsdf "+res);
        assert (res.equals("DatabseName_tableName"));
    }
}
