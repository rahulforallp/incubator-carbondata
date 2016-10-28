package org.apache.carbondata.core.carbon.datastore.block;

import org.junit.*;

public class BlockInfoTest {

    static BlockInfo blockInfo;

    @BeforeClass
    public static void setup() {
        blockInfo = new BlockInfo(new TableBlockInfo("filePath", 6, "segmentId", null, 6));
    }

    @Test
    public void hashCodeTest() {
        int res = blockInfo.hashCode();
        assert (res == -520590451);
    }

    @Test
    public void equalsTestwithSameObject() {
        Boolean res = blockInfo.equals(blockInfo);
        assert (res);
    }

    @Test
    public void equalsTestWithSimilarObject() {
        BlockInfo blockInfoTest = new BlockInfo(new TableBlockInfo("filePath", 6, "segmentId", null, 6));
        Boolean res = blockInfo.equals(blockInfoTest);
        assert (res);
    }

    @Test
    public void equalsTestWithNullObject() {
        Boolean res = blockInfo.equals(null);
        assert (!res);
    }

    @Test
    public void equalsTestWithStringObject() {
        Boolean res = blockInfo.equals("dummy");
        assert (!res);
    }

    @Test
    public void equalsTestWithDifferentSegmentId() {
        BlockInfo blockInfoTest = new BlockInfo(new TableBlockInfo("filePath", 6, "diffSegmentId", null, 6));
        Boolean res = blockInfo.equals(blockInfoTest);
        assert (!res);
    }

    @Test
    public void equalsTestWithDifferentOffset() {
        BlockInfo blockInfoTest = new BlockInfo(new TableBlockInfo("filePath", 62, "segmentId", null, 6));
        Boolean res = blockInfo.equals(blockInfoTest);
        assert (!res );
    }

    @Test
    public void equalsTestWithDifferentBlockLength() {
        BlockInfo blockInfoTest = new BlockInfo(new TableBlockInfo("filePath", 6, "segmentId", null, 62));
        Boolean res = blockInfo.equals(blockInfoTest);
        assert (!res);
    }

    @Test
    public void equalsTestWithDiffFilePath() {
        BlockInfo blockInfoTest = new BlockInfo(new TableBlockInfo("diffFilePath", 6, "segmentId", null, 62));
        Boolean res = blockInfoTest.equals(blockInfo);
        assert (!res);
    }
}
