package org.apache.carbondata.core.cache.dictionary;

import org.junit.*;

public class DictionaryByteArrayWrapperTest {

    DictionaryByteArrayWrapper dictionaryByteArrayWrapper;

    @Before
    public void setup() {
        byte[] data = "Rahul".getBytes();
        dictionaryByteArrayWrapper = new DictionaryByteArrayWrapper(data);
    }

    @Test
    public void equalsTestWithString() {
        Boolean res = dictionaryByteArrayWrapper.equals("Rahul");
        assert (!res);
    }

    @Test
    public void equalsTestWithDictionaryByteArrayWrapper() {
        Boolean res = dictionaryByteArrayWrapper.equals(new DictionaryByteArrayWrapper("Rahul".getBytes()));
        assert (res);
    }

    @Test
    public void equalsTestWithDifferentLength() {
        Boolean res = dictionaryByteArrayWrapper.equals(new DictionaryByteArrayWrapper("Rahul ".getBytes()));
        assert (!res);
    }

    @Test
    public void hashCodeTest(){
        int res = dictionaryByteArrayWrapper.hashCode();
        assert (res == -967077647);
    }

}
