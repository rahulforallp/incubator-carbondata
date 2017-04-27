/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.carbondata.spark.readsupport;

import java.io.IOException;

import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.hadoop.readsupport.impl.DictionaryDecodeReadSupport;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;

public class SparkRowReadSupportImpl extends DictionaryDecodeReadSupport<InternalRow> {

  @Override public void initialize(CarbonColumn[] carbonColumns,
      AbsoluteTableIdentifier absoluteTableIdentifier) throws IOException {
    super.initialize(carbonColumns, absoluteTableIdentifier);
    //can initialize and generate schema here.
  }

  @Override public InternalRow readRow(Object[] data) {
    for (int i = 0; i < dictionaries.length; i++) {
      if (data[i] == null) {
        continue;
      }
      if (dictionaries[i] == null) {
        if (dataTypes[i].equals(DataType.INT)) {
          data[i] = ((Long)(data[i])).intValue();
        } else if (dataTypes[i].equals(DataType.SHORT)) {
          data[i] = ((Long)(data[i])).shortValue();
        }
        else if (dataTypes[i].equals(DataType.ARRAY)){
          if (((CarbonDimension) carbonColumns[i]).getListOfChildDimensions().get(0).getColumnSchema().getDataType() == DataType.LONG && ((GenericArrayData) data[i]).array()[0] instanceof Integer){
            Long[] longData = new Long[((GenericArrayData) data[i]).array().length];
            int j =0 ;
            for (Object object : ((GenericArrayData) data[i]).array()) {
              longData[j] = ((Number) object).longValue();
              j++;
            }
            data[i]=ArrayData.toArrayData(longData);
          }
        }
      }
    }
    return new GenericInternalRow(data);
  }
}
