/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.mpp.aggregation;

import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import static com.google.common.base.Preconditions.checkArgument;

public class MinValueAccumulator implements Accumulator {

  private TSDataType seriesDataType;
  private TsPrimitiveType minResult;
  private boolean initResult = false;

  public MinValueAccumulator(TSDataType seriesDataType) {
    this.seriesDataType = seriesDataType;
    this.minResult = TsPrimitiveType.getByType(seriesDataType);
  }

  // Column should be like: | Time | Value |
  @Override
  public void addInput(Column[] column, TimeRange timeRange) {
    switch (seriesDataType) {
      case INT32:
        addIntInput(column, timeRange);
        break;
      case INT64:
        addLongInput(column, timeRange);
        break;
      case FLOAT:
        addFloatInput(column, timeRange);
        break;
      case DOUBLE:
        addDoubleInput(column, timeRange);
        break;
      case TEXT:
      case BOOLEAN:
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in MinValue: %s", seriesDataType));
    }
  }

  // partialResult should be like: | partialMinValue1 |
  @Override
  public void addIntermediate(Column[] partialResult) {
    checkArgument(partialResult.length == 1, "partialResult of MinValue should be 1");
    switch (seriesDataType) {
      case INT32:
        updateIntResult(partialResult[0].getInt(0));
        break;
      case INT64:
        updateLongResult(partialResult[0].getLong(0));
        break;
      case FLOAT:
        updateFloatResult(partialResult[0].getFloat(0));
        break;
      case DOUBLE:
        updateDoubleResult(partialResult[0].getDouble(0));
        break;
      case TEXT:
      case BOOLEAN:
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in MinValue: %s", seriesDataType));
    }
  }

  @Override
  public void addStatistics(Statistics statistics) {
    switch (seriesDataType) {
      case INT32:
        updateIntResult((int) statistics.getMinValue());
        break;
      case INT64:
        updateLongResult((long) statistics.getMinValue());
        break;
      case FLOAT:
        updateFloatResult((float) statistics.getMinValue());
        break;
      case DOUBLE:
        updateDoubleResult((double) statistics.getMinValue());
        break;
      case TEXT:
      case BOOLEAN:
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in MinValue: %s", seriesDataType));
    }
  }

  // finalResult should be single column, like: | finalCountValue |
  @Override
  public void setFinal(Column finalResult) {
    minResult.setObject(finalResult.getObject(0));
  }

  // columnBuilder should be single in MinValueAccumulator
  @Override
  public void outputIntermediate(ColumnBuilder[] columnBuilders) {
    checkArgument(columnBuilders.length == 1, "partialResult of MinValue should be 1");
    switch (seriesDataType) {
      case INT32:
        columnBuilders[0].writeInt(minResult.getInt());
        break;
      case INT64:
        columnBuilders[0].writeLong(minResult.getLong());
        break;
      case FLOAT:
        columnBuilders[0].writeFloat(minResult.getFloat());
        break;
      case DOUBLE:
        columnBuilders[0].writeDouble(minResult.getDouble());
        break;
      case TEXT:
      case BOOLEAN:
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in MinValue: %s", seriesDataType));
    }
  }

  @Override
  public void outputFinal(ColumnBuilder columnBuilder) {
    switch (seriesDataType) {
      case INT32:
        columnBuilder.writeInt(minResult.getInt());
        break;
      case INT64:
        columnBuilder.writeLong(minResult.getLong());
        break;
      case FLOAT:
        columnBuilder.writeFloat(minResult.getFloat());
        break;
      case DOUBLE:
        columnBuilder.writeDouble(minResult.getDouble());
        break;
      case TEXT:
      case BOOLEAN:
      default:
        throw new UnSupportedDataTypeException(
            String.format("Unsupported data type in MinValue: %s", seriesDataType));
    }
  }

  @Override
  public void reset() {
    initResult = false;
    this.minResult.reset();
  }

  @Override
  public boolean hasFinalResult() {
    return false;
  }

  @Override
  public TSDataType[] getIntermediateType() {
    return new TSDataType[] {minResult.getDataType()};
  }

  @Override
  public TSDataType getFinalType() {
    return minResult.getDataType();
  }

  private void addIntInput(Column[] column, TimeRange timeRange) {
    TimeColumn timeColumn = (TimeColumn) column[0];
    for (int i = 0; i < timeColumn.getPositionCount(); i++) {
      long curTime = timeColumn.getLong(i);
      if (curTime >= timeRange.getMax() || curTime < timeRange.getMin()) {
        break;
      }
      if (!column[1].isNull(i)) {
        updateIntResult(column[1].getInt(i));
      }
    }
  }

  private void updateIntResult(int minVal) {
    if (!initResult || minVal < minResult.getInt()) {
      initResult = true;
      minResult.setInt(minVal);
    }
  }

  private void addLongInput(Column[] column, TimeRange timeRange) {
    TimeColumn timeColumn = (TimeColumn) column[0];
    for (int i = 0; i < timeColumn.getPositionCount(); i++) {
      long curTime = timeColumn.getLong(i);
      if (curTime >= timeRange.getMax() || curTime < timeRange.getMin()) {
        break;
      }
      if (!column[1].isNull(i)) {
        updateLongResult(column[1].getLong(i));
      }
    }
  }

  private void updateLongResult(long minVal) {
    if (!initResult || minVal < minResult.getLong()) {
      initResult = true;
      minResult.setLong(minVal);
    }
  }

  private void addFloatInput(Column[] column, TimeRange timeRange) {
    TimeColumn timeColumn = (TimeColumn) column[0];
    for (int i = 0; i < timeColumn.getPositionCount(); i++) {
      long curTime = timeColumn.getLong(i);
      if (curTime >= timeRange.getMax() || curTime < timeRange.getMin()) {
        break;
      }
      if (!column[1].isNull(i)) {
        updateFloatResult(column[1].getFloat(i));
      }
    }
  }

  private void updateFloatResult(float minVal) {
    if (!initResult || minVal < minResult.getFloat()) {
      initResult = true;
      minResult.setFloat(minVal);
    }
  }

  private void addDoubleInput(Column[] column, TimeRange timeRange) {
    TimeColumn timeColumn = (TimeColumn) column[0];
    for (int i = 0; i < timeColumn.getPositionCount(); i++) {
      long curTime = timeColumn.getLong(i);
      if (curTime >= timeRange.getMax() || curTime < timeRange.getMin()) {
        break;
      }
      if (!column[1].isNull(i)) {
        updateDoubleResult(column[1].getDouble(i));
      }
    }
  }

  private void updateDoubleResult(double minVal) {
    if (!initResult || minVal < minResult.getDouble()) {
      initResult = true;
      minResult.setDouble(minVal);
    }
  }
}