/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.aggregation;

import io.druid.collections.bitmap.BitmapFactory;
import io.druid.collections.bitmap.ConciseBitmapFactory;
import io.druid.collections.bitmap.ImmutableBitmap;
import io.druid.segment.LongColumnSelector;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Comparator;

/**
 */
public class LongMinAggregator implements Aggregator,BitsliceAggregator
{
  static final Comparator COMPARATOR = LongSumAggregator.COMPARATOR;
  private static final BitmapFactory ROARING_BITMAP_FACTORY = new ConciseBitmapFactory();
  static long combineValues(Object lhs, Object rhs)
  {
    return Math.min(((Number) lhs).longValue(), ((Number) rhs).longValue());
  }

  private final LongColumnSelector selector;

  private long min;
  private ArrayList<ImmutableBitmap> bitmaps;


  public LongMinAggregator(LongColumnSelector selector)
  {
    this.selector = selector;
    this.bitmaps=selector.getBitslice();
    reset();
  }

  @Override
  public void aggregate()
  {
    min = Math.min(min, selector.get());
  }

  @Override
  public void reset()
  {
    min = Long.MAX_VALUE;
  }

  @Override
  public Object get()
  {
    return min;
  }

  @Override
  public float getFloat()
  {
    return (float) min;
  }

  @Override
  public long getLong()
  {
    return min;
  }

  @Override
  public Aggregator clone()
  {
    return new LongMinAggregator(selector);
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }

  @Override
  public void aggregate(ImmutableBitmap filter) {
    long max=0;

    ImmutableBitmap comparator=filter;
    int row_size=bitmaps.get(0).size();
    for(int i=bitmaps.size()-1;i>0;i--){
      ImmutableBitmap temp = ROARING_BITMAP_FACTORY
              .complement(bitmaps.get(i),row_size);
      temp=temp.intersection(comparator);
      if(!temp.isEmpty()){
        comparator=temp;
        max+=(1<<(i-1));
      }
    }
    String x=Long.toBinaryString(max);
    String xm=Long.toBinaryString(~max);
    xm=xm.substring(xm.length()-x.length());
    min=new BigInteger(xm,2).longValue();
  }
}
