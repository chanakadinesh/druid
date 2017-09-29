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

package io.druid.segment.data;

import com.google.common.collect.Lists;
import io.druid.collections.bitmap.ImmutableBitmap;
import io.druid.java.util.common.logger.Logger;


import java.util.ArrayList;

public class BitSliceIndexedLongs implements IndexedLongs {

	private static final Logger log = new Logger(BitSliceIndexedLongs.class);
	ArrayList<ImmutableBitmap> bitmaps;

	public BitSliceIndexedLongs(GenericIndexed<ImmutableBitmap> bitmaps) {
		this.bitmaps= Lists.newArrayList(bitmaps.iterator());
		log.debug("BitsliceIndexedLongs: bitslices : %d",bitmaps.size());
	}

	@Override
	public int size() {
		return bitmaps.get(0).size();
	}

	@Override
	public long get(int index) {
		long num=0;
		for(int i=1;i<bitmaps.size();i++){
			num+=(bitmaps.get(i).get(index))?1<<(i-1):0;
		}
		return num;
	}

	@Override
	public void fill(int index, long[] toFill) {
		throw new UnsupportedOperationException("Does not support for immutable bitmaps");
	}

	@Override
	public void close() {
	}

	public ArrayList<ImmutableBitmap> getBitslices(){
		return bitmaps;
	}
}
