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

import com.google.common.io.ByteSink;
import io.druid.collections.bitmap.BitmapFactory;
import io.druid.collections.bitmap.ImmutableBitmap;
import io.druid.collections.bitmap.MutableBitmap;
import io.druid.java.util.common.logger.Logger;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;

public class BitSliceLongSupplierSerializer implements LongSupplierSerializer {

	List<MutableBitmap> bitslices;
	BitmapFactory factory;
	BitmapSerdeFactory serdeFactory;
	IOPeon ioPeon;
	ByteOrder byteOrder;
	String columnName;
	int offset;
	protected GenericIndexedWriter<ImmutableBitmap> bitmapWriter;
	private static final Logger log = new Logger(BitSliceLongSupplierSerializer.class);

	public BitSliceLongSupplierSerializer(IOPeon ioPeon, ByteOrder byteOrder,
	                                      CompressionFactory.LongEncodingStrategy encoding,
	                                      CompressedObjectStrategy.CompressionStrategy
			                                      compression,BitmapSerdeFactory factory,String
			                                      filebasename) {
		this.serdeFactory=factory;
		this.factory=serdeFactory.getBitmapFactory();
		this.columnName=filebasename;
		this.ioPeon=ioPeon;
		this.byteOrder=byteOrder;
	}

	@Override
	public void open() throws IOException {
		bitslices=new ArrayList<>();
		bitslices.add(factory.makeEmptyMutableBitmap()); //recording value count

		String bmpFilename = String.format("%s.inverted", columnName);
		bitmapWriter = new GenericIndexedWriter<>(
				ioPeon,
				bmpFilename,
				serdeFactory.getObjectStrategy()
		);
		bitmapWriter.open();
		offset=0;
	}

	@Override
	public int size() {
		return (int)bitmapWriter.getSerializedSize();
	}

	@Override
	public void add(long value) throws IOException {
		String y= Long.toBinaryString(value);
		int size=y.length();
		for(int i=size;i>0;i--){
			if(y.charAt(i-1)!='1'){continue;}
			MutableBitmap bitmap;
			try{
				bitmap=bitslices.get(1+size-i);
			} catch (IndexOutOfBoundsException e){
				log.debug("new bit-position created for %d",value);
				bitmap=factory.makeEmptyMutableBitmap();
				bitslices.add(bitmap);
			}
			bitmap.add(offset);
		}
		bitslices.get(0).add(offset);
		offset++;
	}

	public void serializeDone() throws IOException{
		for(MutableBitmap bitmap:bitslices){
			bitmapWriter.write(factory.makeImmutableBitmap(bitmap));
		}
	}

	@Override
	public void closeAndConsolidate(ByteSink consolidatedOut) throws IOException {

	}

	@Override
	public long getSerializedSize() {
		return bitmapWriter.getSerializedSize();
	}

	@Override
	public void writeToChannel(WritableByteChannel channel) throws IOException {
		/*final BitmapIndex bitmapIndex = new BitmapIndexColumnPartSupplier(
				factory,
				GenericIndexed.fromIterable(
						FluentIterable.from(bitslices)
						              .transform(
								              new Function<MutableBitmap, ImmutableBitmap>()
								              {
									              @Override
									              public ImmutableBitmap apply(MutableBitmap i)
									              {
										              return factory.makeImmutableBitmap(i);
									              }
								              }
						              ),
						serdeFactory.getObjectStrategy()
				),
				null
		).get();*/
		bitmapWriter.writeToChannel(channel);
	}

	@Override
	public void close() throws IOException {
		serializeDone();
		bitmapWriter.close();
	}
}
