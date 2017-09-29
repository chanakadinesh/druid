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

package io.druid.segment.serde;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.collections.bitmap.ImmutableBitmap;
import io.druid.java.util.common.logger.Logger;
import io.druid.segment.LongColumnSerializer;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.column.ColumnConfig;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.BitSliceIndexedLongs;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.CompressedLongsIndexedSupplier;
import io.druid.segment.data.GenericIndexed;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

/**
 */
public class LongGenericColumnPartSerde implements ColumnPartSerde
{
  @JsonCreator
  public static LongGenericColumnPartSerde createDeserializer(
      @JsonProperty("byteOrder") ByteOrder byteOrder,
      @JsonProperty("serdeFactory") BitmapSerdeFactory serdeFactory
  )
  {
    return new LongGenericColumnPartSerde(byteOrder, null,serdeFactory);
  }

  private final ByteOrder byteOrder;
  private Serializer serializer;
  private final BitmapSerdeFactory serdeFactory;

  private LongGenericColumnPartSerde(ByteOrder byteOrder, Serializer serializer,
                                     BitmapSerdeFactory serdeFactory)
  {
    this.byteOrder = byteOrder;
    this.serializer = serializer;
    this.serdeFactory=serdeFactory;
  }

  /*private LongGenericColumnPartSerde(ByteOrder byteOrder, Serializer serializer)
  {
    this.byteOrder = byteOrder;
    this.serializer = serializer;
    this.serdeFactory=null;
  }*/

  @JsonProperty
  public BitmapSerdeFactory getSerdeFactory(){return serdeFactory;}

  @JsonProperty
  public ByteOrder getByteOrder()
  {
    return byteOrder;
  }

  public static SerializerBuilder serializerBuilder()
  {
    return new SerializerBuilder();
  }

  public static class SerializerBuilder
  {
    private ByteOrder byteOrder = null;
    private LongColumnSerializer delegate = null;
    private BitmapSerdeFactory serdeFactory=null;

    public SerializerBuilder withByteOrder(final ByteOrder byteOrder)
    {
      this.byteOrder = byteOrder;
      return this;
    }

    public SerializerBuilder withDelegate(final LongColumnSerializer delegate)
    {
      this.delegate = delegate;
      return this;
    }

    public SerializerBuilder withSerdeFactory(final BitmapSerdeFactory serdeFactory){
      this.serdeFactory=serdeFactory;
      return this;
    }

    public LongGenericColumnPartSerde build()
    {
      return new LongGenericColumnPartSerde(
          byteOrder, new Serializer()
      {
        @Override
        public long numBytes()
        {
          return delegate.getSerializedSize();
        }

        @Override
        public void write(WritableByteChannel channel) throws IOException
        {
          delegate.writeToChannel(channel);
        }
      }, serdeFactory
      );
    }
  }

  public static LegacySerializerBuilder legacySerializerBuilder()
  {
    return new LegacySerializerBuilder();
  }

  public static class LegacySerializerBuilder
  {
    private ByteOrder byteOrder = null;
    private CompressedLongsIndexedSupplier delegate = null;

    public LegacySerializerBuilder withByteOrder(final ByteOrder byteOrder)
    {
      this.byteOrder = byteOrder;
      return this;
    }

    public LegacySerializerBuilder withDelegate(final CompressedLongsIndexedSupplier delegate)
    {
      this.delegate = delegate;
      return this;
    }

    public LongGenericColumnPartSerde build()
    {
      return new LongGenericColumnPartSerde(
          byteOrder, new Serializer()
      {
        @Override
        public long numBytes()
        {
          return delegate.getSerializedSize();
        }

        @Override
        public void write(WritableByteChannel channel) throws IOException
        {
          delegate.writeToChannel(channel);
        }
      },null
      );
    }
  }

  @Override
  public Serializer getSerializer()
  {
    return serializer;
  }

  @Override
  public Deserializer getDeserializer()
  {
    return new Deserializer()
    {
      private final Logger log = new Logger(LongGenericColumnPartSerde.class);
      @Override
      public void read(ByteBuffer buffer, ColumnBuilder builder, ColumnConfig columnConfig)
      {
        if(serdeFactory==null) {
          final CompressedLongsIndexedSupplier column = CompressedLongsIndexedSupplier.fromByteBuffer(
              buffer,
              byteOrder
          );
          builder.setType(ValueType.LONG)
                 .setHasMultipleValues(false)
                 .setGenericColumn(new LongGenericColumnSupplier(column));
        }else {
          GenericIndexed<ImmutableBitmap> bitmaps = GenericIndexed.read(buffer, serdeFactory.getObjectStrategy());
          log.debug("bitmap loaded size : %d",bitmaps.size());
          log.debug("bitmap loaded 0 posission: %d",bitmaps.get(0).size());
          builder.setType(ValueType.LONG)
                 .setHasMultipleValues(false)
                 .setGenericColumn(new LongGenericColumnSupplier(new BitSliceIndexedLongs(bitmaps)));
        }
      }
    };
  }
}
