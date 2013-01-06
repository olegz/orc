/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.io.orc;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class RecordReaderImpl implements RecordReader {
  private final FSDataInputStream file;
  private final long firstRow;
  private final List<StripeInformation> stripes =
    new ArrayList<StripeInformation>();
  private final long totalRowCount;
  private final CompressionCodec codec;
  private final int bufferSize;
  private final boolean[] included;
  private int currentStripe = -1;
  private long currentRow = 0;
  private final Map<WriterImpl.StreamName, InStream> streams =
    new HashMap<WriterImpl.StreamName, InStream>();
  private final TreeReader reader;

  RecordReaderImpl(Iterable<StripeInformation> stripes,
                   FileSystem fileSystem,
                   Path path,
                   long offset, long length,
                   List<OrcProto.Type> types,
                   CompressionCodec codec,
                   int bufferSize,
                   boolean[] included
                  ) throws IOException {
    this.file = fileSystem.open(path);
    this.codec = codec;
    this.bufferSize = bufferSize;
    this.included = included;
    long rows = 0;
    long skippedRows = 0;
    for(StripeInformation stripe: stripes) {
      long stripeStart = stripe.getOffset();
      if (offset > stripeStart) {
        skippedRows += stripe.getNumberOfRows();
      } else if (stripeStart < offset+length) {
        this.stripes.add(stripe);
        rows += stripe.getNumberOfRows();
      }
    }
    firstRow = skippedRows;
    totalRowCount = rows;
    reader = createTreeReader(0, types, included);
  }

  private abstract static class TreeReader {
    protected final int columnId;
    private boolean done = true;
    private BitFieldReader present = null;
    protected boolean valuePresent = false;

    TreeReader(int columnId) {
      this.columnId = columnId;
    }

    void startStripe(Map<WriterImpl.StreamName,InStream> streams
                        ) throws IOException {
      WriterImpl.StreamName name =
        new WriterImpl.StreamName(columnId,
          OrcProto.StripeSection.Kind.PRESENT);
      InStream in = streams.get(name);
      if (in == null) {
        present = null;
        valuePresent = true;
        done = false;
      } else {
        present = new BitFieldReader(in, 1);
        done = !present.hasNext();
        if (!done) {
          valuePresent = present.next() == 1;
        }
      }
    }

    abstract void seekToRow(long row) throws IOException;

    boolean hasNext() throws IOException {
      return !done;
    }

    Object next(Object previous) throws IOException {
      if (present != null) {
        done = !present.hasNext();
        if (!done) {
          valuePresent = present.next() == 1;
        }
      }
      return previous;
    }
  }

  private static class BooleanTreeReader extends TreeReader{
    private BitFieldReader reader = null;

    BooleanTreeReader(int columnId) {
      super(columnId);
    }

    @Override
    void startStripe(Map<WriterImpl.StreamName, InStream> streams
    ) throws IOException {
      super.startStripe(streams);
      reader = new BitFieldReader(streams.get(new WriterImpl.StreamName
          (columnId, OrcProto.StripeSection.Kind.DATA)), 1);
    }

    @Override
    boolean hasNext() throws IOException {
      return super.hasNext() && (!valuePresent || reader.hasNext());
    }

    @Override
    void seekToRow(long row) throws IOException {
      //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    Object next(Object previous) throws IOException {
      BooleanWritable result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new BooleanWritable();
        } else {
          result = (BooleanWritable) previous;
        }
        result.set(reader.next() == 1);
      }
      return super.next(result);
    }
  }

  private static class ByteTreeReader extends TreeReader{
    private RunLengthByteReader reader = null;

    ByteTreeReader(int columnId) {
      super(columnId);
    }

    @Override
    void startStripe(Map<WriterImpl.StreamName, InStream> streams
    ) throws IOException {
      super.startStripe(streams);
      reader = new RunLengthByteReader(streams.get(new WriterImpl.StreamName
          (columnId, OrcProto.StripeSection.Kind.DATA)));
    }

    @Override
    boolean hasNext() throws IOException {
      return super.hasNext() && (!valuePresent || reader.hasNext());
    }

    @Override
    void seekToRow(long row) throws IOException {
      //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    Object next(Object previous) throws IOException {
      ByteWritable result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new ByteWritable();
        } else {
          result = (ByteWritable) previous;
        }
        result.set(reader.next());
      }
      return super.next(result);
    }
  }

  private static class ShortTreeReader extends TreeReader{
    private RunLengthIntegerReader reader = null;

    ShortTreeReader(int columnId) {
      super(columnId);
    }

    @Override
    void startStripe(Map<WriterImpl.StreamName, InStream> streams
    ) throws IOException {
      super.startStripe(streams);
      WriterImpl.StreamName name =
          new WriterImpl.StreamName(columnId,
              OrcProto.StripeSection.Kind.DATA);
      reader = new RunLengthIntegerReader(streams.get(name), true);
    }

    @Override
    boolean hasNext() throws IOException {
      return super.hasNext() && (!valuePresent || reader.hasNext());
    }

    @Override
    void seekToRow(long row) throws IOException {
      //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    Object next(Object previous) throws IOException {
      ShortWritable result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new ShortWritable();
        } else {
          result = (ShortWritable) previous;
        }
        result.set((short) reader.next());
      }
      return super.next(result);
    }
  }

  private static class IntTreeReader extends TreeReader{
    private RunLengthIntegerReader reader = null;

    IntTreeReader(int columnId) {
      super(columnId);
    }

    @Override
    void startStripe(Map<WriterImpl.StreamName, InStream> streams
    ) throws IOException {
      super.startStripe(streams);
      WriterImpl.StreamName name =
          new WriterImpl.StreamName(columnId,
              OrcProto.StripeSection.Kind.DATA);
      reader = new RunLengthIntegerReader(streams.get(name), true);
    }

    @Override
    boolean hasNext() throws IOException {
      return super.hasNext() && (!valuePresent || reader.hasNext());
    }

    @Override
    void seekToRow(long row) throws IOException {
      //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    Object next(Object previous) throws IOException {
      IntWritable result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new IntWritable();
        } else {
          result = (IntWritable) previous;
        }
        result.set((int) reader.next());
      }
      return super.next(result);
    }
  }

  private static class LongTreeReader extends TreeReader{
    private RunLengthIntegerReader reader = null;

    LongTreeReader(int columnId) {
      super(columnId);
    }

    @Override
    void startStripe(Map<WriterImpl.StreamName, InStream> streams
    ) throws IOException {
      super.startStripe(streams);
      WriterImpl.StreamName name =
          new WriterImpl.StreamName(columnId,
              OrcProto.StripeSection.Kind.DATA);
      reader = new RunLengthIntegerReader(streams.get(name), true);
    }

    @Override
    boolean hasNext() throws IOException {
      return super.hasNext() && (!valuePresent || reader.hasNext());
    }

    @Override
    void seekToRow(long row) throws IOException {
      //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    Object next(Object previous) throws IOException {
      LongWritable result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new LongWritable();
        } else {
          result = (LongWritable) previous;
        }
        result.set(reader.next());
      }
      return super.next(result);
    }
  }

  private static class FloatTreeReader extends TreeReader{
    private InStream stream;

    FloatTreeReader(int columnId) {
      super(columnId);
    }

    @Override
    void startStripe(Map<WriterImpl.StreamName, InStream> streams
                    ) throws IOException {
      super.startStripe(streams);
      WriterImpl.StreamName name =
        new WriterImpl.StreamName(columnId,
          OrcProto.StripeSection.Kind.DATA);
      stream = streams.get(name);
    }

    @Override
    boolean hasNext() throws IOException {
      return super.hasNext() && (!valuePresent || stream.available() > 0);
    }

    @Override
    void seekToRow(long row) throws IOException {
      //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    Object next(Object previous) throws IOException {
      FloatWritable result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new FloatWritable();
        } else {
          result = (FloatWritable) previous;
        }
        result.set(SerializationUtils.readFloat(stream));
      }
      return super.next(result);
    }
  }

  private static class DoubleTreeReader extends TreeReader{
    private InStream stream;

    DoubleTreeReader(int columnId) {
      super(columnId);
    }

    @Override
    void startStripe(Map<WriterImpl.StreamName, InStream> streams
                    ) throws IOException {
      super.startStripe(streams);
      WriterImpl.StreamName name =
        new WriterImpl.StreamName(columnId,
          OrcProto.StripeSection.Kind.DATA);
      stream = streams.get(name);
    }

    @Override
    boolean hasNext() throws IOException {
      return super.hasNext() && (!valuePresent || stream.available() > 0);
    }

    @Override
    void seekToRow(long row) throws IOException {
      //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    Object next(Object previous) throws IOException {
      DoubleWritable result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new DoubleWritable();
        } else {
          result = (DoubleWritable) previous;
        }
        result.set(SerializationUtils.readDouble(stream));
      }
      return super.next(result);
    }
  }

  private static class BinaryTreeReader extends TreeReader{
    private InStream stream;
    private RunLengthIntegerReader lengths;

    BinaryTreeReader(int columnId) {
      super(columnId);
    }

    @Override
    void startStripe(Map<WriterImpl.StreamName, InStream> streams
    ) throws IOException {
      super.startStripe(streams);
      WriterImpl.StreamName name =
          new WriterImpl.StreamName(columnId,
              OrcProto.StripeSection.Kind.DATA);
      stream = streams.get(name);
      lengths = new RunLengthIntegerReader(streams.get(new
          WriterImpl.StreamName(columnId, OrcProto.StripeSection.Kind.LENGTH)),
          false);
    }

    @Override
    boolean hasNext() throws IOException {
      return super.hasNext() && (!valuePresent || lengths.hasNext());
    }

    @Override
    void seekToRow(long row) throws IOException {
      //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    Object next(Object previous) throws IOException {
      BytesWritable result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new BytesWritable();
        } else {
          result = (BytesWritable) previous;
        }
        int len = (int) lengths.next();
        result.setSize(len);
        int offset = 0;
        while (len > 0) {
          int written = stream.read(result.getBytes(), offset, len);
          if (written < 0) {
            throw new EOFException("Can't finish byte read from " + stream);
          }
          len -= written;
          offset += written;
        }
      }
      return super.next(result);
    }
  }

  private static class TimestampTreeReader extends TreeReader{
    private RunLengthIntegerReader data;
    private RunLengthIntegerReader nanos;

    TimestampTreeReader(int columnId) {
      super(columnId);
    }

    @Override
    void startStripe(Map<WriterImpl.StreamName,
        InStream> streams) throws IOException {
      super.startStripe(streams);
      data = new RunLengthIntegerReader(streams.get(new WriterImpl.StreamName
          (columnId, OrcProto.StripeSection.Kind.DATA)), true);
      nanos = new RunLengthIntegerReader(streams.get(new WriterImpl.StreamName
          (columnId, OrcProto.StripeSection.Kind.NANO_DATA)), false);
    }

    @Override
    boolean hasNext() throws IOException {
      return super.hasNext() && (!valuePresent || data.hasNext());
    }

    @Override
    void seekToRow(long row) throws IOException {
      //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    Object next(Object previous) throws IOException {
      Timestamp result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new Timestamp(0);
        } else {
          result = (Timestamp) previous;
        }
        long seconds = data.next() + WriterImpl.BASE_TIMESTAMP;
        int nanos = parseNanos(this.nanos.next());
        int millis = nanos / 1000*1000;
        result.setTime(seconds * WriterImpl.MILLIS_PER_SECOND + millis);
        result.setNanos(nanos);
      }
      return super.next(result);
    }

    private static int parseNanos(long serialized) {
      int zeros = 3 & (int) serialized;
      int result = (int) serialized >>> 3;
      if (zeros != 0) {
        for(int i =0; i <= zeros; ++i) {
          result *= 10;
        }
      }
      return result;
    }
  }

  private static class StringTreeReader extends TreeReader {
    private DynamicByteArray dictionaryBuffer = null;
    private final DynamicIntArray dictionaryOffsets = new DynamicIntArray();
    private final DynamicIntArray dictionaryLengths = new DynamicIntArray();
    private RunLengthIntegerReader reader;

    StringTreeReader(int columnId) {
      super(columnId);
    }

    @Override
    void startStripe(Map<WriterImpl.StreamName, InStream> streams
                    ) throws IOException {
      super.startStripe(streams);

      // read the dictionary blob
      WriterImpl.StreamName name =
        new WriterImpl.StreamName(columnId,
          OrcProto.StripeSection.Kind.DICTIONARY_DATA);
      InStream in = streams.get(name);
      dictionaryBuffer = new DynamicByteArray(64, in.available());
      dictionaryBuffer.readAll(in);
      in.close();

      // read the lengths
      name = new WriterImpl.StreamName(columnId,
        OrcProto.StripeSection.Kind.LENGTH);
      in = streams.get(name);
      RunLengthIntegerReader lenReader = new RunLengthIntegerReader(in, false);
      int offset = 0;
      dictionaryOffsets.clear();
      dictionaryLengths.clear();
      while (lenReader.hasNext()) {
        dictionaryOffsets.add(offset);
        int len = (int) lenReader.next();
        dictionaryLengths.add(len);
        offset += len;
      }
      in.close();

      // set up the row reader
      name = new WriterImpl.StreamName(columnId,
        OrcProto.StripeSection.Kind.DATA);
      reader = new RunLengthIntegerReader(streams.get(name), false);
    }

    @Override
    void seekToRow(long row) throws IOException {
      //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    boolean hasNext() throws IOException {
      return super.hasNext() && (!valuePresent || reader.hasNext());
    }

    @Override
    Object next(Object previous) throws IOException {
      Text result = null;
      if (valuePresent) {
        int entry = (int) reader.next();
        if (previous == null) {
          result = new Text();
        } else {
          result = (Text) previous;
        }
        int offset = dictionaryOffsets.get(entry);
        int length = dictionaryLengths.get(entry);
        dictionaryBuffer.setText(result, offset, length);
      }
      return super.next(result);
    }
  }

  private static class StructTreeReader extends TreeReader {
    private final TreeReader[] fields;
    private final String[] fieldNames;

    StructTreeReader(int columnId,
                     List<OrcProto.Type> types,
                     boolean[] included) throws IOException {
      super(columnId);
      OrcProto.Type type = types.get(columnId);
      int fieldCount = type.getFieldNamesCount();
      this.fields = new TreeReader[fieldCount];
      this.fieldNames = new String[fieldCount];
      for(int i=0; i < fieldCount; ++i) {
        int subtype = type.getSubtypes(i);
        if (included == null || included[subtype]) {
          this.fields[i] = createTreeReader(subtype, types, included);
        }
        this.fieldNames[i] = type.getFieldNames(i);
      }
    }

    /**
     * Check to make sure that the kids all agree about whether there is another
     * row.
     * @return true if there is another row
     * @throws IOException
     */
    private boolean kidsHaveNext() throws IOException {
      if (fields.length == 0) {
        return true;
      }
      int field = 0;
      while (field < fields.length && fields[field] == null) {
        field += 1;
      }
      if (field == fields.length) {
        return false;
      }
      boolean result = fields[field].hasNext();
      for(int i=field+1; i < fields.length; ++i) {
        if (fields[i] != null && fields[i].hasNext() != result) {
          throw new IOException("Inconsistent struct length field " + field +
            " = " + result + " differs from field " + i);
        }
      }
      return result;
    }

    @Override
    boolean hasNext() throws IOException {
      return super.hasNext() && (!valuePresent || kidsHaveNext());
    }

    @Override
    void seekToRow(long row) throws IOException {
      //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    Object next(Object previous) throws IOException {
      OrcStruct result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new OrcStruct(fields.length);
        } else {
          result = (OrcStruct) previous;
        }
        for(int i=0; i < fields.length; ++i) {
          if (fields[i] != null) {
            result.setFieldValue(i, fields[i].next(result.getFieldValue(i)));
          }
        }
      }
      return super.next(result);
    }

    @Override
    void startStripe(Map<WriterImpl.StreamName, InStream> streams
                    ) throws IOException {
      super.startStripe(streams);
      for(TreeReader field: fields) {
        if (field != null) {
          field.startStripe(streams);
        }
      }
    }
  }

  private static class UnionTreeReader extends TreeReader {
    private final TreeReader[] fields;
    private RunLengthIntegerReader tags;

    UnionTreeReader(int columnId,
                     List<OrcProto.Type> types,
                     boolean[] included) throws IOException {
      super(columnId);
      OrcProto.Type type = types.get(columnId);
      int fieldCount = type.getSubtypesCount();
      this.fields = new TreeReader[fieldCount];
      for(int i=0; i < fieldCount; ++i) {
        int subtype = type.getSubtypes(i);
        if (included == null || included[subtype]) {
          this.fields[i] = createTreeReader(subtype, types, included);
        }
      }
    }

    @Override
    boolean hasNext() throws IOException {
      return super.hasNext() && (!valuePresent || tags.hasNext());
    }

    @Override
    void seekToRow(long row) throws IOException {
      //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    Object next(Object previous) throws IOException {
      OrcUnion result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new OrcUnion();
        } else {
          result = (OrcUnion) previous;
        }
        byte tag = (byte) tags.next();
        Object previousVal = result.getObject();
        result.set(tag, fields[tag].next(tag == result.getTag() ?
            previousVal : null));
      }
      return super.next(result);
    }

    @Override
    void startStripe(Map<WriterImpl.StreamName, InStream> streams
    ) throws IOException {
      super.startStripe(streams);
      tags = new RunLengthIntegerReader(streams.get(new WriterImpl.StreamName
          (columnId, OrcProto.StripeSection.Kind.DATA)), false);
      for(TreeReader field: fields) {
        if (field != null) {
          field.startStripe(streams);
        }
      }
    }
  }

  private static class ListTreeReader extends TreeReader {
    private final TreeReader elementReader;
    private RunLengthIntegerReader lengths;

    ListTreeReader(int columnId,
                    List<OrcProto.Type> types,
                    boolean[] included) throws IOException {
      super(columnId);
      OrcProto.Type type = types.get(columnId);
      elementReader = createTreeReader(type.getSubtypes(0), types, included);
    }

    @Override
    boolean hasNext() throws IOException {
      return super.hasNext() && (!valuePresent || lengths.hasNext());
    }

    @Override
    void seekToRow(long row) throws IOException {
      //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    @SuppressWarnings("unchecked")
    Object next(Object previous) throws IOException {
      ArrayList<Object> result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new ArrayList<Object>();
        } else {
          result = (ArrayList<Object>) previous;
        }
        int prevLength = result.size();
        int length = (int) lengths.next();
        // read the new elements into the array
        for(int i=0; i< length; i++) {
          result.set(i, elementReader.next(i < prevLength ?
              result.get(i) : null));
        }
        // remove any extra elements
        for(int i=prevLength - 1; i >= length; --i) {
          result.remove(i);
        }
      }
      return super.next(result);
    }

    @Override
    void startStripe(Map<WriterImpl.StreamName, InStream> streams
    ) throws IOException {
      super.startStripe(streams);
      lengths = new RunLengthIntegerReader(streams.get(new WriterImpl.StreamName
          (columnId, OrcProto.StripeSection.Kind.LENGTH)), false);
      if (elementReader != null) {
        elementReader.startStripe(streams);
      }
    }
  }

  private static class MapTreeReader extends TreeReader {
    private final TreeReader keyReader;
    private final TreeReader valueReader;
    private RunLengthIntegerReader lengths;

    MapTreeReader(int columnId,
                   List<OrcProto.Type> types,
                   boolean[] included) throws IOException {
      super(columnId);
      OrcProto.Type type = types.get(columnId);
      int keyColumn = type.getSubtypes(0);
      int valueColumn = type.getSubtypes(1);
      if (included == null || included[keyColumn]) {
        keyReader = createTreeReader(keyColumn, types, included);
      } else {
        keyReader = null;
      }
      if (included == null || included[valueColumn]) {
        valueReader = createTreeReader(valueColumn, types, included);
      } else {
        valueReader = null;
      }
    }

    @Override
    boolean hasNext() throws IOException {
      return super.hasNext() && (!valuePresent || lengths.hasNext());
    }

    @Override
    void seekToRow(long row) throws IOException {
      //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    @SuppressWarnings("unchecked")
    Object next(Object previous) throws IOException {
      HashMap<Object,Object> result = null;
      if (valuePresent) {
        if (previous == null) {
          result = new HashMap<Object,Object>();
        } else {
          result = (HashMap<Object,Object>) previous;
        }
        // for now just clear and create new objects
        result.clear();
        int length = (int) lengths.next();
        // read the new elements into the array
        for(int i=0; i< length; i++) {
          result.put(keyReader.next(null), valueReader.next(null));
        }
      }
      return super.next(result);
    }

    @Override
    void startStripe(Map<WriterImpl.StreamName, InStream> streams
    ) throws IOException {
      super.startStripe(streams);
      lengths = new RunLengthIntegerReader(streams.get(new WriterImpl.StreamName
          (columnId, OrcProto.StripeSection.Kind.LENGTH)), false);
      if (keyReader != null) {
        keyReader.startStripe(streams);
      }
      if (valueReader != null) {
        valueReader.startStripe(streams);
      }
    }
  }

  private static TreeReader createTreeReader(int columnId,
                                             List<OrcProto.Type> types,
                                             boolean[] included
                                            ) throws IOException {
    OrcProto.Type type = types.get(columnId);
    switch (type.getKind()) {
      case BOOLEAN:
        return new BooleanTreeReader(columnId);
      case BYTE:
        return new ByteTreeReader(columnId);
      case DOUBLE:
        return new DoubleTreeReader(columnId);
      case FLOAT:
        return new FloatTreeReader(columnId);
      case SHORT:
        return new ShortTreeReader(columnId);
      case INT:
        return new IntTreeReader(columnId);
      case LONG:
        return new LongTreeReader(columnId);
      case STRING:
        return new StringTreeReader(columnId);
      case BINARY:
        return new BinaryTreeReader(columnId);
      case TIMESTAMP:
        return new TimestampTreeReader(columnId);
      case STRUCT:
        return new StructTreeReader(columnId, types, included);
      case LIST:
        return new ListTreeReader(columnId, types, included);
      case MAP:
        return new MapTreeReader(columnId, types, included);
      case UNION:
        return new UnionTreeReader(columnId, types, included);
      default:
        throw new IllegalArgumentException("Unsupported type " +
          type.getKind());
    }
  }

  OrcProto.StripeFooter readerStripeFooter(StripeInformation stripe
                                           ) throws IOException {
    long offset = stripe.getOffset();
    int length = (int) stripe.getLength();
    int tailLength = (int) stripe.getTailLength();

    // read the footer
    ByteBuffer tailBuf = ByteBuffer.allocate(tailLength);
    file.seek(offset + length - tailLength);
    file.readFully(tailBuf.array(), tailBuf.arrayOffset(), tailLength);
    return OrcProto.StripeFooter.parseFrom(InStream.create("footer", tailBuf,
      codec, bufferSize));
  }

  private void readStreams(StripeInformation stripe) throws IOException {
    OrcProto.StripeFooter footer = readerStripeFooter(stripe);
    long offset = stripe.getOffset();
    streams.clear();

    // if we aren't projecting columns, just read the whole stripe
    if (included == null) {
      byte[] buffer =
        new byte[(int) (stripe.getLength() - stripe.getTailLength())];
      file.seek(offset);
      file.readFully(buffer, 0, buffer.length);
      int sectionOffset = 0;
      for(OrcProto.StripeSection section: footer.getSectionsList()) {
        int sectionLength = (int) section.getLength();
        ByteBuffer sectionBuffer = ByteBuffer.wrap(buffer, sectionOffset,
          sectionLength);
        WriterImpl.StreamName name =
          new WriterImpl.StreamName(section.getColumn(), section.getKind());
        streams.put(name,
          InStream.create(name.toString(), sectionBuffer, codec, bufferSize));
        sectionOffset += sectionLength;
      }
    } else {
      List<OrcProto.StripeSection> sections = footer.getSectionsList();
      // the index of the current section
      int currentSection = 0;
      // byte position of the current section relative to the stripe start
      long sectionOffset = 0;
      while (currentSection < sections.size()) {
        int bytes = 0;

        // find the first section that shouldn't be read
        int excluded=currentSection;
        while (excluded < sections.size() &&
               included[sections.get(excluded).getColumn()]) {
          bytes += sections.get(excluded).getLength();
          excluded += 1;
        }

        // actually read the bytes as a big chunk
        if (bytes != 0) {
          byte[] buffer = new byte[bytes];
          file.seek(offset + sectionOffset);
          file.readFully(buffer, 0, bytes);
          sectionOffset += bytes;

          // create the streams for the sections we just read
          bytes = 0;
          while (currentSection < excluded) {
            OrcProto.StripeSection section = sections.get(currentSection);
            WriterImpl.StreamName name =
              new WriterImpl.StreamName(section.getColumn(), section.getKind());
            streams.put(name,
              InStream.create(name.toString(),
                ByteBuffer.wrap(buffer, bytes,
                  (int) section.getLength()), codec, bufferSize));
            currentSection += 1;
            bytes += section.getLength();
          }
        }

        // skip forward until we get back to a section that we need
        while (currentSection < sections.size() &&
               !included[sections.get(currentSection).getColumn()]) {
          sectionOffset += sections.get(currentSection).getLength();
          currentSection += 1;
        }
      }
    }
    reader.startStripe(streams);
  }

  @Override
  public boolean hasNext() throws IOException {
    while (!reader.hasNext()) {
      if (currentStripe + 1 < stripes.size()) {
        currentStripe += 1;
        readStreams(stripes.get(currentStripe));
      } else {
        return false;
      }
    }
    return currentRow < totalRowCount;
  }

  @Override
  public Object next(Object previous) throws IOException {
    currentRow += 1;
    return reader.next(previous);
  }

  @Override
  public void close() throws IOException {
    file.close();
  }

  @Override
  public long getRowNumber() {
    return currentRow + firstRow;
  }

  /**
   * Return the fraction of rows that have been read from the selected
   * section of the file
   * @return fraction between 0.0 and 1.0 of rows consumed
   */
  @Override
  public float getProgress() {
    return ((float) currentRow)/totalRowCount;
  }
}
