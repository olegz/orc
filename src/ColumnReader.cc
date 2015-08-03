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

#include "ByteRLE.hh"
#include "ColumnReader.hh"
#include "Exceptions.hh"
#include "orc/Int128.hh"
#include "RLE.hh"

#include <math.h>
#include <iostream>

namespace orc {

  StripeStreams::~StripeStreams() {
    // PASS
  }

  inline RleVersion convertRleVersion(proto::ColumnEncoding_Kind kind) {
    switch (static_cast<int>(kind)) {
    case proto::ColumnEncoding_Kind_DIRECT:
    case proto::ColumnEncoding_Kind_DICTIONARY:
      return RleVersion_1;
    case proto::ColumnEncoding_Kind_DIRECT_V2:
    case proto::ColumnEncoding_Kind_DICTIONARY_V2:
      return RleVersion_2;
    default:
      throw ParseError("Unknown encoding in convertRleVersion");
    }
  }

  ColumnReader::ColumnReader(const Type& type,
                             StripeStreams& stripe,
                             MemoryPool* pool
                             ): columnId(type.getColumnId()),
                                memoryPool(pool) {
    std::unique_ptr<SeekableInputStream> stream =
      stripe.getStream(columnId, proto::Stream_Kind_PRESENT);
    if (stream.get()) {
      notNullDecoder = createBooleanRleDecoder(std::move(stream));
    }
  }

  ColumnReader::~ColumnReader() {
    // PASS
  }

  unsigned long ColumnReader::skip(unsigned long numValues) {
    ByteRleDecoder* decoder = notNullDecoder.get();
    if (decoder) {
      // page through the values that we want to skip
      // and count how many are non-null
      const size_t MAX_BUFFER_SIZE = 32768;
      size_t bufferSize = std::min(MAX_BUFFER_SIZE,
                                   static_cast<size_t>(numValues));
      char buffer[MAX_BUFFER_SIZE];
      unsigned long remaining = numValues;
      while (remaining > 0) {
        unsigned long chunkSize =
          std::min(remaining,
                   static_cast<unsigned long>(bufferSize));
        decoder->next(buffer, chunkSize, 0);
        remaining -= chunkSize;
        for(unsigned long i=0; i < chunkSize; ++i) {
          if (!buffer[i]) {
            numValues -= 1;
          }
        }
      }
    }
    return numValues;
  }

  void ColumnReader::next(ColumnVectorBatch& rowBatch,
                          unsigned long numValues,
                          char* incomingMask) {
    if (numValues > rowBatch.capacity) {
      rowBatch.resize(numValues);
    }
    rowBatch.numElements = numValues;
    ByteRleDecoder* decoder = notNullDecoder.get();
    if (decoder) {
      char* notNullArray = rowBatch.notNull.data();
      decoder->next(notNullArray, numValues, incomingMask);
      // check to see if there are nulls in this batch
      for(unsigned long i=0; i < numValues; ++i) {
        if (!notNullArray[i]) {
          rowBatch.hasNulls = true;
          return;
        }
      }
    }
    rowBatch.hasNulls = false;
  }

  /**
   * Expand an array of bytes in place to the corresponding array of longs.
   * Has to work backwards so that they data isn't clobbered during the
   * expansion.
   * @param buffer the array of chars and array of longs that need to be
   *        expanded
   * @param numValues the number of bytes to convert to longs
   */
  void expandBytesToLongs(int64_t* buffer, uint64_t numValues) {
    for(size_t i=numValues - 1; i < numValues; --i) {
      buffer[i] = reinterpret_cast<char *>(buffer)[i];
    }
  }

  class BooleanColumnReader: public ColumnReader {
  private:
    std::unique_ptr<orc::ByteRleDecoder> rle;

  public:
    BooleanColumnReader(const Type& type,
        StripeStreams& stipe, MemoryPool* pool);
    ~BooleanColumnReader();

    unsigned long skip(unsigned long numValues) override;

    void next(ColumnVectorBatch& rowBatch,
              unsigned long numValues,
              char* notNull) override;
  };

  BooleanColumnReader::BooleanColumnReader(const Type& type,
                                           StripeStreams& stripe,
                                           MemoryPool* pool
                                           ): ColumnReader(type, stripe, pool){
    rle = createBooleanRleDecoder(stripe.getStream(columnId,
                                                   proto::Stream_Kind_DATA));
  }

  BooleanColumnReader::~BooleanColumnReader() {
    // PASS
  }

  unsigned long BooleanColumnReader::skip(unsigned long numValues) {
    numValues = ColumnReader::skip(numValues);
    rle->skip(numValues);
    return numValues;
  }

  void BooleanColumnReader::next(ColumnVectorBatch& rowBatch,
                                 unsigned long numValues,
                                 char *notNull) {
    ColumnReader::next(rowBatch, numValues, notNull);
    // Since the byte rle places the output in a char* instead of long*,
    // we cheat here and use the long* and then expand it in a second pass.
    int64_t *ptr = dynamic_cast<LongVectorBatch&>(rowBatch).data.data();
    rle->next(reinterpret_cast<char*>(ptr),
              numValues, rowBatch.hasNulls ? rowBatch.notNull.data() : 0);
    expandBytesToLongs(ptr, numValues);
  }

  class ByteColumnReader: public ColumnReader {
  private:
    std::unique_ptr<orc::ByteRleDecoder> rle;

  public:
    ByteColumnReader(const Type& type, StripeStreams& stipe, MemoryPool* pool);
    ~ByteColumnReader();

    unsigned long skip(unsigned long numValues) override;

    void next(ColumnVectorBatch& rowBatch,
              unsigned long numValues,
              char* notNull) override;
  };

  ByteColumnReader::ByteColumnReader(const Type& type,
                                           StripeStreams& stripe,
                                           MemoryPool* pool
                                           ): ColumnReader(type, stripe, pool){
    rle = createByteRleDecoder(stripe.getStream(columnId,
                                                proto::Stream_Kind_DATA));
  }

  ByteColumnReader::~ByteColumnReader() {
    // PASS
  }

  unsigned long ByteColumnReader::skip(unsigned long numValues) {
    numValues = ColumnReader::skip(numValues);
    rle->skip(numValues);
    return numValues;
  }

  void ByteColumnReader::next(ColumnVectorBatch& rowBatch,
                              unsigned long numValues,
                              char *notNull) {
    ColumnReader::next(rowBatch, numValues, notNull);
    // Since the byte rle places the output in a char* instead of long*,
    // we cheat here and use the long* and then expand it in a second pass.
    int64_t *ptr = dynamic_cast<LongVectorBatch&>(rowBatch).data.data();
    rle->next(reinterpret_cast<char*>(ptr),
              numValues, rowBatch.hasNulls ? rowBatch.notNull.data() : 0);
    expandBytesToLongs(ptr, numValues);
  }

  class IntegerColumnReader: public ColumnReader {
  protected:
    std::unique_ptr<orc::RleDecoder> rle;

  public:
    IntegerColumnReader(const Type& type,
        StripeStreams& stripe, MemoryPool* pool);
    ~IntegerColumnReader();

    unsigned long skip(unsigned long numValues) override;

    void next(ColumnVectorBatch& rowBatch,
              unsigned long numValues,
              char* notNull) override;
  };

  IntegerColumnReader::IntegerColumnReader(const Type& type,
                                           StripeStreams& stripe,
                                           MemoryPool* pool
                                           ): ColumnReader(type, stripe, pool){
    RleVersion vers = convertRleVersion(stripe.getEncoding(columnId).kind());
    rle = createRleDecoder(stripe.getStream(columnId,
                                            proto::Stream_Kind_DATA),
                           true, vers);
  }

  IntegerColumnReader::~IntegerColumnReader() {
    // PASS
  }

  unsigned long IntegerColumnReader::skip(unsigned long numValues) {
    numValues = ColumnReader::skip(numValues);
    rle->skip(numValues);
    return numValues;
  }

  void IntegerColumnReader::next(ColumnVectorBatch& rowBatch,
                                 unsigned long numValues,
                                 char *notNull) {
    ColumnReader::next(rowBatch, numValues, notNull);
    rle->next(dynamic_cast<LongVectorBatch&>(rowBatch).data.data(),
              numValues, rowBatch.hasNulls ? rowBatch.notNull.data() : 0);
  }

  class TimestampColumnReader: public IntegerColumnReader {
    private:
      std::unique_ptr<orc::RleDecoder> nanos;

    public:
      TimestampColumnReader(const Type& type, StripeStreams& stripe,
          MemoryPool* pool);
      ~TimestampColumnReader();

      unsigned long skip(unsigned long numValues) override;

      void next(ColumnVectorBatch& rowBatch,
                unsigned long numValues,
                char* notNull) override;
  };


  TimestampColumnReader::TimestampColumnReader(const Type& type,
                                           StripeStreams& stripe,
                                           MemoryPool* pool
                                           ):
                                     IntegerColumnReader(type, stripe, pool) {
    RleVersion vers = convertRleVersion(stripe.getEncoding(columnId).kind());
    nanos = createRleDecoder(stripe.getStream(columnId,
                                            proto::Stream_Kind_SECONDARY),
                           false, vers);
  }

  TimestampColumnReader::~TimestampColumnReader() {
    // PASS
  }

  unsigned long TimestampColumnReader::skip(unsigned long numValues) {
    numValues = IntegerColumnReader::skip(numValues);
    nanos->skip(numValues);
    return numValues;
  }

  void TimestampColumnReader::next(ColumnVectorBatch& rowBatch,
                                 unsigned long numValues,
                                 char *notNull) {
    ColumnReader::next(rowBatch, numValues, notNull);

    int64_t* pStamp = dynamic_cast<LongVectorBatch&>(rowBatch).data.data();
    rle->next(pStamp,
              numValues, rowBatch.hasNulls ? rowBatch.notNull.data() : 0);
    DataBuffer<int64_t> nanoseconds(rowBatch.capacity, rowBatch.memoryPool);
    nanos->next(nanoseconds.data(),
              numValues, rowBatch.hasNulls ? rowBatch.notNull.data() : 0);

    // Construct the values
    int zeroes = 0;
    int64_t nanosec = 0;
    for(unsigned int i=0; i<rowBatch.capacity; i++) {
      nanosec =  nanoseconds[i] >> 3 ;
      zeroes = nanoseconds[i] & 0x7 ;
      while(zeroes>=0) {
        nanosec *=10 ;
        zeroes--;
      }
      pStamp[i] =  pStamp[i]*1000000000 + nanosec // ns since 1/1/2015 (ORC)
          + 1420070400000000000; // ns between 1/1/1970 and 1/1/2015
    }
  }

  class DoubleColumnReader: public ColumnReader {
  public:
    DoubleColumnReader(const Type& type, StripeStreams& stripe,
        MemoryPool* pool);
    ~DoubleColumnReader();

    unsigned long skip(unsigned long numValues) override;

    void next(ColumnVectorBatch& rowBatch,
              unsigned long numValues,
              char* notNull) override;

  private:
    std::unique_ptr<SeekableInputStream> inputStream;
    TypeKind columnKind;
    const unsigned int bytesPerValue ;
    const char *bufferPointer;
    const char *bufferEnd;

    unsigned char readByte() {
      if (bufferPointer == bufferEnd) {
        int length;
        if (!inputStream->Next
            (reinterpret_cast<const void**>(&bufferPointer), &length)) {
          throw ParseError("bad read in DoubleColumnReader::next()");
        }
        bufferEnd = bufferPointer + length;
      }
      return static_cast<unsigned char>(*(bufferPointer++));
    }

    double readDouble() {
      int64_t bits = 0;
      for (unsigned int i=0; i < 8; i++) {
        bits |= static_cast<int64_t>(readByte()) << (i*8);
      }
      double *result = reinterpret_cast<double*>(&bits);
      return *result;
    }

    double readFloat() {
      int32_t bits = 0;
      for (unsigned int i=0; i < 4; i++) {
        bits |= readByte() << (i*8);
      }
      float *result = reinterpret_cast<float*>(&bits);
      return *result;
    }
  };

  DoubleColumnReader::DoubleColumnReader
               (const Type& type,
                StripeStreams& stripe,
                MemoryPool* pool
                ): ColumnReader(type, stripe,pool),
                   inputStream(stripe.getStream
                                         (columnId,
                                          proto::Stream_Kind_DATA)),
                   columnKind(type.getKind()),
                   bytesPerValue((type.getKind() == FLOAT) ? 4 : 8),
                   bufferPointer(NULL),
                   bufferEnd(NULL) {
    // PASS
  }

  DoubleColumnReader::~DoubleColumnReader() {
    // PASS
  }

  unsigned long DoubleColumnReader::skip(unsigned long numValues) {
    numValues = ColumnReader::skip(numValues);

    if (static_cast<size_t>(bufferEnd - bufferPointer) >=
        bytesPerValue * numValues) {
      bufferPointer+= bytesPerValue*numValues;
    } else {
      inputStream->Skip(static_cast<int>(bytesPerValue*numValues -
                                         static_cast<size_t>(bufferEnd -
                                                             bufferPointer)));
      bufferEnd = NULL;
      bufferPointer = NULL;
    }

    return numValues;
  }

  void DoubleColumnReader::next(ColumnVectorBatch& rowBatch,
                                unsigned long numValues,
                                char *notNull) {
    ColumnReader::next(rowBatch, numValues, notNull);
    // update the notNull from the parent class
    notNull = rowBatch.hasNulls ? rowBatch.notNull.data() : 0;
    double* outArray = dynamic_cast<DoubleVectorBatch&>(rowBatch).data.data();

    if (columnKind == FLOAT) {
      if (notNull) {
        for(size_t i=0; i < numValues; ++i) {
          if (notNull[i]) {
            outArray[i] = readFloat();
          }
        }
      } else {
        for(size_t i=0; i < numValues; ++i) {
          outArray[i] = readFloat();
        }
      }
    } else {
      if (notNull) {
        for(size_t i=0; i < numValues; ++i) {
          if (notNull[i]) {
            outArray[i] = readDouble();
          }
        }
      } else {
        for(size_t i=0; i < numValues; ++i) {
          outArray[i] = readDouble();
        }
      }
    }
  }

  void readFully(char* buffer, long bufferSize, SeekableInputStream* stream) {
    long posn = 0;
    while (posn < bufferSize) {
      const void* chunk;
      int length;
      if (!stream->Next(&chunk, &length)) {
        throw ParseError("bad read in readFully");
      }
      memcpy(buffer + posn, chunk, static_cast<size_t>(length));
      posn += length;
    }
  }

  class StringDictionaryColumnReader: public ColumnReader {
  private:
    std::vector<char> dictionaryBlob;
    std::vector<int64_t> dictionaryOffset;
    std::unique_ptr<RleDecoder> rle;
    unsigned int dictionaryCount;

  public:
    StringDictionaryColumnReader(const Type& type,
        StripeStreams& stipe, MemoryPool* pool);
    ~StringDictionaryColumnReader();

    unsigned long skip(unsigned long numValues) override;

    void next(ColumnVectorBatch& rowBatch,
              unsigned long numValues,
              char *notNull) override;
  };

  StringDictionaryColumnReader::StringDictionaryColumnReader
      (const Type& type,
       StripeStreams& stripe,
       MemoryPool* pool
       ): ColumnReader(type, stripe, pool) {
    RleVersion rleVersion = convertRleVersion(stripe.getEncoding(columnId)
                                                .kind());
    dictionaryCount = stripe.getEncoding(columnId).dictionarysize();
    rle = createRleDecoder(stripe.getStream(columnId,
                                            proto::Stream_Kind_DATA),
                           false, rleVersion);
    std::unique_ptr<RleDecoder> lengthDecoder =
      createRleDecoder(stripe.getStream(columnId,
                                        proto::Stream_Kind_LENGTH),
                       false, rleVersion);
    dictionaryOffset.resize(dictionaryCount+1);
    int64_t* lengthArray = dictionaryOffset.data();
    lengthDecoder->next(lengthArray + 1, dictionaryCount, 0);
    lengthArray[0] = 0;
    for(unsigned int i=1; i < dictionaryCount + 1; ++i) {
      lengthArray[i] += lengthArray[i-1];
    }
    long blobSize = lengthArray[dictionaryCount];
    dictionaryBlob.resize(static_cast<unsigned long>(blobSize));
    std::unique_ptr<SeekableInputStream> blobStream =
      stripe.getStream(columnId, proto::Stream_Kind_DICTIONARY_DATA);
    readFully(dictionaryBlob.data(), blobSize, blobStream.get());
  }

  StringDictionaryColumnReader::~StringDictionaryColumnReader() {
    // PASS
  }

  unsigned long StringDictionaryColumnReader::skip(unsigned long numValues) {
    numValues = ColumnReader::skip(numValues);
    rle->skip(numValues);
    return numValues;
  }

  void StringDictionaryColumnReader::next(ColumnVectorBatch& rowBatch,
                                          unsigned long numValues,
                                          char *notNull) {
    ColumnReader::next(rowBatch, numValues, notNull);
    // update the notNull from the parent class
    notNull = rowBatch.hasNulls ? rowBatch.notNull.data() : 0;
    StringVectorBatch& byteBatch = dynamic_cast<StringVectorBatch&>(rowBatch);
    char *blob = dictionaryBlob.data();
    int64_t *dictionaryOffsets = dictionaryOffset.data();
    char **outputStarts = byteBatch.data.data();
    int64_t *outputLengths = byteBatch.length.data();
    rle->next(outputLengths, numValues, notNull);
    if (notNull) {
      for(unsigned int i=0; i < numValues; ++i) {
        if (notNull[i]) {
          long entry = outputLengths[i];
          outputStarts[i] = blob + dictionaryOffsets[entry];
          outputLengths[i] = dictionaryOffsets[entry+1] -
            dictionaryOffsets[entry];
        }
      }
    } else {
      for(unsigned int i=0; i < numValues; ++i) {
        long entry = outputLengths[i];
        outputStarts[i] = blob + dictionaryOffsets[entry];
        outputLengths[i] = dictionaryOffsets[entry+1] -
          dictionaryOffsets[entry];
      }
    }
  }

  class StringDirectColumnReader: public ColumnReader {
  private:
    std::vector<char> blobBuffer;
    std::unique_ptr<RleDecoder> lengthRle;
    std::unique_ptr<SeekableInputStream> blobStream;
    const char *lastBuffer;
    size_t lastBufferLength;

    /**
     * Compute the total length of the values.
     * @param lengths the array of lengths
     * @param notNull the array of notNull flags
     * @param numValues the lengths of the arrays
     * @return the total number of bytes for the non-null values
     */
    size_t computeSize(const int64_t *lengths, const char *notNull,
                       uint64_t numValues);

  public:
    StringDirectColumnReader(const Type& type, StripeStreams& stipe,
        MemoryPool* pool);
    ~StringDirectColumnReader();

    unsigned long skip(unsigned long numValues) override;

    void next(ColumnVectorBatch& rowBatch,
              unsigned long numValues,
              char *notNull) override;
  };

  StringDirectColumnReader::StringDirectColumnReader(const Type& type,
                                                     StripeStreams& stripe,
                                                     MemoryPool* pool
                                                     ):
                                           ColumnReader(type,stripe, pool) {
    RleVersion rleVersion = convertRleVersion(stripe.getEncoding(columnId)
                                                .kind());
    lengthRle = createRleDecoder(stripe.getStream(columnId,
                                                  proto::Stream_Kind_LENGTH),
                                 false, rleVersion);
    blobStream = stripe.getStream(columnId, proto::Stream_Kind_DATA);
    lastBuffer = 0;
    lastBufferLength = 0;
  }

  StringDirectColumnReader::~StringDirectColumnReader() {
    // PASS
  }

  unsigned long StringDirectColumnReader::skip(unsigned long numValues) {
    const size_t BUFFER_SIZE = 1024;
    numValues = ColumnReader::skip(numValues);
    int64_t buffer[BUFFER_SIZE];
    uint64_t done = 0;
    size_t totalBytes = 0;
    // read the lengths, so we know haw many bytes to skip
    while (done < numValues) {
      unsigned long step = std::min(BUFFER_SIZE,
                                    static_cast<size_t>(numValues - done));
      lengthRle->next(buffer, step, 0);
      totalBytes += computeSize(buffer, 0, step);
      done += step;
    }
    if (totalBytes <= lastBufferLength) {
      // subtract the needed bytes from the ones left over
      lastBufferLength -= totalBytes;
      lastBuffer += totalBytes;
    } else {
      // move the stream forward after accounting for the buffered bytes
      totalBytes -= lastBufferLength;
      blobStream->Skip(static_cast<int>(totalBytes));
      lastBufferLength = 0;
      lastBuffer = 0;
    }
    return numValues;
  }

  size_t StringDirectColumnReader::computeSize(const int64_t* lengths,
                                               const char* notNull,
                                               uint64_t numValues) {
    size_t totalLength = 0;
    if (notNull) {
      for(size_t i=0; i < numValues; ++i) {
        if (notNull[i]) {
          totalLength += static_cast<size_t>(lengths[i]);
        }
      }
    } else {
      for(size_t i=0; i < numValues; ++i) {
        totalLength += static_cast<size_t>(lengths[i]);
      }
    }
    return totalLength;
  }

  void StringDirectColumnReader::next(ColumnVectorBatch& rowBatch,
                                      unsigned long numValues,
                                      char *notNull) {
    ColumnReader::next(rowBatch, numValues, notNull);
    // update the notNull from the parent class
    notNull = rowBatch.hasNulls ? rowBatch.notNull.data() : 0;
    StringVectorBatch& byteBatch = dynamic_cast<StringVectorBatch&>(rowBatch);
    char **startPtr = byteBatch.data.data();
    int64_t *lengthPtr = byteBatch.length.data();

    // read the length vector
    lengthRle->next(lengthPtr, numValues, notNull);

    // figure out the total length of data we need from the blob stream
    const size_t totalLength = computeSize(lengthPtr, notNull, numValues);

    // Load data from the blob stream into our buffer until we have enough
    // to get the rest directly out of the stream's buffer.
    size_t bytesBuffered = 0;
    blobBuffer.resize(totalLength);
    char *ptr= blobBuffer.data();
    while (bytesBuffered + lastBufferLength < totalLength) {
      blobBuffer.resize(bytesBuffered + lastBufferLength);
      memcpy(ptr + bytesBuffered, lastBuffer, lastBufferLength);
      bytesBuffered += lastBufferLength;
      const void* readBuffer;
      int readLength;
      if (!blobStream->Next(&readBuffer, &readLength)) {
        throw ParseError("failed to read in StringDirectColumnReader.next");
      }
      lastBuffer = static_cast<const char*>(readBuffer);
      lastBufferLength = static_cast<size_t>(readLength);
    }

    // Set up the start pointers for the ones that will come out of the buffer.
    size_t filledSlots = 0;
    size_t usedBytes = 0;
    ptr = blobBuffer.data();
    if (notNull) {
      while (filledSlots < numValues &&
             (!notNull[filledSlots] ||
              usedBytes + static_cast<size_t>(lengthPtr[filledSlots]) <=
              bytesBuffered)) {
        if (notNull[filledSlots]) {
          startPtr[filledSlots] = ptr + usedBytes;
          usedBytes += static_cast<size_t>(lengthPtr[filledSlots]);
        }
        filledSlots += 1;
      }
    } else {
      while (filledSlots < numValues &&
             (usedBytes + static_cast<size_t>(lengthPtr[filledSlots]) <=
              bytesBuffered)) {
        startPtr[filledSlots] = ptr + usedBytes;
        usedBytes += static_cast<size_t>(lengthPtr[filledSlots]);
        filledSlots += 1;
      }
    }

    // do we need to complete the last value in the blob buffer?
    if (usedBytes < bytesBuffered) {
      size_t moreBytes = static_cast<size_t>(lengthPtr[filledSlots]) -
        (bytesBuffered - usedBytes);
      blobBuffer.resize(bytesBuffered + moreBytes);
      ptr = blobBuffer.data();
      memcpy(ptr + bytesBuffered, lastBuffer, moreBytes);
      lastBuffer += moreBytes;
      lastBufferLength -= moreBytes;
      startPtr[filledSlots++] = ptr + usedBytes;
    }

    // Finally, set up any remaining entries into the stream buffer
    if (notNull) {
      while (filledSlots < numValues) {
        if (notNull[filledSlots]) {
          startPtr[filledSlots] = const_cast<char*>(lastBuffer);
          lastBuffer += lengthPtr[filledSlots];
          lastBufferLength -= static_cast<size_t>(lengthPtr[filledSlots]);
        }
        filledSlots += 1;
      }
    } else {
      while (filledSlots < numValues) {
        startPtr[filledSlots] = const_cast<char*>(lastBuffer);
        lastBuffer += lengthPtr[filledSlots];
        lastBufferLength -= static_cast<size_t>(lengthPtr[filledSlots]);
        filledSlots += 1;
      }
    }
  }

  class StructColumnReader: public ColumnReader {
  private:
    std::vector<ColumnReader*> children;

  public:
    StructColumnReader(const Type& type,
                       StripeStreams& stipe,
                       MemoryPool* pool);
    ~StructColumnReader();

    unsigned long skip(unsigned long numValues) override;

    void next(ColumnVectorBatch& rowBatch,
              unsigned long numValues,
              char *notNull) override;
  };

  StructColumnReader::StructColumnReader(const Type& type,
                                         StripeStreams& stripe,
                                         MemoryPool* pool
                                         ): ColumnReader(type, stripe, pool) {
    // count the number of selected sub-columns
    const std::vector<bool> selectedColumns = stripe.getSelectedColumns();
    switch (static_cast<int>(stripe.getEncoding(columnId).kind())) {
    case proto::ColumnEncoding_Kind_DIRECT:
      for(unsigned int i=0; i < type.getSubtypeCount(); ++i) {
        const Type& child = type.getSubtype(i);
        if (selectedColumns[static_cast<unsigned int>(child.getColumnId())]) {
          children.push_back(buildReader(child, stripe, memoryPool).release());
        }
      }
      break;
    case proto::ColumnEncoding_Kind_DIRECT_V2:
    case proto::ColumnEncoding_Kind_DICTIONARY:
    case proto::ColumnEncoding_Kind_DICTIONARY_V2:
    default:
      throw ParseError("Unknown encoding for StructColumnReader");
    }
  }

  StructColumnReader::~StructColumnReader() {
    for (size_t i=0; i<children.size(); i++) {
      delete children[i];
    }
  }

  unsigned long StructColumnReader::skip(unsigned long numValues) {
    numValues = ColumnReader::skip(numValues);
    for(std::vector<ColumnReader*>::iterator ptr=children.begin(); ptr != children.end(); ++ptr) {
      (*ptr)->skip(numValues);
    }
    return numValues;
  }

  void StructColumnReader::next(ColumnVectorBatch& rowBatch,
                                unsigned long numValues,
                                char *notNull) {
    ColumnReader::next(rowBatch, numValues, notNull);
    unsigned int i=0;
    notNull = rowBatch.hasNulls? rowBatch.notNull.data() : 0;
    for(std::vector<ColumnReader*>::iterator ptr=children.begin();
        ptr != children.end(); ++ptr, ++i) {
      (*ptr)->next(*(dynamic_cast<StructVectorBatch&>(rowBatch).fields[i]),
                   numValues, notNull);
    }
  }

  class ListColumnReader: public ColumnReader {
  private:
    std::unique_ptr<ColumnReader> child;
    std::unique_ptr<RleDecoder> rle;

  public:
    ListColumnReader(const Type& type, StripeStreams& stipe,
        MemoryPool* pool);
    ~ListColumnReader();

    unsigned long skip(unsigned long numValues) override;

    void next(ColumnVectorBatch& rowBatch,
              unsigned long numValues,
              char *notNull) override;
  };

  ListColumnReader::ListColumnReader(const Type& type,
                                     StripeStreams& stripe,
                                     MemoryPool* pool
                                     ): ColumnReader(type, stripe, pool) {
    // count the number of selected sub-columns
    const std::vector<bool> selectedColumns = stripe.getSelectedColumns();
    RleVersion vers = convertRleVersion(stripe.getEncoding(columnId).kind());
    rle = createRleDecoder(stripe.getStream(columnId,
                                            proto::Stream_Kind_LENGTH),
                           false, vers);
    const Type& childType = type.getSubtype(0);
    if (selectedColumns[static_cast<unsigned int>(childType.getColumnId())]) {
      child = buildReader(childType, stripe, pool);
    }
  }

  ListColumnReader::~ListColumnReader() {
    // PASS
  }

  unsigned long ListColumnReader::skip(unsigned long numValues) {
    numValues = ColumnReader::skip(numValues);
    ColumnReader *childReader = child.get();
    if (childReader) {
      const uint64_t BUFFER_SIZE = 1024;
      int64_t buffer[BUFFER_SIZE];
      uint64_t childrenElements = 0;
      uint64_t lengthsRead = 0;
      while (lengthsRead < numValues) {
        uint64_t chunk = std::min(numValues - lengthsRead, BUFFER_SIZE);
        rle->next(buffer, chunk, 0);
        for(size_t i=0; i < chunk; ++i) {
          childrenElements += static_cast<size_t>(buffer[i]);
        }
        lengthsRead += chunk;
      }
      childReader->skip(childrenElements);
    } else {
      rle->skip(numValues);
    }
    return numValues;
  }

  void ListColumnReader::next(ColumnVectorBatch& rowBatch,
                              unsigned long numValues,
                              char *notNull) {
    ColumnReader::next(rowBatch, numValues, notNull);
    ListVectorBatch &listBatch = dynamic_cast<ListVectorBatch&>(rowBatch);
    int64_t* offsets = listBatch.offsets.data();
    notNull = listBatch.hasNulls ? listBatch.notNull.data() : 0;
    rle->next(offsets, numValues, notNull);
    unsigned long totalChildren = 0;
    if (notNull) {
      for(size_t i=0; i < numValues; ++i) {
        if (notNull[i]) {
          unsigned long tmp = static_cast<unsigned long>(offsets[i]);
          offsets[i] = static_cast<long>(totalChildren);
          totalChildren += tmp;
        } else {
          offsets[i] = static_cast<long>(totalChildren);
        }
      }
    } else {
      for(size_t i=0; i < numValues; ++i) {
        unsigned long tmp = static_cast<unsigned long>(offsets[i]);
        offsets[i] = static_cast<long>(totalChildren);
        totalChildren += tmp;
      }
    }
    offsets[numValues] = static_cast<long>(totalChildren);
    ColumnReader *childReader = child.get();
    if (childReader) {
      childReader->next(*(listBatch.elements.get()), totalChildren, 0);
    }
  }

  class MapColumnReader: public ColumnReader {
  private:
    std::unique_ptr<ColumnReader> keyReader;
    std::unique_ptr<ColumnReader> elementReader;
    std::unique_ptr<RleDecoder> rle;

  public:
    MapColumnReader(const Type& type, StripeStreams& stipe, MemoryPool* pool);
    ~MapColumnReader();

    unsigned long skip(unsigned long numValues) override;

    void next(ColumnVectorBatch& rowBatch,
              unsigned long numValues,
              char *notNull) override;
  };

  MapColumnReader::MapColumnReader(const Type& type,
                                     StripeStreams& stripe,
                                     MemoryPool* pool
                                     ): ColumnReader(type, stripe, pool) {
    // count the number of selected sub-columns
    const std::vector<bool> selectedColumns = stripe.getSelectedColumns();
    RleVersion vers = convertRleVersion(stripe.getEncoding(columnId).kind());
    rle = createRleDecoder(stripe.getStream(columnId,
                                            proto::Stream_Kind_LENGTH),
                           false, vers);
    const Type& keyType = type.getSubtype(0);
    if (selectedColumns[static_cast<unsigned int>(keyType.getColumnId())]) {
      keyReader = buildReader(keyType, stripe, pool);
    }
    const Type& elementType = type.getSubtype(1);
    if (selectedColumns[static_cast<unsigned int>(elementType.getColumnId())]) {
      elementReader = buildReader(elementType, stripe, pool);
    }
  }

  MapColumnReader::~MapColumnReader() {
    // PASS
  }

  unsigned long MapColumnReader::skip(unsigned long numValues) {
    numValues = ColumnReader::skip(numValues);
    ColumnReader *rawKeyReader = keyReader.get();
    ColumnReader *rawElementReader = elementReader.get();
    if (rawKeyReader || rawElementReader) {
      const uint64_t BUFFER_SIZE = 1024;
      int64_t buffer[BUFFER_SIZE];
      uint64_t childrenElements = 0;
      uint64_t lengthsRead = 0;
      while (lengthsRead < numValues) {
        unsigned long chunk = std::min(numValues - lengthsRead, BUFFER_SIZE);
        rle->next(buffer, chunk, 0);
        for(size_t i=0; i < chunk; ++i) {
          childrenElements += static_cast<size_t>(buffer[i]);
        }
        lengthsRead += chunk;
      }
      if (rawKeyReader) {
        rawKeyReader->skip(childrenElements);
      }
      if (rawElementReader) {
        rawElementReader->skip(childrenElements);
      }
    } else {
      rle->skip(numValues);
    }
    return numValues;
  }

  void MapColumnReader::next(ColumnVectorBatch& rowBatch,
                             unsigned long numValues,
                             char *notNull) {
    ColumnReader::next(rowBatch, numValues, notNull);
    MapVectorBatch &mapBatch = dynamic_cast<MapVectorBatch&>(rowBatch);
    int64_t* offsets = mapBatch.offsets.data();
    notNull = mapBatch.hasNulls ? mapBatch.notNull.data() : 0;
    rle->next(offsets, numValues, notNull);
    unsigned long totalChildren = 0;
    if (notNull) {
      for(size_t i=0; i < numValues; ++i) {
        if (notNull[i]) {
          unsigned long tmp = static_cast<unsigned long>(offsets[i]);
          offsets[i] = static_cast<long>(totalChildren);
          totalChildren += tmp;
        } else {
          offsets[i] = static_cast<long>(totalChildren);
        }
      }
    } else {
      for(size_t i=0; i < numValues; ++i) {
        unsigned long tmp = static_cast<unsigned long>(offsets[i]);
        offsets[i] = static_cast<long>(totalChildren);
        totalChildren += tmp;
      }
    }
    offsets[numValues] = static_cast<long>(totalChildren);
    ColumnReader *rawKeyReader = keyReader.get();
    if (rawKeyReader) {
      rawKeyReader->next(*(mapBatch.keys.get()), totalChildren, 0);
    }
    ColumnReader *rawElementReader = elementReader.get();
    if (rawElementReader) {
      rawElementReader->next(*(mapBatch.elements.get()), totalChildren, 0);
    }
  }

  /**
   * Destructively convert the number from zigzag encoding to the
   * natural signed representation.
   */
  void unZigZagInt128(Int128& value) {
    bool needsNegate = value.getLowBits() & 1;
    value >>= 1;
    if (needsNegate) {
      value.negate();
      value -= 1;
    }
  }

  class Decimal64ColumnReader: public ColumnReader {
  public:
    static const int32_t MAX_PRECISION_64 = 18;
    static const int32_t MAX_PRECISION_128 = 38;
    static const int64_t POWERS_OF_TEN[MAX_PRECISION_64 + 1];

  protected:
    std::unique_ptr<SeekableInputStream> valueStream;
    int32_t precision;
    int32_t scale;
    const char* buffer;
    const char* bufferEnd;

    std::unique_ptr<RleDecoder> scaleDecoder;

    /**
     * Read the valueStream for more bytes.
     */
    void readBuffer() {
      while (buffer == bufferEnd) {
        int length;
        if (!valueStream->Next(reinterpret_cast<const void**>(&buffer),
                               &length)) {
          throw ParseError("Read past end of stream in Decimal64ColumnReader "+
                           valueStream->getName());
        }
        bufferEnd = buffer + length;
      }
    }

    void readInt64(int64_t& value, int32_t currentScale) {
      value = 0;
      size_t offset = 0;
      while (true) {
        readBuffer();
        unsigned char ch = static_cast<unsigned char>(*(buffer++));
        value |= static_cast<uint64_t>(ch & 0x7f) << offset;
        offset += 7;
        if (!(ch & 0x80)) {
          break;
        }
      }
      value = unZigZag(static_cast<uint64_t>(value));
      if (scale > currentScale) {
        value *= POWERS_OF_TEN[scale - currentScale];
      } else if (scale < currentScale) {
        value /= POWERS_OF_TEN[currentScale - scale];
      }
    }

  public:
    Decimal64ColumnReader(const Type& type, StripeStreams& stipe,
        MemoryPool* pool);
    ~Decimal64ColumnReader();

    unsigned long skip(unsigned long numValues) override;

    void next(ColumnVectorBatch& rowBatch,
              unsigned long numValues,
              char *notNull) override;
  };
  const int32_t Decimal64ColumnReader::MAX_PRECISION_64;
  const int32_t Decimal64ColumnReader::MAX_PRECISION_128;
  const int64_t Decimal64ColumnReader::POWERS_OF_TEN[MAX_PRECISION_64 + 1]=
    {1,
     10,
     100,
     1000,
     10000,
     100000,
     1000000,
     10000000,
     100000000,
     1000000000,
     10000000000,
     100000000000,
     1000000000000,
     10000000000000,
     100000000000000,
     1000000000000000,
     10000000000000000,
     100000000000000000,
     1000000000000000000};

  Decimal64ColumnReader::Decimal64ColumnReader(const Type& type,
                                               StripeStreams& stripe,
                                               MemoryPool* pool
                                               ): ColumnReader(type, stripe, pool) {
    scale = static_cast<int32_t>(type.getScale());
    precision = static_cast<int32_t>(type.getPrecision());
    valueStream = stripe.getStream(columnId, proto::Stream_Kind_DATA);
    buffer = nullptr;
    bufferEnd = nullptr;
    RleVersion vers = convertRleVersion(stripe.getEncoding(columnId).kind());
    scaleDecoder = createRleDecoder(stripe.getStream
                                    (columnId,
                                     proto::Stream_Kind_SECONDARY),
                                    true, vers);
  }

  Decimal64ColumnReader::~Decimal64ColumnReader() {
    // PASS
  }

  unsigned long Decimal64ColumnReader::skip(unsigned long numValues) {
    numValues = ColumnReader::skip(numValues);
    uint64_t skipped = 0;
    while (skipped < numValues) {
      readBuffer();
      if (!(0x80 & *(buffer++))) {
        skipped += 1;
      }
    }
    scaleDecoder->skip(numValues);
    return numValues;
  }

  void Decimal64ColumnReader::next(ColumnVectorBatch& rowBatch,
                                   unsigned long numValues,
                                   char *notNull) {
    ColumnReader::next(rowBatch, numValues, notNull);
    notNull = rowBatch.hasNulls ? rowBatch.notNull.data() : 0;
    Decimal64VectorBatch &batch =
      dynamic_cast<Decimal64VectorBatch&>(rowBatch);
    int64_t* values = batch.values.data();
    // read the next group of scales
    int64_t* scaleBuffer = batch.readScales->data();
    scaleDecoder->next(scaleBuffer, numValues, notNull);
    batch.precision = precision;
    batch.scale = scale;
    if (notNull) {
      for(size_t i=0; i < numValues; ++i) {
        if (notNull[i]) {
          readInt64(values[i], static_cast<int32_t>(scaleBuffer[i]));
        }
      }
    } else {
      for(size_t i=0; i < numValues; ++i) {
        readInt64(values[i], static_cast<int32_t>(scaleBuffer[i]));
      }
    }
  }

  void scaleInt128(Int128& value, int32_t scale, int32_t currentScale) {
    if (scale > currentScale) {
      while(scale > currentScale) {
        int scaleAdjust = std::min(Decimal64ColumnReader::MAX_PRECISION_64,
                                   scale - currentScale);
        value *= Decimal64ColumnReader::POWERS_OF_TEN[scaleAdjust];
        currentScale += scaleAdjust;
      }
    } else if (scale < currentScale) {
      Int128 remainder;
      while(currentScale > scale) {
        int scaleAdjust = std::min(Decimal64ColumnReader::MAX_PRECISION_64,
                                   currentScale - scale);
        value = value.divide(Decimal64ColumnReader::POWERS_OF_TEN[scaleAdjust],
                             remainder);
        currentScale -= scaleAdjust;
      }
    }
  }

  class Decimal128ColumnReader: public Decimal64ColumnReader {
  public:
    Decimal128ColumnReader(const Type& type, StripeStreams& stipe,
        MemoryPool* pool);
    ~Decimal128ColumnReader();

    void next(ColumnVectorBatch& rowBatch,
              unsigned long numValues,
              char *notNull) override;

  private:
    void readInt128(Int128& value, int32_t currentScale) {
      value = 0;
      Int128 work;
      uint32_t offset = 0;
      while (true) {
        readBuffer();
        unsigned char ch = static_cast<unsigned char>(*(buffer++));
        work = ch & 0x7f;
        work <<= offset;
        value |=  work;
        offset += 7;
        if (!(ch & 0x80)) {
          break;
        }
      }
      unZigZagInt128(value);
      scaleInt128(value, scale, currentScale);
    }
  };

  Decimal128ColumnReader::Decimal128ColumnReader(const Type& type,
                                                 StripeStreams& stripe,
                                                 MemoryPool* pool
                                                 ):
                                   Decimal64ColumnReader(type, stripe, pool) {
    // PASS
  }

  Decimal128ColumnReader::~Decimal128ColumnReader() {
    // PASS
  }

  void Decimal128ColumnReader::next(ColumnVectorBatch& rowBatch,
                                   unsigned long numValues,
                                   char *notNull) {
    ColumnReader::next(rowBatch, numValues, notNull);
    notNull = rowBatch.hasNulls ? rowBatch.notNull.data() : 0;
    Decimal128VectorBatch &batch =
      dynamic_cast<Decimal128VectorBatch&>(rowBatch);
    Int128* values = batch.values.data();
    // read the next group of scales
    int64_t* scaleBuffer = batch.readScales->data();
    scaleDecoder->next(scaleBuffer, numValues, notNull);
    batch.precision = precision;
    batch.scale = scale;
    if (notNull) {
      for(size_t i=0; i < numValues; ++i) {
        if (notNull[i]) {
          readInt128(values[i], static_cast<int32_t>(scaleBuffer[i]));
        }
      }
    } else {
      for(size_t i=0; i < numValues; ++i) {
        readInt128(values[i], static_cast<int32_t>(scaleBuffer[i]));
      }
    }
  }

  class DecimalHive11ColumnReader: public Decimal64ColumnReader {
  private:
    bool throwOnOverflow;
    std::ostream* errorStream;

    /**
     * Read an Int128 from the stream and correct it to the desired scale.
     */
    bool readInt128(Int128& value, int32_t currentScale) {
      // -/+ 99999999999999999999999999999999999999
      static const Int128 MIN_VALUE(-0x4b3b4ca85a86c47b, 0xf675ddc000000001);
      static const Int128 MAX_VALUE( 0x4b3b4ca85a86c47a, 0x098a223fffffffff);

      value = 0;
      Int128 work;
      uint32_t offset = 0;
      bool result = true;
      while (true) {
        readBuffer();
        unsigned char ch = static_cast<unsigned char>(*(buffer++));
        work = ch & 0x7f;
        // If we have read more than 128 bits, we flag the error, but keep
        // reading bytes so the stream isn't thrown off.
        if (offset > 128 || (offset == 126 && work > 3)) {
          result = false;
        }
        work <<= offset;
        value |=  work;
        offset += 7;
        if (!(ch & 0x80)) {
          break;
        }
      }

      if (!result) {
        return result;
      }
      unZigZagInt128(value);
      scaleInt128(value, scale, currentScale);
      return value >= MIN_VALUE && value <= MAX_VALUE;
    }

  public:
    DecimalHive11ColumnReader(const Type& type, StripeStreams& stipe,
        MemoryPool* pool);
    ~DecimalHive11ColumnReader();

    void next(ColumnVectorBatch& rowBatch,
              unsigned long numValues,
              char *notNull) override;
  };

  DecimalHive11ColumnReader::DecimalHive11ColumnReader(const Type& type,
                                                       StripeStreams& stripe,
                                                       MemoryPool* pool
                                                       ):
                                   Decimal64ColumnReader(type, stripe, pool) {
    const ReaderOptions options = stripe.getReaderOptions();
    scale = options.getForcedScaleOnHive11Decimal();
    throwOnOverflow = options.getThrowOnHive11DecimalOverflow();
    errorStream = options.getErrorStream();
  }

  DecimalHive11ColumnReader::~DecimalHive11ColumnReader() {
    // PASS
  }

  void DecimalHive11ColumnReader::next(ColumnVectorBatch& rowBatch,
                                       unsigned long numValues,
                                       char *notNull) {
    ColumnReader::next(rowBatch, numValues, notNull);
    notNull = rowBatch.hasNulls ? rowBatch.notNull.data() : 0;
    Decimal128VectorBatch &batch =
      dynamic_cast<Decimal128VectorBatch&>(rowBatch);
    Int128* values = batch.values.data();
    // read the next group of scales
    int64_t* scaleBuffer = batch.readScales->data();

    scaleDecoder->next(scaleBuffer, numValues, notNull);

    batch.precision = precision;
    batch.scale = scale;
    if (notNull) {
      for(size_t i=0; i < numValues; ++i) {
        if (notNull[i]) {
          if (!readInt128(values[i],
                          static_cast<int32_t>(scaleBuffer[i]))) {
            if (throwOnOverflow) {
              throw ParseError("Hive 0.11 decimal was more than 38 digits.");
            } else {
              *errorStream << "Warning: "
                           << "Hive 0.11 decimal with more than 38 digits "
                           << "replaced by NULL.\n";
              notNull[i] = false;
            }
          }
        }
      }
    } else {
      for(size_t i=0; i < numValues; ++i) {
        if (!readInt128(values[i],
                        static_cast<int32_t>(scaleBuffer[i]))) {
          if (throwOnOverflow) {
            throw ParseError("Hive 0.11 decimal was more than 38 digits.");
          } else {
            *errorStream << "Warning: "
                         << "Hive 0.11 decimal with more than 38 digits "
                         << "replaced by NULL.\n";
            batch.hasNulls = true;
            batch.notNull[i] = false;
          }
        }
      }
    }
  }

  /**
   * Create a reader for the given stripe.
   */
  std::unique_ptr<ColumnReader> buildReader(const Type& type,
                                            StripeStreams& stripe,
                                            MemoryPool* pool) {
    switch (static_cast<int>(type.getKind())) {
    case DATE:
    case INT:
    case LONG:
    case SHORT:
      return std::unique_ptr<ColumnReader>(
          new IntegerColumnReader(type, stripe, pool));
    case BINARY:
    case CHAR:
    case STRING:
    case VARCHAR:
      switch (static_cast<int>(stripe.getEncoding(type.getColumnId()).kind())){
      case proto::ColumnEncoding_Kind_DICTIONARY:
      case proto::ColumnEncoding_Kind_DICTIONARY_V2:
        return std::unique_ptr<ColumnReader>(
            new StringDictionaryColumnReader(type, stripe, pool));
      case proto::ColumnEncoding_Kind_DIRECT:
      case proto::ColumnEncoding_Kind_DIRECT_V2:
        return std::unique_ptr<ColumnReader>(
            new StringDirectColumnReader(type, stripe, pool));
      default:
        throw NotImplementedYet("buildReader unhandled string encoding");
      }

    case BOOLEAN:
      return std::unique_ptr<ColumnReader>(
          new BooleanColumnReader(type, stripe, pool));

    case BYTE:
      return std::unique_ptr<ColumnReader>(
          new ByteColumnReader(type, stripe, pool));

    case LIST:
      return std::unique_ptr<ColumnReader>(
          new ListColumnReader(type, stripe, pool));

    case MAP:
      return std::unique_ptr<ColumnReader>(
          new MapColumnReader(type, stripe, pool));

    case STRUCT:
      return std::unique_ptr<ColumnReader>(
          new StructColumnReader(type, stripe, pool));

    case FLOAT:
    case DOUBLE:
      return std::unique_ptr<ColumnReader>(
          new DoubleColumnReader(type, stripe, pool));

    case TIMESTAMP:
      return std::unique_ptr<ColumnReader>
        (new TimestampColumnReader(type, stripe, pool));

    case DECIMAL:
      // is this a Hive 0.11 or 0.12 file?
      if (type.getPrecision() == 0) {
        return std::unique_ptr<ColumnReader>
          (new DecimalHive11ColumnReader(type, stripe, pool));

      // can we represent the values using int64_t?
      } else if (type.getPrecision() <=
                 Decimal64ColumnReader::MAX_PRECISION_64) {
        return std::unique_ptr<ColumnReader>
          (new Decimal64ColumnReader(type, stripe, pool));

      // otherwise we use the Int128 implementation
      } else {
        return std::unique_ptr<ColumnReader>
          (new Decimal128ColumnReader(type, stripe, pool));
      }

    case UNION:
    default:
      throw NotImplementedYet("buildReader unhandled type");
    }
  }

}
