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
#include "RLEs.hh"

namespace orc {

  StripeStreams::~StripeStreams() {
    // PASS
  }

  inline RleVersion convertRleVersion(proto::ColumnEncoding_Kind kind) {
    switch (kind) {
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
                             StripeStreams& stripe
                             ): columnId(type.getColumnId()) {
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
      unsigned long bufferSize = std::min(MAX_BUFFER_SIZE, numValues);
      char buffer[MAX_BUFFER_SIZE];
      unsigned long remaining = numValues;
      while (remaining > 0) {
        unsigned long chunkSize = std::min(remaining, bufferSize);
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

  class IntegerColumnReader: public ColumnReader {
  private:
    std::unique_ptr<orc::RleDecoder> rle;

  public:
    IntegerColumnReader(const Type& type, StripeStreams& stipe);
    ~IntegerColumnReader();

    unsigned long skip(unsigned long numValues) override;

    void next(ColumnVectorBatch& rowBatch,
              unsigned long numValues,
              char* notNull) override;
  };

  IntegerColumnReader::IntegerColumnReader(const Type& type,
                                           StripeStreams& stripe
                                           ): ColumnReader(type, stripe) {
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
    std::unique_ptr<char[]> dictionaryBlob;
    std::unique_ptr<long[]> dictionaryOffset;
    std::unique_ptr<RleDecoder> rle;
    unsigned int dictionaryCount;
    
  public:
    StringDictionaryColumnReader(const Type& type, StripeStreams& stipe);
    ~StringDictionaryColumnReader();

    unsigned long skip(unsigned long numValues) override;

    void next(ColumnVectorBatch& rowBatch,
              unsigned long numValues,
              char *notNull) override;
  };

  StringDictionaryColumnReader::StringDictionaryColumnReader
      (const Type& type,
       StripeStreams& stripe
       ): ColumnReader(type, stripe) {
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
    dictionaryOffset = std::unique_ptr<long[]>(new long[dictionaryCount+1]);
    long* lengthArray = dictionaryOffset.get();
    lengthDecoder->next(lengthArray + 1, dictionaryCount, 0);
    lengthArray[0] = 0;
    for(unsigned int i=1; i < dictionaryCount + 1; ++i) {
      lengthArray[i] += lengthArray[i-1];
    }
    long blobSize = lengthArray[dictionaryCount];
    dictionaryBlob = std::unique_ptr<char[]>(new char[blobSize]);
    std::unique_ptr<SeekableInputStream> blobStream =
      stripe.getStream(columnId, proto::Stream_Kind_DICTIONARY_DATA);
    readFully(dictionaryBlob.get(), blobSize, blobStream.get());
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
    char *blob = dictionaryBlob.get();
    long *dictionaryOffsets = dictionaryOffset.get();
    char **outputStarts = byteBatch.data.data();
    long *outputLengths = byteBatch.length.data();
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
    size_t computeSize(const long *lengths, const char *notNull,
                       unsigned long numValues);

  public:
    StringDirectColumnReader(const Type& type, StripeStreams& stipe);
    ~StringDirectColumnReader();

    unsigned long skip(unsigned long numValues) override;

    void next(ColumnVectorBatch& rowBatch,
              unsigned long numValues,
              char *notNull) override;
  };

  StringDirectColumnReader::StringDirectColumnReader(const Type& type,
                                                     StripeStreams& stripe
                                                     ): ColumnReader(type,
                                                                     stripe) {
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
    long buffer[BUFFER_SIZE];
    unsigned long done = 0;
    size_t totalBytes = 0;
    // read the lengths, so we know haw many bytes to skip
    while (done < numValues) {
      unsigned long step = std::min(BUFFER_SIZE, numValues - done);
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

  size_t StringDirectColumnReader::computeSize(const long* lengths,
                                               const char* notNull,
                                               unsigned long numValues) {
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
    long *lengthPtr = byteBatch.length.data();

    // read the length vector
    lengthRle->next(lengthPtr, numValues, notNull);

    // figure out the total length of data we need from the blob stream
    const size_t totalLength = computeSize(lengthPtr, notNull, numValues);

    // Load data from the blob stream into our buffer until we have enough
    // to get the rest directly out of the stream's buffer.
    size_t bytesBuffered = 0;
    blobBuffer.reserve(totalLength);
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
             (usedBytes + static_cast<size_t>(lengthPtr[filledSlots]) <=
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
    std::vector<std::unique_ptr<ColumnReader> > children;

  public:
    StructColumnReader(const Type& type,
                       StripeStreams& stipe);
    ~StructColumnReader();

    unsigned long skip(unsigned long numValues) override;

    void next(ColumnVectorBatch& rowBatch,
              unsigned long numValues,
              char *notNull) override;
  };

  StructColumnReader::StructColumnReader(const Type& type,
                                         StripeStreams& stripe
                                         ): ColumnReader(type, stripe) {
    // count the number of selected sub-columns
    const bool *selectedColumns = stripe.getSelectedColumns();
    switch (stripe.getEncoding(columnId).kind()) {
    case proto::ColumnEncoding_Kind_DIRECT:
      for(unsigned int i=0; i < type.getSubtypeCount(); ++i) {
        const Type& child = type.getSubtype(i);
        if (selectedColumns[child.getColumnId()]) {
          children.push_back(buildReader(child, stripe));
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
    // PASS
  }

  unsigned long StructColumnReader::skip(unsigned long numValues) {
    numValues = ColumnReader::skip(numValues);
    for(auto ptr=children.cbegin(); ptr != children.cend(); ++ptr) {
      ptr->get()->skip(numValues);
    }
    return numValues;
  }

  void StructColumnReader::next(ColumnVectorBatch& rowBatch,
                                unsigned long numValues,
                                char *notNull) {
    ColumnReader::next(rowBatch, numValues, notNull);
    std::unique_ptr<ColumnVectorBatch> *childBatch = 
      dynamic_cast<StructVectorBatch&>(rowBatch).fields.data();
    unsigned int i=0;
    for(auto ptr=children.cbegin(); ptr != children.cend(); ++ptr, ++i) {
      ptr->get()->next(*(childBatch[i]), numValues,
                       rowBatch.hasNulls ? rowBatch.notNull.data(): 0);
    }
  }

  /**
   * Create a reader for the given stripe.
   */
  std::unique_ptr<ColumnReader> buildReader(const Type& type,
                                            StripeStreams& stripe) {
    switch (type.getKind()) {
    case BYTE:
    case SHORT:
    case INT:
    case LONG:
      return std::unique_ptr<ColumnReader>(new IntegerColumnReader(type,
                                                                   stripe));
    case BINARY:
    case CHAR:
    case STRING:
    case VARCHAR:
      switch (stripe.getEncoding(type.getColumnId()).kind()) {
      case proto::ColumnEncoding_Kind_DICTIONARY:
      case proto::ColumnEncoding_Kind_DICTIONARY_V2:
        return std::unique_ptr<ColumnReader>(new StringDictionaryColumnReader
                                             (type, stripe));
      case proto::ColumnEncoding_Kind_DIRECT:
      case proto::ColumnEncoding_Kind_DIRECT_V2:
        return std::unique_ptr<ColumnReader>(new StringDirectColumnReader
                                             (type, stripe));
      default:
        throw NotImplementedYet("buildReader unhandled string encoding");
      }

    case STRUCT:
      return std::unique_ptr<ColumnReader>(new StructColumnReader(type,
                                                                  stripe));
    case FLOAT:
    case DOUBLE:
    case BOOLEAN:
    case TIMESTAMP:
    case LIST:
    case MAP:
    case UNION:
    case DECIMAL:
    case DATE:
    default:
      throw NotImplementedYet("buildReader unhandled type");
    }
  }

}
