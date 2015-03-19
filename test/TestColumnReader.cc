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

#include "ColumnReader.hh"
#include "Exceptions.hh"

#include "wrap/orc-proto-wrapper.hh"
#include "wrap/gtest-wrapper.h"
#include "wrap/gmock.h"

#include <iostream>
#include <vector>

namespace orc {

class MockStripeStreams: public StripeStreams {
public:
  ~MockStripeStreams();
  std::unique_ptr<SeekableInputStream> getStream(int columnId,
                                                 proto::Stream_Kind kind,
                                                 bool stream) const override;
  MOCK_CONST_METHOD0(getReaderOptions, const ReaderOptions&());
  MOCK_CONST_METHOD0(getSelectedColumns, const std::vector<bool>());
  MOCK_CONST_METHOD1(getEncoding, proto::ColumnEncoding (int));
  MOCK_CONST_METHOD3(getStreamProxy, SeekableInputStream*
                     (int, proto::Stream_Kind, bool));
  MemoryPool& getMemoryPool() const {
    return *getDefaultPool();
  }
};

MockStripeStreams::~MockStripeStreams() {
  // PASS
}

std::unique_ptr<SeekableInputStream>
MockStripeStreams::getStream(int columnId,
                             proto::Stream_Kind kind,
                             bool shouldStream) const {
  return std::unique_ptr < SeekableInputStream >
    (getStreamProxy(columnId, kind, shouldStream));
}

TEST(TestColumnReader, testBooleanWithNulls) {
  MockStripeStreams streams;

  // set getSelectedColumns()
  std::vector<bool> selectedColumns = { true, true };
  EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(nullptr));
  // alternate 4 non-null and 4 null via [0xf0 for x in range(512 / 8)]
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x3d, 0xf0 })));

  // [0x0f for x in range(256 / 8)]
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x1d, 0x0f })));

  // create the row type
  std::unique_ptr<Type> rowType =
      createStructType( { createPrimitiveType(BOOLEAN) }, { "col0" });
  rowType->assignIds(0);

  std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);
  LongVectorBatch *longBatch = new LongVectorBatch(1024, *getDefaultPool());
  StructVectorBatch batch(1024, *getDefaultPool());
  batch.fields.push_back(longBatch);
  reader->next(batch, 512, 0);
  ASSERT_EQ(512, batch.numElements);
  ASSERT_EQ(false, batch.hasNulls);
  ASSERT_EQ(512, longBatch->numElements);
  ASSERT_EQ(true, longBatch->hasNulls);
  unsigned int next = 0;
  for (size_t i = 0; i < batch.numElements; ++i) {
    if (i & 4) {
      EXPECT_EQ(0, longBatch->notNull[i]) << "Wrong value at " << i;
    } else {
      EXPECT_EQ(1, longBatch->notNull[i]) << "Wrong value at " << i;
      EXPECT_EQ((next++ & 4) != 0, longBatch->data[i])
          << "Wrong value at " << i;
    }
  }
}

TEST(TestColumnReader, testBooleanSkipsWithNulls) {
  MockStripeStreams streams;

  // set getSelectedColumns()
  std::vector<bool> selectedColumns = { true, true };
  EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(nullptr));
  // alternate 4 non-null and 4 null via [0xf0 for x in range(512 / 8)]
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x3d, 0xf0 })));
  // [0x0f for x in range(128 / 8)]
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x1d, 0x0f })));

  // create the row type
  std::unique_ptr<Type> rowType =
      createStructType( { createPrimitiveType(BOOLEAN) }, { "col0" });
  rowType->assignIds(0);

  std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);
  LongVectorBatch *longBatch = new LongVectorBatch(1024, *getDefaultPool());
  StructVectorBatch batch(1024, *getDefaultPool());
  batch.fields.push_back(longBatch);
  reader->next(batch, 1, 0);
  ASSERT_EQ(1, batch.numElements);
  ASSERT_EQ(false, batch.hasNulls);
  ASSERT_EQ(1, longBatch->numElements);
  ASSERT_EQ(false, longBatch->hasNulls);
  EXPECT_EQ(0, longBatch->data[0]);
  reader->skip(506);
  reader->next(batch, 5, 0);
  ASSERT_EQ(5, batch.numElements);
  ASSERT_EQ(false, batch.hasNulls);
  ASSERT_EQ(5, longBatch->numElements);
  ASSERT_EQ(true, longBatch->hasNulls);
  EXPECT_EQ(1, longBatch->data[0]);
  EXPECT_EQ(false, longBatch->notNull[1]);
  EXPECT_EQ(false, longBatch->notNull[2]);
  EXPECT_EQ(false, longBatch->notNull[3]);
  EXPECT_EQ(false, longBatch->notNull[4]);
}

TEST(TestColumnReader, testByteWithNulls) {
  MockStripeStreams streams;

  // set getSelectedColumns()
  std::vector<bool> selectedColumns = { true, true };
  EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(nullptr));
  // alternate 4 non-null and 4 null via [0xf0 for x in range(512 / 8)]
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x3d, 0xf0 })));

  // range(256)
  char buffer[258];
  buffer[0] = static_cast<char>(0x80);
  for (unsigned int i = 0; i < 128; ++i) {
    buffer[i + 1] = static_cast<char>(i);
  }
  buffer[129] = static_cast<char>(0x80);
  for (unsigned int i = 128; i < 256; ++i) {
    buffer[i + 2] = static_cast<char>(i);
  }
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      (buffer, 258)));

  // create the row type
  std::unique_ptr<Type> rowType =
      createStructType( { createPrimitiveType(BYTE) }, { "col0" });
  rowType->assignIds(0);

  std::unique_ptr<ColumnReader> reader =
      buildReader(*rowType, streams);
  LongVectorBatch *longBatch = new LongVectorBatch(1024, *getDefaultPool());
  StructVectorBatch batch(1024, *getDefaultPool());
  batch.fields.push_back(longBatch);
  reader->next(batch, 512, 0);
  ASSERT_EQ(512, batch.numElements);
  ASSERT_EQ(false, batch.hasNulls);
  ASSERT_EQ(512, longBatch->numElements);
  ASSERT_EQ(true, longBatch->hasNulls);
  unsigned int next = 0;
  for (size_t i = 0; i < batch.numElements; ++i) {
    if (i & 4) {
      EXPECT_EQ(0, longBatch->notNull[i]) << "Wrong value at " << i;
    } else {
      EXPECT_EQ(1, longBatch->notNull[i]) << "Wrong value at " << i;
      EXPECT_EQ(static_cast<char>(next++), longBatch->data[i])
          << "Wrong value at " << i;
    }
  }
}

TEST(TestColumnReader, testByteSkipsWithNulls) {
  MockStripeStreams streams;

  // set getSelectedColumns()
  std::vector<bool> selectedColumns = { true, true };
  EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(nullptr));
  // alternate 4 non-null and 4 null via [0xf0 for x in range(512 / 8)]
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x3d, 0xf0 })));

  // range(256)
  char buffer[258];
  buffer[0] = static_cast<char>(0x80);
  for (unsigned int i = 0; i < 128; ++i) {
    buffer[i + 1] = static_cast<char>(i);
  }
  buffer[129] = static_cast<char>(0x80);
  for (unsigned int i = 128; i < 256; ++i) {
    buffer[i + 2] = static_cast<char>(i);
  }
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      (buffer, 258)));

  // create the row type
  std::unique_ptr<Type> rowType =
      createStructType( { createPrimitiveType(BYTE) }, { "col0" });
  rowType->assignIds(0);

  std::unique_ptr<ColumnReader> reader =
      buildReader(*rowType, streams);
  LongVectorBatch *longBatch = new LongVectorBatch(1024, *getDefaultPool());
  StructVectorBatch batch(1024, *getDefaultPool());
  batch.fields.push_back(longBatch);
  reader->next(batch, 1, 0);
  ASSERT_EQ(1, batch.numElements);
  ASSERT_EQ(false, batch.hasNulls);
  ASSERT_EQ(1, longBatch->numElements);
  ASSERT_EQ(false, longBatch->hasNulls);
  EXPECT_EQ(0, longBatch->data[0]);
  reader->skip(506);
  reader->next(batch, 5, 0);
  ASSERT_EQ(5, batch.numElements);
  ASSERT_EQ(false, batch.hasNulls);
  ASSERT_EQ(5, longBatch->numElements);
  ASSERT_EQ(true, longBatch->hasNulls);
  EXPECT_EQ(static_cast<char>(-1), longBatch->data[0]);
  EXPECT_EQ(false, longBatch->notNull[1]);
  EXPECT_EQ(false, longBatch->notNull[2]);
  EXPECT_EQ(false, longBatch->notNull[3]);
  EXPECT_EQ(false, longBatch->notNull[4]);
}

TEST(TestColumnReader, testIntegerWithNulls) {
  MockStripeStreams streams;

  // set getSelectedColumns()
  std::vector<bool> selectedColumns = { true, true };
  EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(nullptr));
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x16, 0xf0 })));
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x64, 0x01, 0x00 })));

  // create the row type
  std::unique_ptr<Type> rowType =
      createStructType( { createPrimitiveType(INT) }, { "myInt" });
  rowType->assignIds(0);

  std::unique_ptr<ColumnReader> reader =
      buildReader(*rowType, streams);
  LongVectorBatch *longBatch = new LongVectorBatch(1024, *getDefaultPool());
  StructVectorBatch batch(1024, *getDefaultPool());
  batch.fields.push_back(longBatch);
  reader->next(batch, 200, 0);
  ASSERT_EQ(200, batch.numElements);
  ASSERT_EQ(false, batch.hasNulls);
  ASSERT_EQ(200, longBatch->numElements);
  ASSERT_EQ(true, longBatch->hasNulls);
  long next = 0;
  for (size_t i = 0; i < batch.numElements; ++i) {
    if (i & 4) {
      EXPECT_EQ(0, longBatch->notNull[i]);
    } else {
      EXPECT_EQ(1, longBatch->notNull[i]);
      EXPECT_EQ(next++, longBatch->data[i]);
    }
  }
}

TEST(TestColumnReader, testDictionaryWithNulls) {
  MockStripeStreams streams;

  // set getSelectedColumns()
  std::vector<bool> selectedColumns = { true, true };
  EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncoding(0))
      .WillRepeatedly(testing::Return(directEncoding));
  proto::ColumnEncoding dictionaryEncoding;
  dictionaryEncoding.set_kind(proto::ColumnEncoding_Kind_DICTIONARY);
  dictionaryEncoding.set_dictionarysize(2);
  EXPECT_CALL(streams, getEncoding(1))
      .WillRepeatedly(testing::Return(dictionaryEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(nullptr));
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x19, 0xf0 })));
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x2f, 0x00, 0x00,
          0x2f, 0x00, 0x01 })));
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DICTIONARY_DATA,
                                      false))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x4f, 0x52, 0x43, 0x4f, 0x77,
          0x65, 0x6e })));
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, false))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x02, 0x01, 0x03 })));

  // create the row type
  std::unique_ptr<Type> rowType =
      createStructType( { createPrimitiveType(STRING) }, { "myString" });
  rowType->assignIds(0);

  std::unique_ptr<ColumnReader> reader =
      buildReader(*rowType, streams);
  StringVectorBatch *stringBatch = new StringVectorBatch(1024,
                                                         *getDefaultPool());
  StructVectorBatch batch(1024, *getDefaultPool());
  batch.fields.push_back(stringBatch);
  reader->next(batch, 200, 0);
  ASSERT_EQ(200, batch.numElements);
  ASSERT_EQ(false, batch.hasNulls);
  ASSERT_EQ(200, stringBatch->numElements);
  ASSERT_EQ(true, stringBatch->hasNulls);
  for (size_t i = 0; i < batch.numElements; ++i) {
    if (i & 4) {
      EXPECT_EQ(0, stringBatch->notNull[i]);
    } else {
      EXPECT_EQ(1, stringBatch->notNull[i]);
      const char* expected = i < 98 ? "ORC" : "Owen";
      ASSERT_EQ(strlen(expected), stringBatch->length[i])
      << "Wrong length at " << i;
      for (size_t letter = 0; letter < strlen(expected); ++letter) {
        EXPECT_EQ(expected[letter], stringBatch->data[i][letter])
            << "Wrong contents at " << i << ", " << letter;
      }
    }
  }
}

TEST(TestColumnReader, testVarcharDictionaryWithNulls) {
  MockStripeStreams streams;

  // set getSelectedColumns()
  std::vector<bool> selectedColumns = { true, true, true, true };
  EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncoding(0))
      .WillRepeatedly(testing::Return(directEncoding));

  proto::ColumnEncoding dictionary2Encoding;
  dictionary2Encoding.set_kind(proto::ColumnEncoding_Kind_DICTIONARY);
  dictionary2Encoding.set_dictionarysize(2);
  EXPECT_CALL(streams, getEncoding(1))
      .WillRepeatedly(testing::Return(dictionary2Encoding));

  proto::ColumnEncoding dictionary0Encoding;
  dictionary0Encoding.set_kind(proto::ColumnEncoding_Kind_DICTIONARY);
  dictionary0Encoding.set_dictionarysize(0);
  EXPECT_CALL(streams, getEncoding(testing::Ge(2)))
      .WillRepeatedly(testing::Return(dictionary0Encoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(nullptr));

  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x16, 0xff })));
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x61, 0x00, 0x01,
          0x61, 0x00, 0x00 })));
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DICTIONARY_DATA,
                                      false))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x4f, 0x52, 0x43, 0x4f, 0x77,
          0x65, 0x6e })));
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, false))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x02, 0x01, 0x03 })));

  EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x16, 0x00 })));
  EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { })));
  EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_DICTIONARY_DATA,
                                      false))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { })));
  EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_LENGTH, false))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { })));

  EXPECT_CALL(streams, getStreamProxy(3, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x16, 0x00 })));
  EXPECT_CALL(streams, getStreamProxy(3, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { })));
  EXPECT_CALL(streams, getStreamProxy(3, proto::Stream_Kind_DICTIONARY_DATA,
                                      false))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { })));
  EXPECT_CALL(streams, getStreamProxy(3, proto::Stream_Kind_LENGTH, false))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { })));

  // create the row type
  std::unique_ptr<Type> rowType =
      createStructType( { createPrimitiveType(VARCHAR),
          createPrimitiveType(CHAR),
          createPrimitiveType(STRING) },
          { "col0", "col1", "col2" });
  rowType->assignIds(0);

  std::unique_ptr<ColumnReader> reader =
      buildReader(*rowType, streams);
  StructVectorBatch batch(1024, *getDefaultPool());
  StringVectorBatch *stringBatch = new StringVectorBatch(1024,
                                                         *getDefaultPool());
  StringVectorBatch *nullBatch = new StringVectorBatch(1024,
                                                       *getDefaultPool());
  batch.fields.push_back(stringBatch);
  batch.fields.push_back(nullBatch);
  reader->next(batch, 200, 0);
  ASSERT_EQ(200, batch.numElements);
  ASSERT_EQ(false, batch.hasNulls);
  ASSERT_EQ(200, stringBatch->numElements);
  ASSERT_EQ(false, stringBatch->hasNulls);
  ASSERT_EQ(200, nullBatch->numElements);
  ASSERT_EQ(true, nullBatch->hasNulls);
  for (size_t i = 0; i < batch.numElements; ++i) {
    EXPECT_EQ(true, stringBatch->notNull[i]);
    EXPECT_EQ(false, nullBatch->notNull[i]);
    const char* expected = i < 100 ? "Owen" : "ORC";
    ASSERT_EQ(strlen(expected), stringBatch->length[i])
    << "Wrong length at " << i;
    for (size_t letter = 0; letter < strlen(expected); ++letter) {
      EXPECT_EQ(expected[letter], stringBatch->data[i][letter])
          << "Wrong contents at " << i << ", " << letter;
    }
  }
}

TEST(TestColumnReader, testSubstructsWithNulls) {
  MockStripeStreams streams;

  // set getSelectedColumns()
  std::vector<bool> selectedColumns = { true, true, true, true };
  EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(nullptr));

  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x16, 0x0f })));

  EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x0a, 0x55 })));

  EXPECT_CALL(streams, getStreamProxy(3, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x04, 0xf0 })));
  EXPECT_CALL(streams, getStreamProxy(3, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x17, 0x01, 0x00 })));

  // create the row type
  std::unique_ptr<Type> rowType =
      createStructType
      ( { createStructType
          ( { createStructType
              ( { createPrimitiveType(LONG) }, { "col2" }) },
              { "col1" }) },
          { "col0" });
  rowType->assignIds(0);

  std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

  StructVectorBatch batch(1024, *getDefaultPool());
  StructVectorBatch *middle = new StructVectorBatch(1024, *getDefaultPool());
  StructVectorBatch *inner = new StructVectorBatch(1024, *getDefaultPool());
  LongVectorBatch *longs = new LongVectorBatch(1024, *getDefaultPool());
  batch.fields.push_back(middle);
  middle->fields.push_back(inner);
  inner->fields.push_back(longs);
  reader->next(batch, 200, 0);
  ASSERT_EQ(200, batch.numElements);
  ASSERT_EQ(false, batch.hasNulls);
  ASSERT_EQ(200, middle->numElements);
  ASSERT_EQ(true, middle->hasNulls);
  ASSERT_EQ(200, inner->numElements);
  ASSERT_EQ(true, inner->hasNulls);
  ASSERT_EQ(200, longs->numElements);
  ASSERT_EQ(true, longs->hasNulls);
  long middleCount = 0;
  long innerCount = 0;
  long longCount = 0;
  for (size_t i = 0; i < batch.numElements; ++i) {
    if (i & 4) {
      EXPECT_EQ(true, middle->notNull[i]) << "Wrong at " << i;
      if (middleCount++ & 1) {
        EXPECT_EQ(true, inner->notNull[i]) << "Wrong at " << i;
        if (innerCount++ & 4) {
          EXPECT_EQ(false, longs->notNull[i]) << "Wrong at " << i;
        } else {
          EXPECT_EQ(true, longs->notNull[i]) << "Wrong at " << i;
          EXPECT_EQ(longCount++, longs->data[i]) << "Wrong at " << i;
        }
      } else {
        EXPECT_EQ(false, inner->notNull[i]) << "Wrong at " << i;
        EXPECT_EQ(false, longs->notNull[i]) << "Wrong at " << i;
      }
    } else {
      EXPECT_EQ(false, middle->notNull[i]) << "Wrong at " << i;
      EXPECT_EQ(false, inner->notNull[i]) << "Wrong at " << i;
      EXPECT_EQ(false, longs->notNull[i]) << "Wrong at " << i;
    }
  }
}

TEST(TestColumnReader, testSkipWithNulls) {
  MockStripeStreams streams;

  // set getSelectedColumns()
  std::vector<bool> selectedColumns = { true, true, true };
  EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));
  proto::ColumnEncoding dictionaryEncoding;
  dictionaryEncoding.set_kind(proto::ColumnEncoding_Kind_DICTIONARY);
  dictionaryEncoding.set_dictionarysize(100);
  EXPECT_CALL(streams, getEncoding(2))
      .WillRepeatedly(testing::Return(dictionaryEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(nullptr));
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x03, 0x00, 0xff, 0x3f, 0x08, 0xff,
          0xff, 0xfc, 0x03, 0x00 })));
  EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x03, 0x00, 0xff, 0x3f, 0x08, 0xff,
          0xff, 0xfc, 0x03, 0x00 })));
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x61, 0x01, 0x00 })));
  EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x61, 0x01, 0x00 })));

  // fill the dictionary with '00' to '99'
  char digits[200];
  for (int i = 0; i < 10; ++i) {
    for (int j = 0; j < 10; ++j) {
      digits[2 * (10 * i + j)] = '0' + static_cast<char>(i);
      digits[2 * (10 * i + j) + 1] = '0' + static_cast<char>(j);
    }
  }
  EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_DICTIONARY_DATA,
                                      false))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      (digits, 200)));
  EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_LENGTH, false))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x61, 0x00, 0x02 })));

  // create the row type
  std::unique_ptr<Type> rowType =
      createStructType( { createPrimitiveType(INT),
          createPrimitiveType(STRING) }, { "myInt", "myString" });
  rowType->assignIds(0);

  std::unique_ptr<ColumnReader> reader =
      buildReader(*rowType, streams);
  StructVectorBatch batch(100, *getDefaultPool());
  LongVectorBatch *longBatch = new LongVectorBatch(100, *getDefaultPool());
  StringVectorBatch *stringBatch =
    new StringVectorBatch(100, *getDefaultPool());
  batch.fields.push_back(longBatch);
  batch.fields.push_back(stringBatch);
  reader->next(batch, 20, 0);
  ASSERT_EQ(20, batch.numElements);
  ASSERT_EQ(20, longBatch->numElements);
  ASSERT_EQ(20, stringBatch->numElements);
  ASSERT_EQ(false, batch.hasNulls);
  ASSERT_EQ(true, longBatch->hasNulls);
  ASSERT_EQ(true, stringBatch->hasNulls);
  for (size_t i = 0; i < 20; ++i) {
    EXPECT_EQ(false, longBatch->notNull[i]) << "Wrong at " << i;
    EXPECT_EQ(false, stringBatch->notNull[i]) << "Wrong at " << i;
  }
  reader->skip(30);
  reader->next(batch, 100, 0);
  ASSERT_EQ(100, batch.numElements);
  ASSERT_EQ(false, batch.hasNulls);
  ASSERT_EQ(false, longBatch->hasNulls);
  ASSERT_EQ(false, stringBatch->hasNulls);
  for (size_t i = 0; i < 10; ++i) {
    for (size_t j = 0; j < 10; ++j) {
      size_t k = 10 * i + j;
      EXPECT_EQ(1, longBatch->notNull[k]) << "Wrong at " << k;
      ASSERT_EQ(2, stringBatch->length[k])<< "Wrong at " << k;
      EXPECT_EQ('0' + static_cast<char>(i), stringBatch->data[k][0])
          << "Wrong at " << k;
      EXPECT_EQ('0' + static_cast<char>(j), stringBatch->data[k][1])
          << "Wrong at " << k;
    }
  }
  reader->skip(50);
}

TEST(TestColumnReader, testBinaryDirect) {
  MockStripeStreams streams;

  // set getSelectedColumns()
  std::vector<bool> selectedColumns = { true, true };
  EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(nullptr));

  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(nullptr));

  char blob[200];
  for (size_t i = 0; i < 10; ++i) {
    for (size_t j = 0; j < 10; ++j) {
      blob[2 * (10 * i + j)] = static_cast<char>(i);
      blob[2 * (10 * i + j) + 1] = static_cast<char>(j);
    }
  }
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      (blob, 200)));

  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x61, 0x00, 0x02 })));

  // create the row type
  std::unique_ptr<Type> rowType =
      createStructType( { createPrimitiveType(BINARY) }, { "col0" });
  rowType->assignIds(0);

  std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

  StructVectorBatch batch(1024, *getDefaultPool());
  StringVectorBatch *strings = new StringVectorBatch(1024, *getDefaultPool());
  batch.fields.push_back(strings);
  for (size_t i = 0; i < 2; ++i) {
    reader->next(batch, 50, 0);
    ASSERT_EQ(50, batch.numElements);
    ASSERT_EQ(false, batch.hasNulls);
    ASSERT_EQ(50, strings->numElements);
    ASSERT_EQ(false, strings->hasNulls);
    for (size_t j = 0; j < batch.numElements; ++j) {
      ASSERT_EQ(2, strings->length[j]);
      EXPECT_EQ((50 * i + j) / 10, strings->data[j][0]);
      EXPECT_EQ((50 * i + j) % 10, strings->data[j][1]);
    }
  }
}

TEST(TestColumnReader, testBinaryDirectWithNulls) {
  MockStripeStreams streams;

  // set getSelectedColumns()
  std::vector<bool> selectedColumns = { true, true };
  EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(nullptr));

  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x1d, 0xf0 })));

  char blob[256];
  for (size_t i = 0; i < 8; ++i) {
    for (size_t j = 0; j < 16; ++j) {
      blob[2 * (16 * i + j)] = 'A' + static_cast<char>(i);
      blob[2 * (16 * i + j) + 1] = 'A' + static_cast<char>(j);
    }
  }
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      (blob, 256)));

  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x7d, 0x00, 0x02 })));

  // create the row type
  std::unique_ptr<Type> rowType =
      createStructType( { createPrimitiveType(BINARY) }, { "col0" });
  rowType->assignIds(0);

  std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

  StructVectorBatch batch(1024, *getDefaultPool());
  StringVectorBatch *strings = new StringVectorBatch(1024, *getDefaultPool());
  batch.fields.push_back(strings);
  size_t next = 0;
  for (size_t i = 0; i < 2; ++i) {
    reader->next(batch, 128, 0);
    ASSERT_EQ(128, batch.numElements);
    ASSERT_EQ(false, batch.hasNulls);
    ASSERT_EQ(128, strings->numElements);
    ASSERT_EQ(true, strings->hasNulls);
    for (size_t j = 0; j < batch.numElements; ++j) {
      ASSERT_EQ(((128 * i + j) & 4) == 0, strings->notNull[j]);
      if (strings->notNull[j]) {
        ASSERT_EQ(2, strings->length[j]);
        EXPECT_EQ('A' + static_cast<char>(next / 16), strings->data[j][0]);
        EXPECT_EQ('A' + static_cast<char>(next % 16), strings->data[j][1]);
        next += 1;
      }
    }
  }
}

TEST(TestColumnReader, testShortBlobError) {
  MockStripeStreams streams;

  // set getSelectedColumns()
  std::vector<bool> selectedColumns = { true, true };
  EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(nullptr));

  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(nullptr));

  char blob[100];
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      (blob, 100)));

  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x61, 0x00, 0x02 })));

  // create the row type
  std::unique_ptr<Type> rowType =
      createStructType( { createPrimitiveType(STRING) }, { "col0" });
  rowType->assignIds(0);

  std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

  StructVectorBatch batch(1024, *getDefaultPool());
  StringVectorBatch *strings = new StringVectorBatch(1024, *getDefaultPool());
  batch.fields.push_back(strings);
  EXPECT_THROW(reader->next(batch, 100, 0), ParseError);
}

TEST(TestColumnReader, testStringDirectShortBuffer) {
  MockStripeStreams streams;

  // set getSelectedColumns()
  std::vector<bool> selectedColumns = { true, true };
  EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(nullptr));

  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(nullptr));

  char blob[200];
  for (size_t i = 0; i < 10; ++i) {
    for (size_t j = 0; j < 10; ++j) {
      blob[2 * (10 * i + j)] = static_cast<char>(i);
      blob[2 * (10 * i + j) + 1] = static_cast<char>(j);
    }
  }
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      (blob, 200, 3)));

  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x61, 0x00, 0x02 })));

  // create the row type
  std::unique_ptr<Type> rowType =
      createStructType( { createPrimitiveType(STRING) }, { "col0" });
  rowType->assignIds(0);

  std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

  StructVectorBatch batch(25, *getDefaultPool());
  StringVectorBatch *strings = new StringVectorBatch(25, *getDefaultPool());
  batch.fields.push_back(strings);
  for (size_t i = 0; i < 4; ++i) {
    reader->next(batch, 25, 0);
    ASSERT_EQ(25, batch.numElements);
    ASSERT_EQ(false, batch.hasNulls);
    ASSERT_EQ(25, strings->numElements);
    ASSERT_EQ(false, strings->hasNulls);
    for (size_t j = 0; j < batch.numElements; ++j) {
      ASSERT_EQ(2, strings->length[j]);
      EXPECT_EQ((25 * i + j) / 10, strings->data[j][0]);
      EXPECT_EQ((25 * i + j) % 10, strings->data[j][1]);
    }
  }
}

TEST(TestColumnReader, testStringDirectShortBufferWithNulls) {
  MockStripeStreams streams;

  // set getSelectedColumns()
  std::vector<bool> selectedColumns = { true, true };
  EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(nullptr));

  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x3d, 0xf0 })));

  char blob[512];
  for (size_t i = 0; i < 16; ++i) {
    for (size_t j = 0; j < 16; ++j) {
      blob[2 * (16 * i + j)] = 'A' + static_cast<char>(i);
      blob[2 * (16 * i + j) + 1] = 'A' + static_cast<char>(j);
    }
  }
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      (blob, 512, 30)));

  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x7d, 0x00, 0x02, 0x7d, 0x00, 0x02 })));

  // create the row type
  std::unique_ptr<Type> rowType =
      createStructType( { createPrimitiveType(STRING) }, { "col0" });
  rowType->assignIds(0);

  std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

  StructVectorBatch batch(64, *getDefaultPool());
  StringVectorBatch *strings = new StringVectorBatch(64, *getDefaultPool());
  batch.fields.push_back(strings);
  size_t next = 0;
  for (size_t i = 0; i < 8; ++i) {
    reader->next(batch, 64, 0);
    ASSERT_EQ(64, batch.numElements);
    ASSERT_EQ(false, batch.hasNulls);
    ASSERT_EQ(64, strings->numElements);
    ASSERT_EQ(true, strings->hasNulls);
    for (size_t j = 0; j < batch.numElements; ++j) {
      ASSERT_EQ((j & 4) == 0, strings->notNull[j]);
      if (strings->notNull[j]) {
        ASSERT_EQ(2, strings->length[j]);
        EXPECT_EQ('A' + next / 16, strings->data[j][0]);
        EXPECT_EQ('A' + next % 16, strings->data[j][1]);
        next += 1;
      }
    }
  }
}

TEST(TestColumnReader, testStringDirectSkip) {
  MockStripeStreams streams;

  // set getSelectedColumns()
  std::vector<bool> selectedColumns = { true, true };
  EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(nullptr));

  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(nullptr));

  // sum(0 to 1199)
  const size_t BLOB_SIZE = 719400;
  char blob[BLOB_SIZE];
  size_t posn = 0;
  for (size_t item = 0; item < 1200; ++item) {
    for (size_t ch = 0; ch < item; ++ch) {
      blob[posn++] = static_cast<char>(ch);
    }
  }
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      (blob, BLOB_SIZE, 200)));

  // the stream of 0 to 1199
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x7f, 0x01, 0x00,
          0x7f, 0x01, 0x82, 0x01,
          0x7f, 0x01, 0x84, 0x02,
          0x7f, 0x01, 0x86, 0x03,
          0x7f, 0x01, 0x88, 0x04,
          0x7f, 0x01, 0x8a, 0x05,
          0x7f, 0x01, 0x8c, 0x06,
          0x7f, 0x01, 0x8e, 0x07,
          0x7f, 0x01, 0x90, 0x08,
          0x1b, 0x01, 0x92, 0x09 })));

  // create the row type
  std::unique_ptr<Type> rowType =
      createStructType( { createPrimitiveType(STRING) }, { "col0" });
  rowType->assignIds(0);

  std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

  StructVectorBatch batch(2, *getDefaultPool());
  StringVectorBatch *strings = new StringVectorBatch(2, *getDefaultPool());
  batch.fields.push_back(strings);
  reader->next(batch, 2, 0);
  ASSERT_EQ(2, batch.numElements);
  ASSERT_EQ(false, batch.hasNulls);
  ASSERT_EQ(2, strings->numElements);
  ASSERT_EQ(false, strings->hasNulls);
  for (size_t i = 0; i < batch.numElements; ++i) {
    ASSERT_EQ(i, strings->length[i]);
    for (size_t j = 0; j < i; ++j) {
      EXPECT_EQ(static_cast<char>(j), strings->data[i][j]);
    }
  }
  reader->skip(14);
  reader->next(batch, 2, 0);
  ASSERT_EQ(2, batch.numElements);
  ASSERT_EQ(false, batch.hasNulls);
  ASSERT_EQ(2, strings->numElements);
  ASSERT_EQ(false, strings->hasNulls);
  for (size_t i = 0; i < batch.numElements; ++i) {
    ASSERT_EQ(16 + i, strings->length[i]);
    for (size_t j = 0; j < 16 + i; ++j) {
      EXPECT_EQ(static_cast<char>(j), strings->data[i][j]);
    }
  }
  reader->skip(1180);
  reader->next(batch, 2, 0);
  ASSERT_EQ(2, batch.numElements);
  ASSERT_EQ(false, batch.hasNulls);
  ASSERT_EQ(2, strings->numElements);
  ASSERT_EQ(false, strings->hasNulls);
  for (size_t i = 0; i < batch.numElements; ++i) {
    ASSERT_EQ(1198 + i, strings->length[i]);
    for (size_t j = 0; j < 1198 + i; ++j) {
      EXPECT_EQ(static_cast<char>(j), strings->data[i][j]);
    }
  }
}

TEST(TestColumnReader, testStringDirectSkipWithNulls) {
  MockStripeStreams streams;

  // set getSelectedColumns()
  std::vector<bool> selectedColumns = { true, true };
  EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(nullptr));

  // alternate 4 non-null and 4 null via [0xf0 for x in range(2400 / 8)]
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x7f, 0xf0, 0x7f, 0xf0, 0x25, 0xf0 })));

  // sum(range(1200))
  const size_t BLOB_SIZE = 719400;

  // each string is [x % 256 for x in range(r)]
  char blob[BLOB_SIZE];
  size_t posn = 0;
  for (size_t item = 0; item < 1200; ++item) {
    for (size_t ch = 0; ch < item; ++ch) {
      blob[posn++] = static_cast<char>(ch);
    }
  }
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      (blob, BLOB_SIZE, 200)));

  // range(1200)
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x7f, 0x01, 0x00,
          0x7f, 0x01, 0x82, 0x01,
          0x7f, 0x01, 0x84, 0x02,
          0x7f, 0x01, 0x86, 0x03,
          0x7f, 0x01, 0x88, 0x04,
          0x7f, 0x01, 0x8a, 0x05,
          0x7f, 0x01, 0x8c, 0x06,
          0x7f, 0x01, 0x8e, 0x07,
          0x7f, 0x01, 0x90, 0x08,
          0x1b, 0x01, 0x92, 0x09 })));

  // create the row type
  std::unique_ptr<Type> rowType =
      createStructType( { createPrimitiveType(STRING) }, { "col0" });
  rowType->assignIds(0);

  std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

  StructVectorBatch batch(2, *getDefaultPool());
  StringVectorBatch *strings = new StringVectorBatch(2, *getDefaultPool());
  batch.fields.push_back(strings);
  reader->next(batch, 2, 0);
  ASSERT_EQ(2, batch.numElements);
  ASSERT_EQ(false, batch.hasNulls);
  ASSERT_EQ(2, strings->numElements);
  ASSERT_EQ(false, strings->hasNulls);
  for (size_t i = 0; i < batch.numElements; ++i) {
    ASSERT_EQ(i, strings->length[i]);
    for (size_t j = 0; j < i; ++j) {
      EXPECT_EQ(static_cast<char>(j), strings->data[i][j]);
    }
  }
  reader->skip(30);
  reader->next(batch, 2, 0);
  ASSERT_EQ(2, batch.numElements);
  ASSERT_EQ(false, batch.hasNulls);
  ASSERT_EQ(2, strings->numElements);
  ASSERT_EQ(false, strings->hasNulls);
  for (size_t i = 0; i < batch.numElements; ++i) {
    ASSERT_EQ(16 + i, strings->length[i]);
    for (size_t j = 0; j < 16 + i; ++j) {
      EXPECT_EQ(static_cast<char>(j), strings->data[i][j]);
    }
  }
  reader->skip(2364);
  reader->next(batch, 2, 0);
  ASSERT_EQ(2, batch.numElements);
  ASSERT_EQ(false, batch.hasNulls);
  ASSERT_EQ(2, strings->numElements);
  ASSERT_EQ(true, strings->hasNulls);
  for (size_t i = 0; i < batch.numElements; ++i) {
    EXPECT_EQ(false, strings->notNull[i]);
  }
}

TEST(TestColumnReader, testList) {
  MockStripeStreams streams;

  // set getSelectedColumns()
  std::vector<bool> selectedColumns = { true, true, true };
  EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(testing::_,
          proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(nullptr));

  // [2 for x in range(600)]
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x7f, 0x00, 0x02,
          0x7f, 0x00, 0x02,
          0x7f, 0x00, 0x02,
          0x7f, 0x00, 0x02,
          0x4d, 0x00, 0x02 })));

  // range(1200)
  EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x7f, 0x01, 0x00,
          0x7f, 0x01, 0x84, 0x02,
          0x7f, 0x01, 0x88, 0x04,
          0x7f, 0x01, 0x8c, 0x06,
          0x7f, 0x01, 0x90, 0x08,
          0x7f, 0x01, 0x94, 0x0a,
          0x7f, 0x01, 0x98, 0x0c,
          0x7f, 0x01, 0x9c, 0x0e,
          0x7f, 0x01, 0xa0, 0x10,
          0x1b, 0x01, 0xa4, 0x12 })));

  // create the row type
  std::unique_ptr<Type> rowType =
      createStructType( { createListType(createPrimitiveType(LONG)) },
          { "col0" });
  rowType->assignIds(0);

  std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

  StructVectorBatch batch(512, *getDefaultPool());
  ListVectorBatch *lists = new ListVectorBatch(512, *getDefaultPool());
  LongVectorBatch *longs = new LongVectorBatch(512, *getDefaultPool());
  batch.fields.push_back(lists);
  lists->elements = std::unique_ptr < ColumnVectorBatch > (longs);
  reader->next(batch, 512, 0);
  ASSERT_EQ(512, batch.numElements);
  ASSERT_EQ(false, batch.hasNulls);
  ASSERT_EQ(512, lists->numElements);
  ASSERT_EQ(false, lists->hasNulls);
  ASSERT_EQ(1024, longs->numElements);
  ASSERT_EQ(false, longs->hasNulls);
  for (size_t i = 0; i <= batch.numElements; ++i) {
    EXPECT_EQ(2 * i, lists->offsets[i]);
  }
  for (size_t i = 0; i < longs->numElements; ++i) {
    EXPECT_EQ(i, longs->data[i]);
  }
}

TEST(TestColumnReader, testListWithNulls) {
  MockStripeStreams streams;

  // set getSelectedColumns()
  std::vector<bool> selectedColumns = { true, true, true };
  EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(nullptr));

  // [0xaa for x in range(2048/8)]
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x7f, 0xaa, 0x7b, 0xaa })));

  EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(nullptr));

  // [1 for x in range(260)] +
  // [4 for x in range(260)] +
  // [0 for x in range(260)] +
  // [3 for x in range(243)] +
  // [19]
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x7f, 0x00, 0x01,
          0x7f, 0x00, 0x01,
          0x7f, 0x00, 0x04,
          0x7f, 0x00, 0x04,
          0x7f, 0x00, 0x00,
          0x7f, 0x00, 0x00,
          0x7f, 0x00, 0x03,
          0x6e, 0x00, 0x03,
          0xff, 0x13 })));

  // range(2048)
  EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x7f, 0x01, 0x00,
          0x7f, 0x01, 0x84, 0x02,
          0x7f, 0x01, 0x88, 0x04,
          0x7f, 0x01, 0x8c, 0x06,
          0x7f, 0x01, 0x90, 0x08,
          0x7f, 0x01, 0x94, 0x0a,
          0x7f, 0x01, 0x98, 0x0c,
          0x7f, 0x01, 0x9c, 0x0e,
          0x7f, 0x01, 0xa0, 0x10,
          0x7f, 0x01, 0xa4, 0x12,
          0x7f, 0x01, 0xa8, 0x14,
          0x7f, 0x01, 0xac, 0x16,
          0x7f, 0x01, 0xb0, 0x18,
          0x7f, 0x01, 0xb4, 0x1a,
          0x7f, 0x01, 0xb8, 0x1c,
          0x5f, 0x01, 0xbc, 0x1e })));

  // create the row type
  std::unique_ptr<Type> rowType =
      createStructType( { createListType(createPrimitiveType(LONG)) },
          { "col0" });
  rowType->assignIds(0);

  std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

  StructVectorBatch batch(512, *getDefaultPool());
  ListVectorBatch *lists = new ListVectorBatch(512, *getDefaultPool());
  LongVectorBatch *longs = new LongVectorBatch(512, *getDefaultPool());
  batch.fields.push_back(lists);
  lists->elements = std::unique_ptr < ColumnVectorBatch > (longs);
  reader->next(batch, 512, 0);
  ASSERT_EQ(512, batch.numElements);
  ASSERT_EQ(false, batch.hasNulls);
  ASSERT_EQ(512, lists->numElements);
  ASSERT_EQ(true, lists->hasNulls);
  ASSERT_EQ(256, longs->numElements);
  ASSERT_EQ(false, longs->hasNulls);
  for (size_t i = 0; i < batch.numElements; ++i) {
    EXPECT_EQ(i % 2 == 0, lists->notNull[i]) << "Wrong value at " << i;
    EXPECT_EQ((i + 1) / 2, lists->offsets[i]) << "Wrong value at " << i;
  }
  EXPECT_EQ(256, lists->offsets[512]);
  for (size_t i = 0; i < longs->numElements; ++i) {
    EXPECT_EQ(i, longs->data[i]);
  }

  reader->next(batch, 512, 0);
  ASSERT_EQ(512, batch.numElements);
  ASSERT_EQ(false, batch.hasNulls);
  ASSERT_EQ(512, lists->numElements);
  ASSERT_EQ(true, lists->hasNulls);
  ASSERT_EQ(1012, longs->numElements);
  ASSERT_EQ(false, longs->hasNulls);
  for (size_t i = 0; i < batch.numElements; ++i) {
    EXPECT_EQ(i % 2 == 0, lists->notNull[i]) << "Wrong value at " << i;
    if (i < 8) {
      EXPECT_EQ((i + 1) / 2, lists->offsets[i])
          << "Wrong value at " << i;
    } else {
      EXPECT_EQ(4 * ((i + 1) / 2) - 12, lists->offsets[i])
          << "Wrong value at " << i;
    }
  }
  EXPECT_EQ(1012, lists->offsets[512]);
  for (size_t i = 0; i < longs->numElements; ++i) {
    EXPECT_EQ(256 + i, longs->data[i]);
  }

  reader->next(batch, 512, 0);
  ASSERT_EQ(512, batch.numElements);
  ASSERT_EQ(false, batch.hasNulls);
  ASSERT_EQ(512, lists->numElements);
  ASSERT_EQ(true, lists->hasNulls);
  ASSERT_EQ(32, longs->numElements);
  ASSERT_EQ(false, longs->hasNulls);
  for (size_t i = 0; i < batch.numElements; ++i) {
    EXPECT_EQ(i % 2 == 0, lists->notNull[i]) << "Wrong value at " << i;
    if (i < 16) {
      EXPECT_EQ(4 * ((i + 1) / 2), lists->offsets[i])
          << "Wrong value at " << i;
    } else {
      EXPECT_EQ(32, lists->offsets[i]) << "Wrong value at " << i;
    }
  }
  EXPECT_EQ(32, lists->offsets[512]);
  for (size_t i = 0; i < longs->numElements; ++i) {
    EXPECT_EQ(1268 + i, longs->data[i]);
  }

  reader->next(batch, 512, 0);
  ASSERT_EQ(512, batch.numElements);
  ASSERT_EQ(false, batch.hasNulls);
  ASSERT_EQ(512, lists->numElements);
  ASSERT_EQ(true, lists->hasNulls);
  ASSERT_EQ(748, longs->numElements);
  ASSERT_EQ(false, longs->hasNulls);
  for (size_t i = 0; i < batch.numElements; ++i) {
    EXPECT_EQ(i % 2 == 0, lists->notNull[i]) << "Wrong value at " << i;
    if (i < 24) {
      EXPECT_EQ(0, lists->offsets[i]) << "Wrong value at " << i;
    } else if (i < 510) {
      EXPECT_EQ(3 * ((i - 23) / 2), lists->offsets[i])
          << "Wrong value at " << i;
    } else if (i < 511) {
      EXPECT_EQ(729, lists->offsets[i]) << "Wrong value at " << i;
    } else {
      EXPECT_EQ(748, lists->offsets[i]) << "Wrong value at " << i;
    }
  }
  EXPECT_EQ(748, lists->offsets[512]);
  for (size_t i = 0; i < longs->numElements; ++i) {
    EXPECT_EQ(1300 + i, longs->data[i]);
  }
}

TEST(TestColumnReader, testListSkipWithNulls) {
  MockStripeStreams streams;

  // set getSelectedColumns()
  std::vector<bool> selectedColumns = { true, true, true };
  EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(nullptr));

  // [0xaa for x in range(2048/8)]
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x7f, 0xaa, 0x7b, 0xaa })));

  EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(nullptr));

  // [1 for x in range(260)] +
  // [4 for x in range(260)] +
  // [0 for x in range(260)] +
  // [3 for x in range(243)] +
  // [19]
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x7f, 0x00, 0x01,
          0x7f, 0x00, 0x01,
          0x7f, 0x00, 0x04,
          0x7f, 0x00, 0x04,
          0x7f, 0x00, 0x00,
          0x7f, 0x00, 0x00,
          0x7f, 0x00, 0x03,
          0x6e, 0x00, 0x03,
          0xff, 0x13 })));

  // range(2048)
  EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x7f, 0x01, 0x00,
          0x7f, 0x01, 0x84, 0x02,
          0x7f, 0x01, 0x88, 0x04,
          0x7f, 0x01, 0x8c, 0x06,
          0x7f, 0x01, 0x90, 0x08,
          0x7f, 0x01, 0x94, 0x0a,
          0x7f, 0x01, 0x98, 0x0c,
          0x7f, 0x01, 0x9c, 0x0e,
          0x7f, 0x01, 0xa0, 0x10,
          0x7f, 0x01, 0xa4, 0x12,
          0x7f, 0x01, 0xa8, 0x14,
          0x7f, 0x01, 0xac, 0x16,
          0x7f, 0x01, 0xb0, 0x18,
          0x7f, 0x01, 0xb4, 0x1a,
          0x7f, 0x01, 0xb8, 0x1c,
          0x5f, 0x01, 0xbc, 0x1e })));

  // create the row type
  std::unique_ptr<Type> rowType =
      createStructType( { createListType(createPrimitiveType(LONG)) },
          { "col0" });
  rowType->assignIds(0);

  std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

  StructVectorBatch batch(1, *getDefaultPool());
  ListVectorBatch *lists = new ListVectorBatch(1, *getDefaultPool());
  LongVectorBatch *longs = new LongVectorBatch(1, *getDefaultPool());
  batch.fields.push_back(lists);
  lists->elements = std::unique_ptr < ColumnVectorBatch > (longs);

  reader->next(batch, 1, 0);
  ASSERT_EQ(1, batch.numElements);
  ASSERT_EQ(false, batch.hasNulls);
  ASSERT_EQ(1, lists->numElements);
  ASSERT_EQ(false, lists->hasNulls);
  ASSERT_EQ(1, longs->numElements);
  ASSERT_EQ(false, longs->hasNulls);
  EXPECT_EQ(0, lists->offsets[0]);
  EXPECT_EQ(1, lists->offsets[1]);
  EXPECT_EQ(0, longs->data[0]);

  reader->skip(13);
  reader->next(batch, 1, 0);
  ASSERT_EQ(1, batch.numElements);
  ASSERT_EQ(false, batch.hasNulls);
  ASSERT_EQ(1, lists->numElements);
  ASSERT_EQ(false, lists->hasNulls);
  ASSERT_EQ(1, longs->numElements);
  ASSERT_EQ(false, longs->hasNulls);
  EXPECT_EQ(0, lists->offsets[0]);
  EXPECT_EQ(1, lists->offsets[1]);
  EXPECT_EQ(7, longs->data[0]);

  reader->skip(2031);
  reader->next(batch, 2, 0);
  ASSERT_EQ(2, batch.numElements);
  ASSERT_EQ(false, batch.hasNulls);
  ASSERT_EQ(2, lists->numElements);
  ASSERT_EQ(true, lists->hasNulls);
  ASSERT_EQ(19, longs->numElements);
  ASSERT_EQ(false, longs->hasNulls);
  EXPECT_EQ(0, lists->offsets[0]);
  EXPECT_EQ(19, lists->offsets[1]);
  EXPECT_EQ(19, lists->offsets[2]);
  for (size_t i = 0; i < longs->numElements; ++i) {
    EXPECT_EQ(2029 + i, longs->data[i]);
  }
}

TEST(TestColumnReader, testListSkipWithNullsNoData) {
  MockStripeStreams streams;

  // set getSelectedColumns()
  std::vector<bool> selectedColumns = { true, true, true };
  EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(nullptr));

  // [0xaa for x in range(2048/8)]
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x7f, 0xaa, 0x7b, 0xaa })));

  EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(nullptr));

  // [1 for x in range(260)] +
  // [4 for x in range(260)] +
  // [0 for x in range(260)] +
  // [3 for x in range(243)] +
  // [19]
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x7f, 0x00, 0x01,
          0x7f, 0x00, 0x01,
          0x7f, 0x00, 0x04,
          0x7f, 0x00, 0x04,
          0x7f, 0x00, 0x00,
          0x7f, 0x00, 0x00,
          0x7f, 0x00, 0x03,
          0x6e, 0x00, 0x03,
          0xff, 0x13 })));

  EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(testing::Return(nullptr));

  // create the row type
  std::unique_ptr<Type> rowType =
      createStructType( { createListType(createPrimitiveType(LONG)) },
          { "col0" });
  rowType->assignIds(0);

  std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

  StructVectorBatch batch(1, *getDefaultPool());
  ListVectorBatch *lists = new ListVectorBatch(1, *getDefaultPool());
  batch.fields.push_back(lists);

  reader->next(batch, 1, 0);
  ASSERT_EQ(1, batch.numElements);
  ASSERT_EQ(false, batch.hasNulls);
  ASSERT_EQ(1, lists->numElements);
  ASSERT_EQ(false, lists->hasNulls);
  EXPECT_EQ(0, lists->offsets[0]);
  EXPECT_EQ(1, lists->offsets[1]);

  reader->skip(13);
  reader->next(batch, 1, 0);
  ASSERT_EQ(1, batch.numElements);
  ASSERT_EQ(false, batch.hasNulls);
  ASSERT_EQ(1, lists->numElements);
  ASSERT_EQ(false, lists->hasNulls);
  EXPECT_EQ(0, lists->offsets[0]);
  EXPECT_EQ(1, lists->offsets[1]);

  reader->skip(2031);
  reader->next(batch, 2, 0);
  ASSERT_EQ(2, batch.numElements);
  ASSERT_EQ(false, batch.hasNulls);
  ASSERT_EQ(2, lists->numElements);
  ASSERT_EQ(true, lists->hasNulls);
  EXPECT_EQ(0, lists->offsets[0]);
  EXPECT_EQ(19, lists->offsets[1]);
  EXPECT_EQ(19, lists->offsets[2]);
}

TEST(TestColumnReader, testMap) {
  MockStripeStreams streams;

  // set getSelectedColumns()
  std::vector<bool> selectedColumns = { true, true, true, true };
  EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(testing::_,
          proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(nullptr));

  // [2 for x in range(600)]
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x7f, 0x00, 0x02,
          0x7f, 0x00, 0x02,
          0x7f, 0x00, 0x02,
          0x7f, 0x00, 0x02,
          0x4d, 0x00, 0x02 })));

  // range(1200)
  EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x7f, 0x01, 0x00,
          0x7f, 0x01, 0x84, 0x02,
          0x7f, 0x01, 0x88, 0x04,
          0x7f, 0x01, 0x8c, 0x06,
          0x7f, 0x01, 0x90, 0x08,
          0x7f, 0x01, 0x94, 0x0a,
          0x7f, 0x01, 0x98, 0x0c,
          0x7f, 0x01, 0x9c, 0x0e,
          0x7f, 0x01, 0xa0, 0x10,
          0x1b, 0x01, 0xa4, 0x12 })));

  // range(8, 1208)
  EXPECT_CALL(streams, getStreamProxy(3, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x7f, 0x01, 0x10,
          0x7f, 0x01, 0x94, 0x02,
          0x7f, 0x01, 0x98, 0x04,
          0x7f, 0x01, 0x9c, 0x06,
          0x7f, 0x01, 0xa0, 0x08,
          0x7f, 0x01, 0xa4, 0x0a,
          0x7f, 0x01, 0xa8, 0x0c,
          0x7f, 0x01, 0xac, 0x0e,
          0x7f, 0x01, 0xb0, 0x10,
          0x1b, 0x01, 0xb4, 0x12 })));

  // create the row type
  std::unique_ptr<Type> rowType =
      createStructType( { createMapType(createPrimitiveType(LONG),
          createPrimitiveType(LONG)) },
          { "col0", "col1" });
  rowType->assignIds(0);

  std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

  StructVectorBatch batch(512, *getDefaultPool());
  MapVectorBatch *maps = new MapVectorBatch(512, *getDefaultPool());
  LongVectorBatch *keys = new LongVectorBatch(512, *getDefaultPool());
  LongVectorBatch *elements = new LongVectorBatch(512, *getDefaultPool());
  batch.fields.push_back(maps);
  maps->keys = std::unique_ptr < ColumnVectorBatch > (keys);
  maps->elements = std::unique_ptr < ColumnVectorBatch > (elements);
  reader->next(batch, 512, 0);
  ASSERT_EQ(512, batch.numElements);
  ASSERT_EQ(false, batch.hasNulls);
  ASSERT_EQ(512, maps->numElements);
  ASSERT_EQ(false, maps->hasNulls);
  ASSERT_EQ(1024, keys->numElements);
  ASSERT_EQ(false, keys->hasNulls);
  ASSERT_EQ(1024, elements->numElements);
  ASSERT_EQ(false, elements->hasNulls);
  for (size_t i = 0; i <= batch.numElements; ++i) {
    EXPECT_EQ(2 * i, maps->offsets[i]);
  }
  for (size_t i = 0; i < keys->numElements; ++i) {
    EXPECT_EQ(i, keys->data[i]);
    EXPECT_EQ(i + 8, elements->data[i]);
  }
}

TEST(TestColumnReader, testMapWithNulls) {
  MockStripeStreams streams;

  // set getSelectedColumns()
  std::vector<bool> selectedColumns = { true, true, true, true };
  EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(nullptr));

  // [0xaa for x in range(2048/8)]
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x7f, 0xaa, 0x7b, 0xaa })));

  EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(nullptr));

  // [0x55 for x in range(2048/8)]
  EXPECT_CALL(streams, getStreamProxy(3, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x7f, 0x55, 0x7b, 0x55 })));

  // [1 for x in range(260)] +
  // [4 for x in range(260)] +
  // [0 for x in range(260)] +
  // [3 for x in range(243)] +
  // [19]
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x7f, 0x00, 0x01,
          0x7f, 0x00, 0x01,
          0x7f, 0x00, 0x04,
          0x7f, 0x00, 0x04,
          0x7f, 0x00, 0x00,
          0x7f, 0x00, 0x00,
          0x7f, 0x00, 0x03,
          0x6e, 0x00, 0x03,
          0xff, 0x13 })));

  // range(2048)
  EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x7f, 0x01, 0x00,
          0x7f, 0x01, 0x84, 0x02,
          0x7f, 0x01, 0x88, 0x04,
          0x7f, 0x01, 0x8c, 0x06,
          0x7f, 0x01, 0x90, 0x08,
          0x7f, 0x01, 0x94, 0x0a,
          0x7f, 0x01, 0x98, 0x0c,
          0x7f, 0x01, 0x9c, 0x0e,
          0x7f, 0x01, 0xa0, 0x10,
          0x7f, 0x01, 0xa4, 0x12,
          0x7f, 0x01, 0xa8, 0x14,
          0x7f, 0x01, 0xac, 0x16,
          0x7f, 0x01, 0xb0, 0x18,
          0x7f, 0x01, 0xb4, 0x1a,
          0x7f, 0x01, 0xb8, 0x1c,
          0x5f, 0x01, 0xbc, 0x1e })));

  // range(8, 1032)
  EXPECT_CALL(streams, getStreamProxy(3, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x7f, 0x01, 0x10,
          0x7f, 0x01, 0x94, 0x02,
          0x7f, 0x01, 0x98, 0x04,
          0x7f, 0x01, 0x9c, 0x06,
          0x7f, 0x01, 0xa0, 0x08,
          0x7f, 0x01, 0xa4, 0x0a,
          0x7f, 0x01, 0xa8, 0x0c,
          0x6f, 0x01, 0xac, 0x0e })));

  // create the row type
  std::unique_ptr<Type> rowType =
      createStructType( { createMapType(createPrimitiveType(LONG),
          createPrimitiveType(LONG)) },
          { "col0", "col1" });
  rowType->assignIds(0);

  std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

  StructVectorBatch batch(512, *getDefaultPool());
  MapVectorBatch *maps = new MapVectorBatch(512, *getDefaultPool());
  LongVectorBatch *keys = new LongVectorBatch(512, *getDefaultPool());
  LongVectorBatch *elements = new LongVectorBatch(512, *getDefaultPool());
  batch.fields.push_back(maps);
  maps->keys = std::unique_ptr < ColumnVectorBatch > (keys);
  maps->elements = std::unique_ptr < ColumnVectorBatch > (elements);
  reader->next(batch, 512, 0);
  ASSERT_EQ(512, batch.numElements);
  ASSERT_EQ(false, batch.hasNulls);
  ASSERT_EQ(512, maps->numElements);
  ASSERT_EQ(true, maps->hasNulls);
  ASSERT_EQ(256, keys->numElements);
  ASSERT_EQ(false, keys->hasNulls);
  ASSERT_EQ(256, elements->numElements);
  ASSERT_EQ(true, elements->hasNulls);
  for (size_t i = 0; i < batch.numElements; ++i) {
    EXPECT_EQ(i % 2 == 0, maps->notNull[i]) << "Wrong value at " << i;
    EXPECT_EQ((i + 1) / 2, maps->offsets[i]) << "Wrong value at " << i;
  }
  EXPECT_EQ(256, maps->offsets[512]);
  for (size_t i = 0; i < keys->numElements; ++i) {
    EXPECT_EQ(i, keys->data[i]);
    EXPECT_EQ(i & 1, elements->notNull[i]);
    if (elements->notNull[i]) {
      EXPECT_EQ(i / 2 + 8, elements->data[i]);
    }
  }

  reader->next(batch, 512, 0);
  ASSERT_EQ(512, batch.numElements);
  ASSERT_EQ(false, batch.hasNulls);
  ASSERT_EQ(512, maps->numElements);
  ASSERT_EQ(true, maps->hasNulls);
  ASSERT_EQ(1012, keys->numElements);
  ASSERT_EQ(false, keys->hasNulls);
  ASSERT_EQ(1012, elements->numElements);
  ASSERT_EQ(true, elements->hasNulls);
  for (size_t i = 0; i < batch.numElements; ++i) {
    EXPECT_EQ(i % 2 == 0, maps->notNull[i]) << "Wrong value at " << i;
    if (i < 8) {
      EXPECT_EQ((i + 1) / 2, maps->offsets[i])
          << "Wrong value at " << i;
    } else {
      EXPECT_EQ(4 * ((i + 1) / 2) - 12, maps->offsets[i])
          << "Wrong value at " << i;
    }
  }
  EXPECT_EQ(1012, maps->offsets[512]);
  for (size_t i = 0; i < keys->numElements; ++i) {
    EXPECT_EQ(256 + i, keys->data[i]);
    EXPECT_EQ(i & 1, elements->notNull[i]);
    if (elements->notNull[i]) {
      EXPECT_EQ(128 + 8 + i / 2, elements->data[i]);
    }
  }

  reader->next(batch, 512, 0);
  ASSERT_EQ(512, batch.numElements);
  ASSERT_EQ(false, batch.hasNulls);
  ASSERT_EQ(512, maps->numElements);
  ASSERT_EQ(true, maps->hasNulls);
  ASSERT_EQ(32, keys->numElements);
  ASSERT_EQ(false, keys->hasNulls);
  ASSERT_EQ(32, elements->numElements);
  ASSERT_EQ(true, elements->hasNulls);
  for (size_t i = 0; i < batch.numElements; ++i) {
    EXPECT_EQ(i % 2 == 0, maps->notNull[i]) << "Wrong value at " << i;
    if (i < 16) {
      EXPECT_EQ(4 * ((i + 1) / 2), maps->offsets[i])
          << "Wrong value at " << i;
    } else {
      EXPECT_EQ(32, maps->offsets[i]) << "Wrong value at " << i;
    }
  }
  EXPECT_EQ(32, maps->offsets[512]);
  for (size_t i = 0; i < keys->numElements; ++i) {
    EXPECT_EQ(1268 + i, keys->data[i]);
    EXPECT_EQ(i & 1, elements->notNull[i]);
    if (elements->notNull[i]) {
      EXPECT_EQ(634 + 8 + i / 2, elements->data[i]);
    }
  }

  reader->next(batch, 512, 0);
  ASSERT_EQ(512, batch.numElements);
  ASSERT_EQ(false, batch.hasNulls);
  ASSERT_EQ(512, maps->numElements);
  ASSERT_EQ(true, maps->hasNulls);
  ASSERT_EQ(748, keys->numElements);
  ASSERT_EQ(false, keys->hasNulls);
  ASSERT_EQ(748, elements->numElements);
  ASSERT_EQ(true, elements->hasNulls);
  for (size_t i = 0; i < batch.numElements; ++i) {
    EXPECT_EQ(i % 2 == 0, maps->notNull[i]) << "Wrong value at " << i;
    if (i < 24) {
      EXPECT_EQ(0, maps->offsets[i]) << "Wrong value at " << i;
    } else if (i < 510) {
      EXPECT_EQ(3 * ((i - 23) / 2), maps->offsets[i])
          << "Wrong value at " << i;
    } else if (i < 511) {
      EXPECT_EQ(729, maps->offsets[i]) << "Wrong value at " << i;
    } else {
      EXPECT_EQ(748, maps->offsets[i]) << "Wrong value at " << i;
    }
  }
  EXPECT_EQ(748, maps->offsets[512]);
  for (size_t i = 0; i < keys->numElements; ++i) {
    EXPECT_EQ(1300 + i, keys->data[i]);
    EXPECT_EQ(i & 1, elements->notNull[i]);
    if (elements->notNull[i]) {
      EXPECT_EQ(650 + 8 + i / 2, elements->data[i]);
    }
  }
}

TEST(TestColumnReader, testMapSkipWithNulls) {
  MockStripeStreams streams;

  // set getSelectedColumns()
  std::vector<bool> selectedColumns = { true, true, true, true };
  EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(testing::_,proto::Stream_Kind_PRESENT,
                                      false))
      .WillRepeatedly(testing::Return(nullptr));

  // [0xaa for x in range(2048/8)]
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x7f, 0xaa, 0x7b, 0xaa })));

  // [1 for x in range(260)] +
  // [4 for x in range(260)] +
  // [0 for x in range(260)] +
  // [3 for x in range(243)] +
  // [19]
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x7f, 0x00, 0x01,
          0x7f, 0x00, 0x01,
          0x7f, 0x00, 0x04,
          0x7f, 0x00, 0x04,
          0x7f, 0x00, 0x00,
          0x7f, 0x00, 0x00,
          0x7f, 0x00, 0x03,
          0x6e, 0x00, 0x03,
          0xff, 0x13 })));

  // range(2048)
  EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x7f, 0x01, 0x00,
          0x7f, 0x01, 0x84, 0x02,
          0x7f, 0x01, 0x88, 0x04,
          0x7f, 0x01, 0x8c, 0x06,
          0x7f, 0x01, 0x90, 0x08,
          0x7f, 0x01, 0x94, 0x0a,
          0x7f, 0x01, 0x98, 0x0c,
          0x7f, 0x01, 0x9c, 0x0e,
          0x7f, 0x01, 0xa0, 0x10,
          0x7f, 0x01, 0xa4, 0x12,
          0x7f, 0x01, 0xa8, 0x14,
          0x7f, 0x01, 0xac, 0x16,
          0x7f, 0x01, 0xb0, 0x18,
          0x7f, 0x01, 0xb4, 0x1a,
          0x7f, 0x01, 0xb8, 0x1c,
          0x5f, 0x01, 0xbc, 0x1e })));

  // range(8, 2056)
  EXPECT_CALL(streams, getStreamProxy(3, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x7f, 0x01, 0x10,
          0x7f, 0x01, 0x94, 0x02,
          0x7f, 0x01, 0x98, 0x04,
          0x7f, 0x01, 0x9c, 0x06,
          0x7f, 0x01, 0xa0, 0x08,
          0x7f, 0x01, 0xa4, 0x0a,
          0x7f, 0x01, 0xa8, 0x0c,
          0x7f, 0x01, 0xac, 0x0e,
          0x7f, 0x01, 0xb0, 0x10,
          0x7f, 0x01, 0xb4, 0x12,
          0x7f, 0x01, 0xb8, 0x14,
          0x7f, 0x01, 0xbc, 0x16,
          0x7f, 0x01, 0xc0, 0x18,
          0x7f, 0x01, 0xc4, 0x1a,
          0x7f, 0x01, 0xc8, 0x1c,
          0x5f, 0x01, 0xcc, 0x1e })));

  // create the row type
  std::unique_ptr<Type> rowType =
      createStructType( { createMapType(createPrimitiveType(LONG),
          createPrimitiveType(LONG)) },
          { "col0", "col1" });
  rowType->assignIds(0);

  std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

  StructVectorBatch batch(1, *getDefaultPool());
  MapVectorBatch *maps = new MapVectorBatch(1, *getDefaultPool());
  LongVectorBatch *keys = new LongVectorBatch(1, *getDefaultPool());
  LongVectorBatch *elements = new LongVectorBatch(1, *getDefaultPool());
  batch.fields.push_back(maps);
  maps->keys = std::unique_ptr < ColumnVectorBatch > (keys);
  maps->elements = std::unique_ptr < ColumnVectorBatch > (elements);

  reader->next(batch, 1, 0);
  ASSERT_EQ(1, batch.numElements);
  ASSERT_EQ(false, batch.hasNulls);
  ASSERT_EQ(1, maps->numElements);
  ASSERT_EQ(false, maps->hasNulls);
  ASSERT_EQ(1, keys->numElements);
  ASSERT_EQ(false, keys->hasNulls);
  ASSERT_EQ(1, elements->numElements);
  ASSERT_EQ(false, elements->hasNulls);
  EXPECT_EQ(0, maps->offsets[0]);
  EXPECT_EQ(1, maps->offsets[1]);
  EXPECT_EQ(0, keys->data[0]);
  EXPECT_EQ(8, elements->data[0]);

  reader->skip(13);
  reader->next(batch, 1, 0);
  ASSERT_EQ(1, batch.numElements);
  ASSERT_EQ(false, batch.hasNulls);
  ASSERT_EQ(1, maps->numElements);
  ASSERT_EQ(false, maps->hasNulls);
  ASSERT_EQ(1, keys->numElements);
  ASSERT_EQ(false, keys->hasNulls);
  ASSERT_EQ(1, elements->numElements);
  ASSERT_EQ(false, elements->hasNulls);
  EXPECT_EQ(0, maps->offsets[0]);
  EXPECT_EQ(1, maps->offsets[1]);
  EXPECT_EQ(7, keys->data[0]);
  EXPECT_EQ(7 + 8, elements->data[0]);

  reader->skip(2031);
  reader->next(batch, 2, 0);
  ASSERT_EQ(2, batch.numElements);
  ASSERT_EQ(false, batch.hasNulls);
  ASSERT_EQ(2, maps->numElements);
  ASSERT_EQ(true, maps->hasNulls);
  ASSERT_EQ(19, keys->numElements);
  ASSERT_EQ(false, keys->hasNulls);
  ASSERT_EQ(19, elements->numElements);
  ASSERT_EQ(false, elements->hasNulls);
  EXPECT_EQ(0, maps->offsets[0]);
  EXPECT_EQ(19, maps->offsets[1]);
  EXPECT_EQ(19, maps->offsets[2]);
  for (size_t i = 0; i < keys->numElements; ++i) {
    EXPECT_EQ(2029 + i, keys->data[i]);
    EXPECT_EQ(2029 + 8 + i, elements->data[i]);
  }
}

TEST(TestColumnReader, testMapSkipWithNullsNoData) {
  MockStripeStreams streams;

  // set getSelectedColumns()
  std::vector<bool> selectedColumns = { true, true, true, false };
  EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(testing::_,proto::Stream_Kind_PRESENT,
                                      true))
      .WillRepeatedly(testing::Return(nullptr));

  // [0xaa for x in range(2048/8)]
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x7f, 0xaa, 0x7b, 0xaa })));

  // [1 for x in range(260)] +
  // [4 for x in range(260)] +
  // [0 for x in range(260)] +
  // [3 for x in range(243)] +
  // [19]
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x7f, 0x00, 0x01,
          0x7f, 0x00, 0x01,
          0x7f, 0x00, 0x04,
          0x7f, 0x00, 0x04,
          0x7f, 0x00, 0x00,
          0x7f, 0x00, 0x00,
          0x7f, 0x00, 0x03,
          0x6e, 0x00, 0x03,
          0xff, 0x13 })));

  // create the row type
  std::unique_ptr<Type> rowType =
      createStructType( { createMapType(createPrimitiveType(LONG),
          createPrimitiveType(LONG)) },
          { "col0", "col1" });
  rowType->assignIds(0);

  std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

  StructVectorBatch batch(1, *getDefaultPool());
  MapVectorBatch *maps = new MapVectorBatch(1, *getDefaultPool());
  batch.fields.push_back(maps);

  reader->next(batch, 1, 0);
  ASSERT_EQ(1, batch.numElements);
  ASSERT_EQ(false, batch.hasNulls);
  ASSERT_EQ(1, maps->numElements);
  ASSERT_EQ(false, maps->hasNulls);
  EXPECT_EQ(0, maps->offsets[0]);
  EXPECT_EQ(1, maps->offsets[1]);

  reader->skip(13);
  reader->next(batch, 1, 0);
  ASSERT_EQ(1, batch.numElements);
  ASSERT_EQ(false, batch.hasNulls);
  ASSERT_EQ(1, maps->numElements);
  ASSERT_EQ(false, maps->hasNulls);
  EXPECT_EQ(0, maps->offsets[0]);
  EXPECT_EQ(1, maps->offsets[1]);

  reader->skip(2031);
  reader->next(batch, 2, 0);
  ASSERT_EQ(2, batch.numElements);
  ASSERT_EQ(false, batch.hasNulls);
  ASSERT_EQ(2, maps->numElements);
  ASSERT_EQ(true, maps->hasNulls);
  EXPECT_EQ(0, maps->offsets[0]);
  EXPECT_EQ(19, maps->offsets[1]);
  EXPECT_EQ(19, maps->offsets[2]);
}

TEST(TestColumnReader, testFloatWithNulls) {
  MockStripeStreams streams;

  // set getSelectedColumns()
  std::vector<bool> selectedColumns = { true, true };
  EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(nullptr));

  // 13 non-nulls followed by 19 nulls
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0xfc, 0xff, 0xf8, 0x0, 0x0 })));

  float test_vals[] = { 1.0f, 2.5f, -100.125f, 10000.0f, 1.234567E23f,
      -2.3456E-12f,
      std::numeric_limits<float>::infinity(),
      std::numeric_limits<float>::quiet_NaN(),
      -std::numeric_limits<float>::infinity(),
      3.4028235E38f, -3.4028235E38f, 1.4e-45f, -1.4e-45f };
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x00, 0x00, 0x80, 0x3f,
          0x00, 0x00, 0x20, 0x40,
          0x00, 0x40, 0xc8, 0xc2,
          0x00, 0x40, 0x1c, 0x46,
          0xcf, 0x24, 0xd1, 0x65,
          0x93, 0xe, 0x25, 0xac,
          0x0, 0x0, 0x80, 0x7f,
          0x0, 0x0, 0xc0, 0x7f,
          0x0, 0x0, 0x80, 0xff,
          0xff, 0xff, 0x7f, 0x7f,
          0xff, 0xff, 0x7f, 0xff,
          0x1, 0x0, 0x0, 0x0,
          0x1, 0x0, 0x0, 0x80,
      })));
  // create the row type
  std::unique_ptr<Type> rowType =
      createStructType( { createPrimitiveType(FLOAT) }, { "myFloat" });
  rowType->assignIds(0);

  std::unique_ptr<ColumnReader> reader =
      buildReader(*rowType, streams);

  DoubleVectorBatch *doubleBatch = new DoubleVectorBatch(1024,
                                                         *getDefaultPool());
  StructVectorBatch batch(1024, *getDefaultPool());
  batch.fields.push_back(doubleBatch);
  reader->next(batch, 32, 0);
  ASSERT_EQ(32, batch.numElements);
  ASSERT_EQ(false, batch.hasNulls);
  ASSERT_EQ(32, doubleBatch->numElements);
  ASSERT_EQ(true, doubleBatch->hasNulls);

  for (size_t i = 0; i < batch.numElements; ++i) {
    if (i > 12) {
      EXPECT_EQ(0, doubleBatch->notNull[i]);
    } else if (i == 7) {
      EXPECT_EQ(1, doubleBatch->notNull[i]);
      EXPECT_EQ(true, isnan(doubleBatch->data[i]));
    } else {
      EXPECT_EQ(1, doubleBatch->notNull[i]);
      EXPECT_DOUBLE_EQ(test_vals[i], doubleBatch->data[i]);
    }
  }
}

TEST(TestColumnReader, testFloatSkipWithNulls) {
  MockStripeStreams streams;

  // set getSelectedColumns()
  std::vector<bool> selectedColumns = { true, true };
  EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(nullptr));

  // 2 non-nulls, 2 nulls, 2 non-nulls, 2 nulls
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0xff, 0xcc })));

  // 1, 2.5, -100.125, 10000
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x00, 0x00, 0x80, 0x3f,
          0x00, 0x00, 0x20, 0x40,
          0x00, 0x40, 0xc8, 0xc2,
          0x00, 0x40, 0x1c, 0x46
      })));
  // create the row type
  std::unique_ptr<Type> rowType =
      createStructType( { createPrimitiveType(FLOAT) }, { "myFloat" });
  rowType->assignIds(0);

  std::unique_ptr<ColumnReader> reader =
      buildReader(*rowType, streams);

  DoubleVectorBatch *doubleBatch = new DoubleVectorBatch(1024,
                                                         *getDefaultPool());
  StructVectorBatch batch(1024, *getDefaultPool());
  batch.fields.push_back(doubleBatch);

  float test_vals[] = { 1.0, 2.5, -100.125, 10000.0 };
  int vals_ix = 0;

  reader->next(batch, 3, 0);
  ASSERT_EQ(3, batch.numElements);
  ASSERT_EQ(false, batch.hasNulls);
  ASSERT_EQ(3, doubleBatch->numElements);
  ASSERT_EQ(true, doubleBatch->hasNulls);

  for (size_t i = 0; i < batch.numElements; ++i) {
    if (i > 1) {
      EXPECT_EQ(0, doubleBatch->notNull[i]);
    } else {
      EXPECT_EQ(1, doubleBatch->notNull[i]);
      EXPECT_DOUBLE_EQ(test_vals[vals_ix], doubleBatch->data[i]);
      vals_ix++;
    }
  }

  reader->skip(1);

  reader->next(batch, 4, 0);
  ASSERT_EQ(4, batch.numElements);
  ASSERT_EQ(false, batch.hasNulls);
  ASSERT_EQ(4, doubleBatch->numElements);
  ASSERT_EQ(true, doubleBatch->hasNulls);
  for (size_t i = 0; i < batch.numElements; ++i) {
    if (i > 1) {
      EXPECT_EQ(0, doubleBatch->notNull[i]);
    } else {
      EXPECT_EQ(1, doubleBatch->notNull[i]);
      EXPECT_DOUBLE_EQ(test_vals[vals_ix], doubleBatch->data[i]);
      vals_ix++;
    }
  }
}

TEST(TestColumnReader, testDoubleWithNulls) {
  MockStripeStreams streams;

  // set getSelectedColumns()
  std::vector<bool> selectedColumns = { true, true };
  EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(nullptr));

  // 13 non-nulls followed by 19 nulls
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0xfc, 0xff, 0xf8, 0x0, 0x0 })));

  double test_vals[] = { 1.0, 2.0, -2.0, 100.0, 1.23456789E32,
      -3.42234E-18,
      std::numeric_limits<double>::infinity(),
      std::numeric_limits<double>::quiet_NaN(),
      -std::numeric_limits<double>::infinity(),
      1.7976931348623157e308, -1.7976931348623157E308,
      4.9e-324, -4.9e-324 };
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf0, 0x3f,
          0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x40,
          0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xc0,
          0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x59, 0x40,
          0xe8, 0x38, 0x65, 0x99, 0xf9, 0x58, 0x98,
          0x46,
          0xa1, 0x88, 0x41, 0x98, 0xc5, 0x90, 0x4f,
          0xbc,
          0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf0, 0x7f,
          0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf8, 0x7f,
          0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf0, 0xff,
          0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xef,
          0x7f,
          0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xef,
          0xff,
          0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
          0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x80
      })));

  // create the row type
  std::unique_ptr<Type> rowType =
      createStructType( { createPrimitiveType(DOUBLE) }, { "myDouble" });
  rowType->assignIds(0);

  std::unique_ptr<ColumnReader> reader =
      buildReader(*rowType, streams);

  DoubleVectorBatch *doubleBatch = new DoubleVectorBatch(1024,
                                                         *getDefaultPool());
  StructVectorBatch batch(1024, *getDefaultPool());
  batch.fields.push_back(doubleBatch);
  reader->next(batch, 32, 0);
  ASSERT_EQ(32, batch.numElements);
  ASSERT_EQ(false, batch.hasNulls);
  ASSERT_EQ(32, doubleBatch->numElements);
  ASSERT_EQ(true, doubleBatch->hasNulls);

  for (size_t i = 0; i < batch.numElements; ++i) {
    if (i > 12) {
      EXPECT_EQ(0, doubleBatch->notNull[i]) << "Wrong value at " << i;
    } else if (i == 7) {
      EXPECT_EQ(1, doubleBatch->notNull[i]) << "Wrong value at " << i;
      EXPECT_EQ(true, isnan(doubleBatch->data[i]));
    } else {
      EXPECT_EQ(1, doubleBatch->notNull[i]) << "Wrong value at " << i;
      EXPECT_DOUBLE_EQ(test_vals[i], doubleBatch->data[i])
          << "Wrong value at " << i;
    }
  }
}

TEST(TestColumnReader, testDoubleSkipWithNulls) {
  MockStripeStreams streams;

  // set getSelectedColumns()
  std::vector<bool> selectedColumns = { true, true };
  EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(nullptr));

  // 1 non-null, 5 nulls, 2 non-nulls
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0xff, 0x83 })));

  // 1, 2, -2
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(testing::Return
      (new SeekableArrayInputStream
          ( { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f,
              0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40,
              0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xc0
          })));

  // create the row type
  std::unique_ptr<Type> rowType =
      createStructType( { createPrimitiveType(DOUBLE) }, { "myDouble" });
  rowType->assignIds(0);

  std::unique_ptr<ColumnReader> reader =
      buildReader(*rowType, streams);

  DoubleVectorBatch *doubleBatch = new DoubleVectorBatch(1024,
                                                         *getDefaultPool());
  StructVectorBatch batch(1024, *getDefaultPool());
  batch.fields.push_back(doubleBatch);

  double test_vals[] = { 1.0, 2.0, -2.0 };
  int vals_ix = 0;

  reader->next(batch, 2, 0);
  ASSERT_EQ(2, batch.numElements);
  ASSERT_EQ(false, batch.hasNulls);
  ASSERT_EQ(2, doubleBatch->numElements);
  ASSERT_EQ(true, doubleBatch->hasNulls);

  for (size_t i = 0; i < batch.numElements; ++i) {
    if (i > 0) {
      EXPECT_EQ(0, doubleBatch->notNull[i]);
    } else {
      EXPECT_EQ(1, doubleBatch->notNull[i]);
      EXPECT_DOUBLE_EQ(test_vals[vals_ix], doubleBatch->data[i]);
      vals_ix++;
    }
  }

  reader->skip(3);

  reader->next(batch, 3, 0);
  ASSERT_EQ(3, batch.numElements);
  ASSERT_EQ(false, batch.hasNulls);
  ASSERT_EQ(3, doubleBatch->numElements);
  ASSERT_EQ(true, doubleBatch->hasNulls);
  for (size_t i = 0; i < batch.numElements; ++i) {
    if (i < 1) {
      EXPECT_EQ(0, doubleBatch->notNull[i]);
    } else {
      EXPECT_EQ(1, doubleBatch->notNull[i]);
      EXPECT_DOUBLE_EQ(test_vals[vals_ix], doubleBatch->data[i]);
      vals_ix++;
    }
  }
}

TEST(TestColumnReader, testTimestampSkipWithNulls) {
  MockStripeStreams streams;

  // set getSelectedColumns()
  std::vector<bool> selectedColumns = { true, true };
  EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(nullptr));

  // 2 non-nulls, 2 nulls, 2 non-nulls, 2 nulls
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0xff, 0xcc })));

  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0xfc, 0xbb, 0xb5, 0xbe, 0x31, 0xa1, 0xee, 0xe2, 0x10, 0xf8,
          0x92, 0xee, 0xf, 0x92, 0xa0, 0xd4, 0x30 })));

  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_SECONDARY, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
      ( { 0x1, 0x8, 0x5e })));

  // create the row type
  std::unique_ptr<Type> rowType =
      createStructType( { createPrimitiveType(TIMESTAMP) }, { "myTimestamp" });
  rowType->assignIds(0);

  std::unique_ptr<ColumnReader> reader =
      buildReader(*rowType, streams);

  LongVectorBatch *longBatch = new LongVectorBatch(1024, *getDefaultPool());
  StructVectorBatch batch(1024, *getDefaultPool());
  batch.fields.push_back(longBatch);

  // Test values are nanoseconds since 1970-01-01 00:00:00.0
  int64_t test_vals[] = {
      1368178850110000000,     //  2013-05-10 10:40:50.11
      1402483311120000000,     //  2014-06-11 11:41:51.12
      1436701372130000000,      //  2015-07-12 12:42:52.13
      1471092233140000000       //  2016-08-13 13:43:53.14
  };
  int vals_ix = 0;

  reader->next(batch, 3, 0);
  ASSERT_EQ(3, batch.numElements);
  ASSERT_EQ(false, batch.hasNulls);
  ASSERT_EQ(3, longBatch->numElements);
  ASSERT_EQ(true, longBatch->hasNulls);

  for (size_t i = 0; i < batch.numElements; ++i) {
    if (i > 1) {
      EXPECT_EQ(0, longBatch->notNull[i]);
    } else {
      EXPECT_EQ(1, longBatch->notNull[i]);
      EXPECT_DOUBLE_EQ(test_vals[vals_ix], longBatch->data[i]);
      vals_ix++;
    }
  }

  reader->skip(1);

  reader->next(batch, 4, 0);
  ASSERT_EQ(4, batch.numElements);
  ASSERT_EQ(false, batch.hasNulls);
  ASSERT_EQ(4, longBatch->numElements);
  ASSERT_EQ(true, longBatch->hasNulls);
  for (size_t i = 0; i < batch.numElements; ++i) {
    if (i > 1) {
      EXPECT_EQ(0, longBatch->notNull[i]);
    } else {
      EXPECT_EQ(1, longBatch->notNull[i]);
      EXPECT_DOUBLE_EQ(test_vals[vals_ix], longBatch->data[i]);
      vals_ix++;
    }
  }
}

TEST(DecimalColumnReader, testDecimal64) {
  MockStripeStreams streams;

  // set getSelectedColumns()
  std::vector<bool> selectedColumns = { true, true};
  EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(testing::_, proto::Stream_Kind_PRESENT,
                                      false))
      .WillRepeatedly(testing::Return(nullptr));

  // [0xff] * (64/8) + [0x00] * (56/8) + [0x01]
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                       ( { 0x05, 0xff, 0x04, 0x00, 0xff, 0x01 })));

  char numBuffer[65];
  for(int i=0; i < 65; ++i) {
    if (i < 32) {
      numBuffer[i] = static_cast<char>(0x3f - 2*i);
    } else {
      numBuffer[i] = static_cast<char>(2*(i - 32));
    }
  }
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
    .WillRepeatedly(testing::Return(new SeekableArrayInputStream(numBuffer,
                                                                 65, 3)));

  // [0x02] * 65
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_SECONDARY, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                       ( { 0x3e, 0x00, 0x04 })));

  // create the row type
  std::unique_ptr<Type> rowType =
    createStructType( { createDecimalType(12, 2) }, { "col0" });
  rowType->assignIds(0);

  std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

  StructVectorBatch batch(64, *getDefaultPool());
  Decimal64VectorBatch *decimals =
    new Decimal64VectorBatch(64, *getDefaultPool());
  batch.fields.push_back(decimals);
  reader->next(batch, 64, 0);
  EXPECT_EQ(false, batch.hasNulls);
  EXPECT_EQ(64, batch.numElements);
  EXPECT_EQ(false, decimals->hasNulls);
  EXPECT_EQ(64, decimals->numElements);
  EXPECT_EQ(2, decimals->scale);
  int64_t *values = decimals->values.data();
  for(int64_t i = 0; i < 64; ++i) {
    EXPECT_EQ(i - 32, values[i]);
  }
  reader->next(batch, 64, 0);
  EXPECT_EQ(false, batch.hasNulls);
  EXPECT_EQ(64, batch.numElements);
  EXPECT_EQ(true, decimals->hasNulls);
  EXPECT_EQ(64, decimals->numElements);
  for(size_t i=0; i < 63; ++i) {
    EXPECT_EQ(0, decimals->notNull[i]);
  }
  EXPECT_EQ(1, decimals->notNull[63]);
  EXPECT_EQ(32, decimals->values.data()[63]);
}

TEST(DecimalColumnReader, testDecimal64Skip) {
  MockStripeStreams streams;

  // set getSelectedColumns()
  std::vector<bool> selectedColumns = { true, true};
  EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(testing::_, proto::Stream_Kind_PRESENT,
                                      false))
      .WillRepeatedly(testing::Return(nullptr));

  // [0xff]
  unsigned char presentBuffer[] = {0xfe, 0xff, 0x80};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      (presentBuffer, 3)));

  // [493827160549382716, 4938271605493827, 49382716054938, 493827160549,
  //  4938271605, 49382716, 493827, 4938, 49]
  unsigned char numBuffer[] =
    { 0xf8, 0xe8, 0xe2, 0xcf, 0xf4, 0xcb, 0xb6, 0xda, 0x0d,
      0x86, 0xc1, 0xcc, 0xcd, 0x9e, 0xd5, 0xc5, 0x11,
      0xb4, 0xf6, 0xfc, 0xf3, 0xb9, 0xba, 0x16,
      0xca, 0xe7, 0xa3, 0xa6, 0xdf, 0x1c,
      0xea, 0xad, 0xc0, 0xe5, 0x24,
      0xf8, 0x94, 0x8c, 0x2f,
      0x86, 0xa4, 0x3c,
      0x94, 0x4d,
      0x62 };
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
    .WillRepeatedly(testing::Return(new SeekableArrayInputStream(numBuffer,
                                                                 45)));

  // [0x0a] * 9
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_SECONDARY, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                       ( { 0x06, 0x00, 0x14 })));

  // create the row type
  std::unique_ptr<Type> rowType =
    createStructType( { createDecimalType(12, 10) }, { "col0" });
  rowType->assignIds(0);

  std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

  StructVectorBatch batch(64, *getDefaultPool());
  Decimal64VectorBatch *decimals =
    new Decimal64VectorBatch(64, *getDefaultPool());
  batch.fields.push_back(decimals);
  reader->next(batch, 6, 0);
  EXPECT_EQ(false, batch.hasNulls);
  EXPECT_EQ(6, batch.numElements);
  EXPECT_EQ(false, decimals->hasNulls);
  EXPECT_EQ(6, decimals->numElements);
  EXPECT_EQ(10, decimals->scale);
  int64_t *values = decimals->values.data();
  EXPECT_EQ(493827160549382716, values[0]);
  EXPECT_EQ(4938271605493827, values[1]);
  EXPECT_EQ(49382716054938, values[2]);
  EXPECT_EQ(493827160549, values[3]);
  EXPECT_EQ(4938271605, values[4]);
  EXPECT_EQ(49382716, values[5]);
  reader->skip(2);
  reader->next(batch, 1, 0);
  EXPECT_EQ(false, batch.hasNulls);
  EXPECT_EQ(1, batch.numElements);
  EXPECT_EQ(false, decimals->hasNulls);
  EXPECT_EQ(1, decimals->numElements);
  EXPECT_EQ(49, values[0]);
}

TEST(DecimalColumnReader, testDecimal128) {
  MockStripeStreams streams;

  // set getSelectedColumns()
  std::vector<bool> selectedColumns = { true, true};
  EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(testing::_, proto::Stream_Kind_PRESENT,
                                      false))
      .WillRepeatedly(testing::Return(nullptr));

  // [0xff] * (64/8) + [0x00] * (56/8) + [0x01]
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                       ( { 0x05, 0xff, 0x04, 0x00, 0xff, 0x01 })));

  char numBuffer[65];
  for(int i=0; i < 65; ++i) {
    if (i < 32) {
      numBuffer[i] = static_cast<char>(0x3f - 2*i);
    } else {
      numBuffer[i] = static_cast<char>(2*(i - 32));
    }
  }
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
    .WillRepeatedly(testing::Return(new SeekableArrayInputStream(numBuffer,
                                                                 65, 3)));

  // [0x02] * 65
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_SECONDARY, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                       ( { 0x3e, 0x00, 0x04 })));

  // create the row type
  std::unique_ptr<Type> rowType =
    createStructType( { createDecimalType(32, 2) }, { "col0" });
  rowType->assignIds(0);

  std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

  StructVectorBatch batch(64, *getDefaultPool());
  Decimal128VectorBatch *decimals =
    new Decimal128VectorBatch(64, *getDefaultPool());
  batch.fields.push_back(decimals);
  reader->next(batch, 64, 0);
  EXPECT_EQ(false, batch.hasNulls);
  EXPECT_EQ(64, batch.numElements);
  EXPECT_EQ(false, decimals->hasNulls);
  EXPECT_EQ(64, decimals->numElements);
  EXPECT_EQ(2, decimals->scale);
  Int128 *values = decimals->values.data();
  for(int64_t i = 0; i < 64; ++i) {
    EXPECT_EQ(i - 32, values[i].toLong());
  }
  reader->next(batch, 64, 0);
  EXPECT_EQ(false, batch.hasNulls);
  EXPECT_EQ(64, batch.numElements);
  EXPECT_EQ(true, decimals->hasNulls);
  EXPECT_EQ(64, decimals->numElements);
  for(size_t i=0; i < 63; ++i) {
    EXPECT_EQ(0, decimals->notNull[i]);
  }
  EXPECT_EQ(1, decimals->notNull[63]);
  EXPECT_EQ(32, decimals->values.data()[63].toLong());
}

TEST(DecimalColumnReader, testDecimal128Skip) {
  MockStripeStreams streams;

  // set getSelectedColumns()
  std::vector<bool> selectedColumns = { true, true};
  EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(testing::_, proto::Stream_Kind_PRESENT,
                                      false))
      .WillRepeatedly(testing::Return(nullptr));

  // [0xff, 0xf8]
  unsigned char presentBuffer[] = {0xfe, 0xff, 0xf8};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      (presentBuffer, 3)));

  // [493827160549382716, 4938271605493827, 49382716054938, 493827160549,
  //  4938271605, 49382716, 493827, 4938, 49,
  //  17320508075688772935274463415058723669,
  //  -17320508075688772935274463415058723669,
  //  99999999999999999999999999999999999999,
  //  -99999999999999999999999999999999999999]
  unsigned char numBuffer[] =
    { 0xf8, 0xe8, 0xe2, 0xcf, 0xf4, 0xcb, 0xb6, 0xda, 0x0d,
      0x86, 0xc1, 0xcc, 0xcd, 0x9e, 0xd5, 0xc5, 0x11,
      0xb4, 0xf6, 0xfc, 0xf3, 0xb9, 0xba, 0x16,
      0xca, 0xe7, 0xa3, 0xa6, 0xdf, 0x1c,
      0xea, 0xad, 0xc0, 0xe5, 0x24,
      0xf8, 0x94, 0x8c, 0x2f,
      0x86, 0xa4, 0x3c,
      0x94, 0x4d,
      0x62,
      0xaa, 0xcd, 0xb3, 0xf2, 0x9e, 0xf0, 0x99, 0xd6, 0xbe, 0xf8, 0xb6,
      0x9e, 0xe4, 0xb7, 0xfd, 0xce, 0x8f, 0x34,
      0xa9, 0xcd, 0xb3, 0xf2, 0x9e, 0xf0, 0x99, 0xd6, 0xbe, 0xf8, 0xb6,
      0x9e, 0xe4, 0xb7, 0xfd, 0xce, 0x8f, 0x34,
      0xfe, 0xff, 0xff, 0xff, 0xff, 0x8f, 0x91, 0x8a, 0x93, 0xe8, 0xa3,
      0xec, 0xd0, 0x96, 0xd4, 0xcc, 0xf6, 0xac, 0x02,
      0xfd, 0xff, 0xff, 0xff, 0xff, 0x8f, 0x91, 0x8a, 0x93, 0xe8, 0xa3,
      0xec, 0xd0, 0x96, 0xd4, 0xcc, 0xf6, 0xac, 0x02,
    };
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
    .WillRepeatedly(testing::Return(new SeekableArrayInputStream(numBuffer,
                                                                 119)));
  // [0x02] * 13
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_SECONDARY, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                       ( { 0x0a, 0x00, 0x4a })));

  // create the row type
  std::unique_ptr<Type> rowType =
    createStructType( { createDecimalType(38, 37) }, { "col0" });
  rowType->assignIds(0);

  std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

  StructVectorBatch batch(64, *getDefaultPool());
  Decimal128VectorBatch *decimals =
    new Decimal128VectorBatch(64, *getDefaultPool());
  batch.fields.push_back(decimals);
  reader->next(batch, 6, 0);
  EXPECT_EQ(false, batch.hasNulls);
  EXPECT_EQ(6, batch.numElements);
  EXPECT_EQ(false, decimals->hasNulls);
  EXPECT_EQ(6, decimals->numElements);
  EXPECT_EQ(37, decimals->scale);
  Int128 *values = decimals->values.data();
  EXPECT_EQ(493827160549382716, values[0].toLong());
  EXPECT_EQ(4938271605493827, values[1].toLong());
  EXPECT_EQ(49382716054938, values[2].toLong());
  EXPECT_EQ(493827160549, values[3].toLong());
  EXPECT_EQ(4938271605, values[4].toLong());
  EXPECT_EQ(49382716, values[5].toLong());
  reader->skip(2);
  reader->next(batch, 5, 0);
  EXPECT_EQ(false, batch.hasNulls);
  EXPECT_EQ(5, batch.numElements);
  EXPECT_EQ(false, decimals->hasNulls);
  EXPECT_EQ(5, decimals->numElements);
  EXPECT_EQ(49, values[0].toLong());
  EXPECT_EQ("1.7320508075688772935274463415058723669",
            values[1].toDecimalString(decimals->scale));
  EXPECT_EQ("-1.7320508075688772935274463415058723669",
            values[2].toDecimalString(decimals->scale));
  EXPECT_EQ("9.9999999999999999999999999999999999999",
            values[3].toDecimalString(decimals->scale));
  EXPECT_EQ("-9.9999999999999999999999999999999999999",
            values[4].toDecimalString(decimals->scale));
}

TEST(DecimalColumnReader, testDecimalHive11) {
  MockStripeStreams streams;

  // set getReaderOptions()
  ReaderOptions readerOptions;
  EXPECT_CALL(streams, getReaderOptions())
    .WillRepeatedly(testing::ReturnRef(readerOptions));

  // set getSelectedColumns()
  std::vector<bool> selectedColumns = { true, true};
  EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(testing::_, proto::Stream_Kind_PRESENT,
                                      false))
      .WillRepeatedly(testing::Return(nullptr));

  // [0xff] * (64/8) + [0x00] * (56/8) + [0x01]
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                       ( { 0x05, 0xff, 0x04, 0x00, 0xff, 0x01 })));

  char numBuffer[65];
  for(int i=0; i < 65; ++i) {
    if (i < 32) {
      numBuffer[i] = static_cast<char>(0x3f - 2*i);
    } else {
      numBuffer[i] = static_cast<char>(2*(i - 32));
    }
  }
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
    .WillRepeatedly(testing::Return(new SeekableArrayInputStream(numBuffer,
                                                                 65, 3)));

  unsigned char scaleBuffer[] = {0x3e, 0x00, 0x0c};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_SECONDARY, true))
    .WillRepeatedly(testing::Return(new SeekableArrayInputStream(scaleBuffer,
                                                                 3)));

  // create the row type
  std::unique_ptr<Type> rowType =
    createStructType( { createDecimalType(0, 0) }, { "col0" });
  rowType->assignIds(0);

  std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

  StructVectorBatch batch(64, *getDefaultPool());
  Decimal128VectorBatch *decimals =
    new Decimal128VectorBatch(64, *getDefaultPool());
  batch.fields.push_back(decimals);
  reader->next(batch, 64, 0);
  EXPECT_EQ(false, batch.hasNulls);
  EXPECT_EQ(64, batch.numElements);
  EXPECT_EQ(false, decimals->hasNulls);
  EXPECT_EQ(64, decimals->numElements);
  EXPECT_EQ(6, decimals->scale);
  Int128 *values = decimals->values.data();
  for(int64_t i = 0; i < 64; ++i) {
    EXPECT_EQ(i - 32, values[i].toLong());
  }
  reader->next(batch, 64, 0);
  EXPECT_EQ(false, batch.hasNulls);
  EXPECT_EQ(64, batch.numElements);
  EXPECT_EQ(true, decimals->hasNulls);
  EXPECT_EQ(64, decimals->numElements);
  for(size_t i=0; i < 63; ++i) {
    EXPECT_EQ(0, decimals->notNull[i]);
  }
  EXPECT_EQ(1, decimals->notNull[63]);
  EXPECT_EQ(32, decimals->values.data()[63].toLong());
}

TEST(DecimalColumnReader, testDecimalHive11Skip) {
  MockStripeStreams streams;

  // set getReaderOptions()
  ReaderOptions readerOptions;
  readerOptions.throwOnHive11DecimalOverflow(false)
    .forcedScaleOnHive11Decimal(3);
  EXPECT_CALL(streams, getReaderOptions())
      .WillRepeatedly(testing::ReturnRef(readerOptions));

  // set getSelectedColumns()
  std::vector<bool> selectedColumns = { true, true};
  EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(testing::_, proto::Stream_Kind_PRESENT,
                                      false))
      .WillRepeatedly(testing::Return(nullptr));

  // [0xff, 0xf8]
  unsigned char presentBuffer[] = {0xfe, 0xff, 0xf8};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      (presentBuffer, 3)));

  // [493827160549382716, 4938271605493827, 49382716054938, 493827160549,
  //  4938271605, 49382716, 493827, 4938, 49,
  //  17320508075688772935274463415058723669,
  //  -17320508075688772935274463415058723669,
  //  99999999999999999999999999999999999999,
  //  -99999999999999999999999999999999999999]
  unsigned char numBuffer[] =
    { 0xf8, 0xe8, 0xe2, 0xcf, 0xf4, 0xcb, 0xb6, 0xda, 0x0d,
      0x86, 0xc1, 0xcc, 0xcd, 0x9e, 0xd5, 0xc5, 0x11,
      0xb4, 0xf6, 0xfc, 0xf3, 0xb9, 0xba, 0x16,
      0xca, 0xe7, 0xa3, 0xa6, 0xdf, 0x1c,
      0xea, 0xad, 0xc0, 0xe5, 0x24,
      0xf8, 0x94, 0x8c, 0x2f,
      0x86, 0xa4, 0x3c,
      0x94, 0x4d,
      0x62,
      0xaa, 0xcd, 0xb3, 0xf2, 0x9e, 0xf0, 0x99, 0xd6, 0xbe, 0xf8, 0xb6,
      0x9e, 0xe4, 0xb7, 0xfd, 0xce, 0x8f, 0x34,
      0xa9, 0xcd, 0xb3, 0xf2, 0x9e, 0xf0, 0x99, 0xd6, 0xbe, 0xf8, 0xb6,
      0x9e, 0xe4, 0xb7, 0xfd, 0xce, 0x8f, 0x34,
      0xfe, 0xff, 0xff, 0xff, 0xff, 0x8f, 0x91, 0x8a, 0x93, 0xe8, 0xa3,
      0xec, 0xd0, 0x96, 0xd4, 0xcc, 0xf6, 0xac, 0x02,
      0xfd, 0xff, 0xff, 0xff, 0xff, 0x8f, 0x91, 0x8a, 0x93, 0xe8, 0xa3,
      0xec, 0xd0, 0x96, 0xd4, 0xcc, 0xf6, 0xac, 0x02,
    };
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
    .WillRepeatedly(testing::Return(new SeekableArrayInputStream(numBuffer,
                                                                 119)));
  unsigned char scaleBuffer[] = { 0x0a, 0x00, 0x06};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_SECONDARY, true))
    .WillRepeatedly(testing::Return(new SeekableArrayInputStream(scaleBuffer,
                                                                 3)));

  // create the row type
  std::unique_ptr<Type> rowType =
    createStructType( { createDecimalType(0, 0) }, { "col0" });
  rowType->assignIds(0);

  std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

  StructVectorBatch batch(64, *getDefaultPool());
  Decimal128VectorBatch *decimals =
    new Decimal128VectorBatch(64, *getDefaultPool());
  batch.fields.push_back(decimals);
  reader->next(batch, 6, 0);
  EXPECT_EQ(false, batch.hasNulls);
  EXPECT_EQ(6, batch.numElements);
  EXPECT_EQ(false, decimals->hasNulls);
  EXPECT_EQ(6, decimals->numElements);
  EXPECT_EQ(3, decimals->scale);
  Int128 *values = decimals->values.data();
  EXPECT_EQ(493827160549382716, values[0].toLong());
  EXPECT_EQ(4938271605493827, values[1].toLong());
  EXPECT_EQ(49382716054938, values[2].toLong());
  EXPECT_EQ(493827160549, values[3].toLong());
  EXPECT_EQ(4938271605, values[4].toLong());
  EXPECT_EQ(49382716, values[5].toLong());
  reader->skip(2);
  reader->next(batch, 5, 0);
  EXPECT_EQ(false, batch.hasNulls);
  EXPECT_EQ(5, batch.numElements);
  EXPECT_EQ(false, decimals->hasNulls);
  EXPECT_EQ(5, decimals->numElements);
  EXPECT_EQ(49, values[0].toLong());
  EXPECT_EQ("17320508075688772935274463415058723.669",
            values[1].toDecimalString(decimals->scale));
  EXPECT_EQ("-17320508075688772935274463415058723.669",
            values[2].toDecimalString(decimals->scale));
  EXPECT_EQ("99999999999999999999999999999999999.999",
            values[3].toDecimalString(decimals->scale));
  EXPECT_EQ("-99999999999999999999999999999999999.999",
            values[4].toDecimalString(decimals->scale));
}

TEST(DecimalColumnReader, testDecimalHive11ScaleUp) {
  MockStripeStreams streams;

  // set getReaderOptions()
  ReaderOptions readerOptions;
  readerOptions.forcedScaleOnHive11Decimal(20);
  EXPECT_CALL(streams, getReaderOptions())
      .WillRepeatedly(testing::ReturnRef(readerOptions));

  // set getSelectedColumns()
  std::vector<bool> selectedColumns = { true, true};
  EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(testing::_, proto::Stream_Kind_PRESENT,
                                      false))
      .WillRepeatedly(testing::Return(nullptr));

  // [0xff, 0xff, 0xf8]
  unsigned char presentBuffer[] = {0xfd, 0xff, 0xff, 0xf8};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      (presentBuffer, 4)));

  // [1] * 21
  unsigned char numBuffer[] = {0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02,
                               0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02,
                               0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
    .WillRepeatedly(testing::Return(new SeekableArrayInputStream(numBuffer,
                                                                 21)));
  unsigned char scaleBuffer[] = { 0x12, 0xff, 0x28};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_SECONDARY, true))
    .WillRepeatedly(testing::Return(new SeekableArrayInputStream(scaleBuffer,
                                                                 3)));

  // create the row type
  std::unique_ptr<Type> rowType =
    createStructType( { createDecimalType(0, 0) }, { "col0" });
  rowType->assignIds(0);

  std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

  StructVectorBatch batch(64, *getDefaultPool());
  Decimal128VectorBatch *decimals =
    new Decimal128VectorBatch(64, *getDefaultPool());
  batch.fields.push_back(decimals);
  reader->next(batch, 21, 0);
  EXPECT_EQ(false, batch.hasNulls);
  EXPECT_EQ(21, batch.numElements);
  EXPECT_EQ(false, decimals->hasNulls);
  EXPECT_EQ(21, decimals->numElements);
  EXPECT_EQ(20, decimals->scale);
  Int128 *values = decimals->values.data();
  Int128 expected = 1;
  for(int i = 0; i < 21; ++i) {
    EXPECT_EQ(expected.toString(), values[i].toString());
    expected *= 10;
  }
}

TEST(DecimalColumnReader, testDecimalHive11ScaleDown) {
  MockStripeStreams streams;

  // set getReaderOptions()
  ReaderOptions readerOptions;
  readerOptions.forcedScaleOnHive11Decimal(0);
  EXPECT_CALL(streams, getReaderOptions())
      .WillRepeatedly(testing::ReturnRef(readerOptions));

  // set getSelectedColumns()
  std::vector<bool> selectedColumns = { true, true};
  EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(testing::_, proto::Stream_Kind_PRESENT,
                                      false))
      .WillRepeatedly(testing::Return(nullptr));

  // [0xff, 0xff, 0xf8]
  unsigned char presentBuffer[] = {0xfd, 0xff, 0xff, 0xf8};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      (presentBuffer, 4)));

  // [100000000000000000000] * 21
  unsigned char numBuffer[] = {
    0x80, 0x80, 0x80, 0xb1, 0xac, 0x8b, 0xaf, 0xc7, 0xd7, 0x15,
    0x80, 0x80, 0x80, 0xb1, 0xac, 0x8b, 0xaf, 0xc7, 0xd7, 0x15,
    0x80, 0x80, 0x80, 0xb1, 0xac, 0x8b, 0xaf, 0xc7, 0xd7, 0x15,
    0x80, 0x80, 0x80, 0xb1, 0xac, 0x8b, 0xaf, 0xc7, 0xd7, 0x15,
    0x80, 0x80, 0x80, 0xb1, 0xac, 0x8b, 0xaf, 0xc7, 0xd7, 0x15,
    0x80, 0x80, 0x80, 0xb1, 0xac, 0x8b, 0xaf, 0xc7, 0xd7, 0x15,
    0x80, 0x80, 0x80, 0xb1, 0xac, 0x8b, 0xaf, 0xc7, 0xd7, 0x15,
    0x80, 0x80, 0x80, 0xb1, 0xac, 0x8b, 0xaf, 0xc7, 0xd7, 0x15,
    0x80, 0x80, 0x80, 0xb1, 0xac, 0x8b, 0xaf, 0xc7, 0xd7, 0x15,
    0x80, 0x80, 0x80, 0xb1, 0xac, 0x8b, 0xaf, 0xc7, 0xd7, 0x15,
    0x80, 0x80, 0x80, 0xb1, 0xac, 0x8b, 0xaf, 0xc7, 0xd7, 0x15,
    0x80, 0x80, 0x80, 0xb1, 0xac, 0x8b, 0xaf, 0xc7, 0xd7, 0x15,
    0x80, 0x80, 0x80, 0xb1, 0xac, 0x8b, 0xaf, 0xc7, 0xd7, 0x15,
    0x80, 0x80, 0x80, 0xb1, 0xac, 0x8b, 0xaf, 0xc7, 0xd7, 0x15,
    0x80, 0x80, 0x80, 0xb1, 0xac, 0x8b, 0xaf, 0xc7, 0xd7, 0x15,
    0x80, 0x80, 0x80, 0xb1, 0xac, 0x8b, 0xaf, 0xc7, 0xd7, 0x15,
    0x80, 0x80, 0x80, 0xb1, 0xac, 0x8b, 0xaf, 0xc7, 0xd7, 0x15,
    0x80, 0x80, 0x80, 0xb1, 0xac, 0x8b, 0xaf, 0xc7, 0xd7, 0x15,
    0x80, 0x80, 0x80, 0xb1, 0xac, 0x8b, 0xaf, 0xc7, 0xd7, 0x15,
    0x80, 0x80, 0x80, 0xb1, 0xac, 0x8b, 0xaf, 0xc7, 0xd7, 0x15,
    0x80, 0x80, 0x80, 0xb1, 0xac, 0x8b, 0xaf, 0xc7, 0xd7, 0x15};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
    .WillRepeatedly(testing::Return(new SeekableArrayInputStream(numBuffer,
                                                                 210)));
  unsigned char scaleBuffer[] = { 0x12, 0x01, 0x00};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_SECONDARY, true))
    .WillRepeatedly(testing::Return(new SeekableArrayInputStream(scaleBuffer,
                                                                 3)));

  // create the row type
  std::unique_ptr<Type> rowType =
    createStructType( { createDecimalType(0, 0) }, { "col0" });
  rowType->assignIds(0);

  std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

  StructVectorBatch batch(64, *getDefaultPool());
  Decimal128VectorBatch *decimals =
    new Decimal128VectorBatch(64, *getDefaultPool());
  batch.fields.push_back(decimals);
  reader->next(batch, 21, 0);
  EXPECT_EQ(false, batch.hasNulls);
  EXPECT_EQ(21, batch.numElements);
  EXPECT_EQ(false, decimals->hasNulls);
  EXPECT_EQ(21, decimals->numElements);
  EXPECT_EQ(0, decimals->scale);
  Int128 *values = decimals->values.data();
  Int128 expected = Int128(0x5, 0x6bc75e2d63100000);
  Int128 remainder;
  for(int i = 0; i < 21; ++i) {
    EXPECT_EQ(expected.toString(), values[i].toString());
    expected = expected.divide(10, remainder);
  }
}

TEST(DecimalColumnReader, testDecimalHive11OverflowException) {
  MockStripeStreams streams;

  // set getReaderOptions()
  ReaderOptions readerOptions;
  EXPECT_CALL(streams, getReaderOptions())
      .WillRepeatedly(testing::ReturnRef(readerOptions));

  // set getSelectedColumns()
  std::vector<bool> selectedColumns = { true, true};
  EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(testing::_, proto::Stream_Kind_PRESENT,
                                      false))
      .WillRepeatedly(testing::Return(nullptr));

  // [0x80]
  unsigned char presentBuffer[] = {0xff, 0x80};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      (presentBuffer, sizeof(presentBuffer))));

  // [10000000000000000000]
  unsigned char numBuffer[] = {0x80, 0x80, 0x80, 0x80, 0x80, 0x90, 0x91, 0x8a,
                               0x93, 0xe8, 0xa3, 0xec, 0xd0, 0x96, 0xd4, 0xcc,
                               0xf6, 0xac, 0x02};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
    .WillRepeatedly(testing::Return(new SeekableArrayInputStream(numBuffer,
                                                                 19)));
  unsigned char scaleBuffer[] = { 0xff, 0x0c};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_SECONDARY, true))
    .WillRepeatedly(testing::Return(new SeekableArrayInputStream(scaleBuffer,
                                                                 2)));

  // create the row type
  std::unique_ptr<Type> rowType =
    createStructType( { createDecimalType(0, 0) }, { "col0" });
  rowType->assignIds(0);

  std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

  StructVectorBatch batch(64, *getDefaultPool());
  Decimal128VectorBatch *decimals =
    new Decimal128VectorBatch(64, *getDefaultPool());
  batch.fields.push_back(decimals);
  EXPECT_THROW(reader->next(batch, 1, 0), ParseError);
}

TEST(DecimalColumnReader, testDecimalHive11OverflowExceptionNull) {
  MockStripeStreams streams;

  // set getReaderOptions()
  ReaderOptions readerOptions;
  EXPECT_CALL(streams, getReaderOptions())
      .WillRepeatedly(testing::ReturnRef(readerOptions));

  // set getSelectedColumns()
  std::vector<bool> selectedColumns = { true, true};
  EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(testing::_, proto::Stream_Kind_PRESENT,
                                      false))
      .WillRepeatedly(testing::Return(nullptr));

  // [0x40]
  unsigned char presentBuffer[] = {0xff, 0x40};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      (presentBuffer, sizeof(presentBuffer))));

  // [10000000000000000000]
  unsigned char numBuffer[] = {0x80, 0x80, 0x80, 0x80, 0x80, 0x90, 0x91, 0x8a,
                               0x93, 0xe8, 0xa3, 0xec, 0xd0, 0x96, 0xd4, 0xcc,
                               0xf6, 0xac, 0x02};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
    .WillRepeatedly(testing::Return(new SeekableArrayInputStream(numBuffer,
                                                                 19)));
  unsigned char scaleBuffer[] = { 0xff, 0x0c};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_SECONDARY, true))
    .WillRepeatedly(testing::Return(new SeekableArrayInputStream(scaleBuffer,
                                                                 2)));

  // create the row type
  std::unique_ptr<Type> rowType =
    createStructType( { createDecimalType(0, 0) }, { "col0" });
  rowType->assignIds(0);

  std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

  StructVectorBatch batch(64, *getDefaultPool());
  Decimal128VectorBatch *decimals =
    new Decimal128VectorBatch(64, *getDefaultPool());
  batch.fields.push_back(decimals);
  EXPECT_THROW(reader->next(batch, 2, 0), ParseError);
}

TEST(DecimalColumnReader, testDecimalHive11OverflowNull) {
  MockStripeStreams streams;

  // set getReaderOptions()
  ReaderOptions readerOptions;
  std::stringstream errStream;
  readerOptions.throwOnHive11DecimalOverflow(false)
    .setErrorStream(errStream);
  EXPECT_CALL(streams, getReaderOptions())
      .WillRepeatedly(testing::ReturnRef(readerOptions));

  // set getSelectedColumns()
  std::vector<bool> selectedColumns = { true, true};
  EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(testing::_, proto::Stream_Kind_PRESENT,
                                      false))
      .WillRepeatedly(testing::Return(nullptr));

  // [0x78]
  unsigned char presentBuffer[] = {0xff, 0x78};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      (presentBuffer, sizeof(presentBuffer))));

  // [1000000000000000000000000000000000000000, 1,
  //  -10000000000000000000000000000000000000, 1]
  unsigned char numBuffer[] = { 0x80, 0x80, 0x80, 0x80, 0x80, 0xc0, 0xb0, 0xf5,
                                0xf3, 0xae, 0xfd, 0xcb, 0x94, 0xd7, 0xe1, 0xf1,
                                0xd3, 0x8c, 0xeb, 0x01,
                                0x02,
                                0xff, 0xff, 0xff, 0xff, 0xff, 0x8f, 0x91, 0x8a,
                                0x93, 0xe8, 0xa3, 0xec, 0xd0, 0x96, 0xd4, 0xcc,
                                0xf6, 0xac, 0x02,
                                0x02};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
    .WillRepeatedly(testing::Return(new SeekableArrayInputStream(numBuffer,
                                                                 41)));
  unsigned char scaleBuffer[] = { 0x01, 0x00, 0x0c};
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_SECONDARY, true))
    .WillRepeatedly(testing::Return(new SeekableArrayInputStream(scaleBuffer,
                                                                 3)));

  // create the row type
  std::unique_ptr<Type> rowType =
    createStructType( { createDecimalType(0, 0) }, { "col0" });
  rowType->assignIds(0);

  std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

  StructVectorBatch batch(64, *getDefaultPool());
  Decimal128VectorBatch *decimals =
    new Decimal128VectorBatch(64, *getDefaultPool());
  batch.fields.push_back(decimals);

  reader->next(batch, 3, 0);
  EXPECT_EQ(false, batch.hasNulls);
  EXPECT_EQ(3, batch.numElements);
  EXPECT_EQ(true, decimals->hasNulls);
  EXPECT_EQ(3, decimals->numElements);
  EXPECT_EQ(6, decimals->scale);
  EXPECT_EQ(false, decimals->notNull[0]);
  EXPECT_EQ(false, decimals->notNull[1]);
  EXPECT_EQ(true, decimals->notNull[2]);
  EXPECT_EQ(1, decimals->values[2].toLong());

  reader->next(batch, 2, 0);
  EXPECT_EQ(false, batch.hasNulls);
  EXPECT_EQ(2, batch.numElements);
  EXPECT_EQ(true, decimals->hasNulls);
  EXPECT_EQ(2, decimals->numElements);
  EXPECT_EQ(6, decimals->scale);
  EXPECT_EQ(false, decimals->notNull[0]);
  EXPECT_EQ(true, decimals->notNull[1]);
  EXPECT_EQ(1, decimals->values[1].toLong());

  EXPECT_EQ("Warning: Hive 0.11 decimal with more than 38 digits"
            " replaced by NULL.\n"
            "Warning: Hive 0.11 decimal with more than 38 digits"
            " replaced by NULL.\n", errStream.str());
}

TEST(DecimalColumnReader, testDecimalHive11BigBatches) {
  MockStripeStreams streams;

  // set getReaderOptions()
  ReaderOptions readerOptions;
  EXPECT_CALL(streams, getReaderOptions())
      .WillRepeatedly(testing::ReturnRef(readerOptions));

  // set getSelectedColumns()
  std::vector<bool> selectedColumns = { true, true};
  EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(testing::_, proto::Stream_Kind_PRESENT,
                                      false))
      .WillRepeatedly(testing::Return(nullptr));

  // range(64) * 32
  unsigned char numBuffer[2048];
  for(size_t i=0; i < 2048; ++i) {
    numBuffer[i] = (i % 64) * 2;
  }
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
    .WillRepeatedly(testing::Return(new SeekableArrayInputStream(numBuffer,
                                                                 2048)));

  // [5] * 1024 + [4] * 1024
  unsigned char scaleBuffer[48];
  for(size_t i=0; i < 48; i += 3) {
    scaleBuffer[i] = 0x7d;
    scaleBuffer[i + 1] = 0x00;
    scaleBuffer[i + 2] = (i < 24) ? 0x0a : 0x08;
  }
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_SECONDARY, true))
    .WillRepeatedly(testing::Return(new SeekableArrayInputStream(scaleBuffer,
                                                                 48)));

  // create the row type
  std::unique_ptr<Type> rowType =
    createStructType( { createDecimalType(0, 0) }, { "col0" });
  rowType->assignIds(0);

  std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

  StructVectorBatch batch(2048, *getDefaultPool());
  Decimal128VectorBatch *decimals =
    new Decimal128VectorBatch(2048, *getDefaultPool());
  batch.fields.push_back(decimals);

  reader->next(batch, 2048, 0);
  EXPECT_EQ(false, batch.hasNulls);
  EXPECT_EQ(2048, batch.numElements);
  EXPECT_EQ(false, decimals->hasNulls);
  EXPECT_EQ(2048, decimals->numElements);
  EXPECT_EQ(6, decimals->scale);
  for(size_t i=0; i < decimals->numElements; ++i) {
    EXPECT_EQ((i % 64) * (i < 1024 ? 10 : 100),
              decimals->values[i].toLong()) << "Wrong value at " << i;
  }
}

TEST(TestColumnReader, testUnimplementedTypes) {
  MockStripeStreams streams;

  // set getSelectedColumns()
  std::vector<bool> selectedColumns = { true, true };
  EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT, true))
      .WillRepeatedly(testing::Return(nullptr));

  // create the row type
  std::unique_ptr<Type> rowType;

  rowType = createStructType( { createPrimitiveType(UNION) }, { "col0" });
  rowType->assignIds(0);
  EXPECT_THROW(buildReader(*rowType, streams), NotImplementedYet);

}

}  // namespace orc
