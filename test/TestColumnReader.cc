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

  class MockStripeStreams : public StripeStreams {
  public:
    ~MockStripeStreams();
    std::unique_ptr<SeekableInputStream> getStream(int columnId,
                                                   proto::Stream_Kind kind
                                                   ) const override;
    MOCK_CONST_METHOD0(getSelectedColumns, const bool*());
    MOCK_CONST_METHOD1(getEncoding, proto::ColumnEncoding (int));
    MOCK_CONST_METHOD2(getStreamProxy, SeekableInputStream*
                       (int, proto::Stream_Kind));
  };

  MockStripeStreams::~MockStripeStreams() {
    // PASS
  }

  std::unique_ptr<SeekableInputStream>
       MockStripeStreams::getStream(int columnId,
                                    proto::Stream_Kind kind) const {
    return std::auto_ptr<SeekableInputStream>(getStreamProxy(columnId,
                                                             kind));
  }

  TEST(TestColumnReader, testBooleanWithNulls) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    bool selectedColumns[] = {true, true};
    EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(nullptr));
    // alternate 4 non-null and 4 null via [0xf0 for x in range(512 / 8)]
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x3d, 0xf0})));

    // [0x0f for x in range(256 / 8)]
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x1d, 0x0f})));

    // create the row type
    std::unique_ptr<Type> rowType =
      createStructType({createPrimitiveType(BOOLEAN)}, {"col0"});
    rowType->assignIds(0);

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);
    LongVectorBatch *longBatch = new LongVectorBatch(1024);
    StructVectorBatch batch(1024);
    batch.fields.push_back(std::unique_ptr<ColumnVectorBatch>(longBatch));
    reader->next(batch, 512, 0);
    ASSERT_EQ(512, batch.numElements);
    ASSERT_EQ(false, batch.hasNulls);
    ASSERT_EQ(512, longBatch->numElements);
    ASSERT_EQ(true, longBatch->hasNulls);
    unsigned int next = 0;
    for(size_t i=0; i < batch.numElements; ++i) {
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
    bool selectedColumns[] = {true, true};
    EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(nullptr));
    // alternate 4 non-null and 4 null via [0xf0 for x in range(512 / 8)]
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x3d, 0xf0})));
    // [0x0f for x in range(128 / 8)]
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x1d, 0x0f})));

    // create the row type
    std::unique_ptr<Type> rowType =
      createStructType({createPrimitiveType(BOOLEAN)}, {"col0"});
    rowType->assignIds(0);

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);
    LongVectorBatch *longBatch = new LongVectorBatch(1024);
    StructVectorBatch batch(1024);
    batch.fields.push_back(std::unique_ptr<ColumnVectorBatch>(longBatch));
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
    bool selectedColumns[] = {true, true};
    EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(nullptr));
    // alternate 4 non-null and 4 null via [0xf0 for x in range(512 / 8)]
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x3d, 0xf0})));

    // range(256)
    char buffer[258];
    buffer[0] = static_cast<char>(0x80);
    for(unsigned int i=0; i < 128; ++i) {
      buffer[i+1] = static_cast<char>(i);
    }
    buffer[129] = static_cast<char>(0x80);
    for(unsigned int i=128; i < 256; ++i) {
      buffer[i+2] = static_cast<char>(i);
    }
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      (buffer, 258)));

    // create the row type
    std::unique_ptr<Type> rowType =
      createStructType({createPrimitiveType(BYTE)}, {"col0"});
    rowType->assignIds(0);

    std::unique_ptr<ColumnReader> reader =
      buildReader(*rowType, streams);
    LongVectorBatch *longBatch = new LongVectorBatch(1024);
    StructVectorBatch batch(1024);
    batch.fields.push_back(std::unique_ptr<ColumnVectorBatch>(longBatch));
    reader->next(batch, 512, 0);
    ASSERT_EQ(512, batch.numElements);
    ASSERT_EQ(false, batch.hasNulls);
    ASSERT_EQ(512, longBatch->numElements);
    ASSERT_EQ(true, longBatch->hasNulls);
    unsigned int next = 0;
    for(size_t i=0; i < batch.numElements; ++i) {
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
    bool selectedColumns[] = {true, true};
    EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(nullptr));
    // alternate 4 non-null and 4 null via [0xf0 for x in range(512 / 8)]
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x3d, 0xf0})));

    // range(256)
    char buffer[258];
    buffer[0] = static_cast<char>(0x80);
    for(unsigned int i=0; i < 128; ++i) {
      buffer[i+1] = static_cast<char>(i);
    }
    buffer[129] = static_cast<char>(0x80);
    for(unsigned int i=128; i < 256; ++i) {
      buffer[i+2] = static_cast<char>(i);
    }
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      (buffer, 258)));

    // create the row type
    std::unique_ptr<Type> rowType =
      createStructType({createPrimitiveType(BYTE)}, {"col0"});
    rowType->assignIds(0);

    std::unique_ptr<ColumnReader> reader =
      buildReader(*rowType, streams);
    LongVectorBatch *longBatch = new LongVectorBatch(1024);
    StructVectorBatch batch(1024);
    batch.fields.push_back(std::unique_ptr<ColumnVectorBatch>(longBatch));
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
    std::unique_ptr<bool[]> selectedColumns =
      std::unique_ptr<bool[]>(new bool[2]);
    memset(selectedColumns.get(), true, 2);
    EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns.get()));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(nullptr));
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x16, 0xf0})));
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x64, 0x01, 0x00})));

    // create the row type
    std::unique_ptr<Type> rowType =
      createStructType({createPrimitiveType(INT)}, {"myInt"});
    rowType->assignIds(0);

    std::unique_ptr<ColumnReader> reader =
      buildReader(*rowType, streams);
    LongVectorBatch *longBatch = new LongVectorBatch(1024);
    StructVectorBatch batch(1024);
    batch.fields.push_back(std::unique_ptr<ColumnVectorBatch>(longBatch));
    reader->next(batch, 200, 0);
    ASSERT_EQ(200, batch.numElements);
    ASSERT_EQ(false, batch.hasNulls);
    ASSERT_EQ(200, longBatch->numElements);
    ASSERT_EQ(true, longBatch->hasNulls);
    long next = 0;
    for(size_t i=0; i < batch.numElements; ++i) {
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
    std::unique_ptr<bool[]> selectedColumns =
      std::unique_ptr<bool[]>(new bool[2]);
    memset(selectedColumns.get(), true, 2);
    EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns.get()));

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
    EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(nullptr));
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x19, 0xf0})));
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x2f, 0x00, 0x00,
                                          0x2f, 0x00, 0x01})));
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DICTIONARY_DATA))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x4f, 0x52, 0x43, 0x4f, 0x77,
                                          0x65, 0x6e})));
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x02, 0x01, 0x03})));

    // create the row type
    std::unique_ptr<Type> rowType =
      createStructType({createPrimitiveType(STRING)}, {"myString"});
    rowType->assignIds(0);


    std::unique_ptr<ColumnReader> reader =
      buildReader(*rowType, streams);
    StringVectorBatch *stringBatch = new StringVectorBatch(1024);
    StructVectorBatch batch(1024);
    batch.fields.push_back(std::unique_ptr<ColumnVectorBatch>(stringBatch));
    reader->next(batch, 200, 0);
    ASSERT_EQ(200, batch.numElements);
    ASSERT_EQ(false, batch.hasNulls);
    ASSERT_EQ(200, stringBatch->numElements);
    ASSERT_EQ(true, stringBatch->hasNulls);
    for(size_t i=0; i < batch.numElements; ++i) {
      if (i & 4) {
        EXPECT_EQ(0, stringBatch->notNull[i]);
      } else {
        EXPECT_EQ(1, stringBatch->notNull[i]);
        const char* expected = i < 98 ? "ORC" : "Owen";
        ASSERT_EQ(strlen(expected), stringBatch->length[i])
            << "Wrong length at " << i;
        for(size_t letter = 0; letter < strlen(expected); ++letter) {
          EXPECT_EQ(expected[letter], stringBatch->data[i][letter])
            << "Wrong contents at " << i << ", " << letter;
        }
      }
    }
  }

  TEST(TestColumnReader, testVarcharDictionaryWithNulls) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    std::unique_ptr<bool[]> selectedColumns =
      std::unique_ptr<bool[]>(new bool[4]);
    memset(selectedColumns.get(), true, 3);
    selectedColumns.get()[3] = false;
    EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns.get()));

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
    EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(nullptr));

    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x16, 0xff})));
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x61, 0x00, 0x01,
                                          0x61, 0x00, 0x00})));
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DICTIONARY_DATA))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x4f, 0x52, 0x43, 0x4f, 0x77,
                                          0x65, 0x6e})));
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x02, 0x01, 0x03})));

    EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x16, 0x00})));
    EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_DATA))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({})));
    EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_DICTIONARY_DATA))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({})));
    EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_LENGTH))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({})));

    // create the row type
    std::unique_ptr<Type> rowType =
      createStructType({createPrimitiveType(VARCHAR),
                        createPrimitiveType(CHAR),
                        createPrimitiveType(STRING)},
        {"col0", "col1", "col2"});
    rowType->assignIds(0);

    std::unique_ptr<ColumnReader> reader =
      buildReader(*rowType, streams);
    StructVectorBatch batch(1024);
    StringVectorBatch *stringBatch = new StringVectorBatch(1024);
    StringVectorBatch *nullBatch = new StringVectorBatch(1024);
    batch.fields.push_back(std::unique_ptr<ColumnVectorBatch>(stringBatch));
    batch.fields.push_back(std::unique_ptr<ColumnVectorBatch>(nullBatch));
    reader->next(batch, 200, 0);
    ASSERT_EQ(200, batch.numElements);
    ASSERT_EQ(false, batch.hasNulls);
    ASSERT_EQ(200, stringBatch->numElements);
    ASSERT_EQ(false, stringBatch->hasNulls);
    ASSERT_EQ(200, nullBatch->numElements);
    ASSERT_EQ(true, nullBatch->hasNulls);
    for(size_t i=0; i < batch.numElements; ++i) {
      EXPECT_EQ(true, stringBatch->notNull[i]);
      EXPECT_EQ(false, nullBatch->notNull[i]);
      const char* expected = i < 100 ? "Owen" : "ORC";
      ASSERT_EQ(strlen(expected), stringBatch->length[i])
        << "Wrong length at " << i;
      for(size_t letter = 0; letter < strlen(expected); ++letter) {
        EXPECT_EQ(expected[letter], stringBatch->data[i][letter])
          << "Wrong contents at " << i << ", " << letter;
      }
    }
  }

  TEST(TestColumnReader, testSubstructsWithNulls) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    std::unique_ptr<bool[]> selectedColumns =
      std::unique_ptr<bool[]>(new bool[4]);
    memset(selectedColumns.get(), true, 4);
    EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns.get()));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(nullptr));

    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x16, 0x0f})));

    EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x0a, 0x55})));

    EXPECT_CALL(streams, getStreamProxy(3, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x04, 0xf0})));
    EXPECT_CALL(streams, getStreamProxy(3, proto::Stream_Kind_DATA))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x17, 0x01, 0x00})));

    // create the row type
    std::unique_ptr<Type> rowType =
      createStructType
        ({createStructType
            ({createStructType
                ({createPrimitiveType(LONG)}, {"col2"})},
              {"col1"})},
          {"col0"});
    rowType->assignIds(0);

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    StructVectorBatch batch(1024);
    StructVectorBatch *middle = new StructVectorBatch(1024);
    StructVectorBatch *inner = new StructVectorBatch(1024);
    LongVectorBatch *longs = new LongVectorBatch(1024);
    batch.fields.push_back(std::unique_ptr<ColumnVectorBatch>(middle));
    middle->fields.push_back(std::unique_ptr<ColumnVectorBatch>(inner));
    inner->fields.push_back(std::unique_ptr<ColumnVectorBatch>(longs));
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
    for(size_t i=0; i < batch.numElements; ++i) {
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
    std::unique_ptr<bool[]> selectedColumns =
      std::unique_ptr<bool[]>(new bool[3]);
    memset(selectedColumns.get(), true, 3);
    EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns.get()));

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
    EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(nullptr));
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x03, 0x00, 0xff, 0x3f, 0x08, 0xff,
                                          0xff, 0xfc, 0x03, 0x00})));
    EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x03, 0x00, 0xff, 0x3f, 0x08, 0xff,
                                          0xff, 0xfc, 0x03, 0x00})));
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x61, 0x01, 0x00})));
    EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_DATA))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x61, 0x01, 0x00})));

    // fill the dictionary with '00' to '99'
    char digits[200];
    for(int i=0; i < 10; ++i) {
      for(int j=0; j < 10; ++j) {
        digits[2 * (10 * i + j)] = '0' + static_cast<char>(i);
        digits[2 * (10 * i + j) + 1] = '0' + static_cast<char>(j);
      }
    }
    EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_DICTIONARY_DATA))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      (digits, 200)));
    EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_LENGTH))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x61, 0x00, 0x02})));

    // create the row type
    std::unique_ptr<Type> rowType =
      createStructType({createPrimitiveType(INT),
            createPrimitiveType(STRING)}, {"myInt", "myString"});
    rowType->assignIds(0);


    std::unique_ptr<ColumnReader> reader =
      buildReader(*rowType, streams);
    StructVectorBatch batch(100);
    LongVectorBatch *longBatch = new LongVectorBatch(100);
    StringVectorBatch *stringBatch = new StringVectorBatch(100);
    batch.fields.push_back(std::unique_ptr<ColumnVectorBatch>(longBatch));
    batch.fields.push_back(std::unique_ptr<ColumnVectorBatch>(stringBatch));
    reader->next(batch, 20, 0);
    ASSERT_EQ(20, batch.numElements);
    ASSERT_EQ(20, longBatch->numElements);
    ASSERT_EQ(20, stringBatch->numElements);
    ASSERT_EQ(false, batch.hasNulls);
    ASSERT_EQ(true, longBatch->hasNulls);
    ASSERT_EQ(true, stringBatch->hasNulls);
    for(size_t i=0; i < 20; ++i) {
      EXPECT_EQ(false, longBatch->notNull[i]) << "Wrong at " << i;
      EXPECT_EQ(false, stringBatch->notNull[i]) << "Wrong at " << i;
    }
    reader->skip(30);
    reader->next(batch, 100, 0);
    ASSERT_EQ(100, batch.numElements);
    ASSERT_EQ(false, batch.hasNulls);
    ASSERT_EQ(false, longBatch->hasNulls);
    ASSERT_EQ(false, stringBatch->hasNulls);
    for(size_t i=0; i < 10; ++i) {
      for(size_t j=0; j < 10; ++j) {
        size_t k = 10 * i + j;
        EXPECT_EQ(1, longBatch->notNull[k]) << "Wrong at " << k;
        ASSERT_EQ(2, stringBatch->length[k]) << "Wrong at " << k;
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
    bool selectedColumns[] = {true, true};
    EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(nullptr));

    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(nullptr));

    char blob[200];
    for(size_t i=0; i < 10; ++i) {
      for(size_t j=0; j < 10; ++j) {
        blob[2*(10*i+j)] = static_cast<char>(i);
        blob[2*(10*i+j) + 1] = static_cast<char>(j);
      }
    }
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      (blob, 200)));

    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x61, 0x00, 0x02})));

    // create the row type
    std::unique_ptr<Type> rowType =
      createStructType({createPrimitiveType(BINARY)}, {"col0"});
    rowType->assignIds(0);

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    StructVectorBatch batch(1024);
    StringVectorBatch *strings = new StringVectorBatch(1024);
    batch.fields.push_back(std::unique_ptr<ColumnVectorBatch>(strings));
    for(size_t i=0; i < 2; ++i) {
      reader->next(batch, 50, 0);
      ASSERT_EQ(50, batch.numElements);
      ASSERT_EQ(false, batch.hasNulls);
      ASSERT_EQ(50, strings->numElements);
      ASSERT_EQ(false, strings->hasNulls);
      for(size_t j=0; j < batch.numElements; ++j) {
        ASSERT_EQ(2, strings->length[j]);
        EXPECT_EQ((50 * i + j) / 10, strings->data[j][0]);
        EXPECT_EQ((50 * i + j) % 10, strings->data[j][1]);
      }
    }
  }

  TEST(TestColumnReader, testBinaryDirectWithNulls) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    bool selectedColumns[] = {true, true};
    EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(nullptr));

    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x1d, 0xf0})));

    char blob[256];
    for(size_t i=0; i < 8; ++i) {
      for(size_t j=0; j < 16; ++j) {
        blob[2*(16*i+j)] = 'A' + static_cast<char>(i);
        blob[2*(16*i+j) + 1] = 'A' + static_cast<char>(j);
      }
    }
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      (blob, 256)));

    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x7d, 0x00, 0x02})));

    // create the row type
    std::unique_ptr<Type> rowType =
      createStructType({createPrimitiveType(BINARY)}, {"col0"});
    rowType->assignIds(0);

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    StructVectorBatch batch(1024);
    StringVectorBatch *strings = new StringVectorBatch(1024);
    batch.fields.push_back(std::unique_ptr<ColumnVectorBatch>(strings));
    size_t next = 0;
    for(size_t i=0; i < 2; ++i) {
      reader->next(batch, 128, 0);
      ASSERT_EQ(128, batch.numElements);
      ASSERT_EQ(false, batch.hasNulls);
      ASSERT_EQ(128, strings->numElements);
      ASSERT_EQ(true, strings->hasNulls);
      for(size_t j=0; j < batch.numElements; ++j) {
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
    bool selectedColumns[] = {true, true};
    EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(nullptr));

    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(nullptr));

    char blob[100];
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      (blob, 100)));

    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x61, 0x00, 0x02})));

    // create the row type
    std::unique_ptr<Type> rowType =
      createStructType({createPrimitiveType(STRING)}, {"col0"});
    rowType->assignIds(0);

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    StructVectorBatch batch(1024);
    StringVectorBatch *strings = new StringVectorBatch(1024);
    batch.fields.push_back(std::unique_ptr<ColumnVectorBatch>(strings));
    EXPECT_THROW(reader->next(batch, 100, 0), ParseError);
  }

  TEST(TestColumnReader, testStringDirectShortBuffer) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    bool selectedColumns[] = {true, true};
    EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(nullptr));

    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(nullptr));

    char blob[200];
    for(size_t i=0; i < 10; ++i) {
      for(size_t j=0; j < 10; ++j) {
        blob[2*(10*i+j)] = static_cast<char>(i);
        blob[2*(10*i+j) + 1] = static_cast<char>(j);
      }
    }
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      (blob, 200, 3)));

    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x61, 0x00, 0x02})));

    // create the row type
    std::unique_ptr<Type> rowType =
      createStructType({createPrimitiveType(STRING)}, {"col0"});
    rowType->assignIds(0);

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    StructVectorBatch batch(25);
    StringVectorBatch *strings = new StringVectorBatch(25);
    batch.fields.push_back(std::unique_ptr<ColumnVectorBatch>(strings));
    for(size_t i=0; i < 4; ++i) {
      reader->next(batch, 25, 0);
      ASSERT_EQ(25, batch.numElements);
      ASSERT_EQ(false, batch.hasNulls);
      ASSERT_EQ(25, strings->numElements);
      ASSERT_EQ(false, strings->hasNulls);
      for(size_t j=0; j < batch.numElements; ++j) {
        ASSERT_EQ(2, strings->length[j]);
        EXPECT_EQ((25 * i + j) / 10, strings->data[j][0]);
        EXPECT_EQ((25 * i + j) % 10, strings->data[j][1]);
      }
    }
  }

  TEST(TestColumnReader, testStringDirectShortBufferWithNulls) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    bool selectedColumns[] = {true, true};
    EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(nullptr));

    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x3d, 0xf0})));

    char blob[512];
    for(size_t i=0; i < 16; ++i) {
      for(size_t j=0; j < 16; ++j) {
        blob[2*(16*i+j)] = 'A' + static_cast<char>(i);
        blob[2*(16*i+j) + 1] = 'A' + static_cast<char>(j);
      }
    }
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      (blob, 512, 30)));

    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x7d, 0x00, 0x02, 0x7d, 0x00, 0x02})));

    // create the row type
    std::unique_ptr<Type> rowType =
      createStructType({createPrimitiveType(STRING)}, {"col0"});
    rowType->assignIds(0);

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    StructVectorBatch batch(64);
    StringVectorBatch *strings = new StringVectorBatch(64);
    batch.fields.push_back(std::unique_ptr<ColumnVectorBatch>(strings));
    size_t next = 0;
    for(size_t i=0; i < 8; ++i) {
      reader->next(batch, 64, 0);
      ASSERT_EQ(64, batch.numElements);
      ASSERT_EQ(false, batch.hasNulls);
      ASSERT_EQ(64, strings->numElements);
      ASSERT_EQ(true, strings->hasNulls);
      for(size_t j=0; j < batch.numElements; ++j) {
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
    bool selectedColumns[] = {true, true};
    EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(nullptr));

    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(nullptr));

    // sum(0 to 1199)
    const size_t BLOB_SIZE = 719400;
    char blob[BLOB_SIZE];
    size_t posn = 0;
    for(size_t item=0; item < 1200; ++item) {
      for(size_t ch=0; ch < item; ++ch) {
        blob[posn++] = static_cast<char>(ch);
      }
    }
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      (blob, BLOB_SIZE, 200)));

    // the stream of 0 to 1199
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x7f, 0x01, 0x00,
                                          0x7f, 0x01, 0x82, 0x01,
                                          0x7f, 0x01, 0x84, 0x02,
                                          0x7f, 0x01, 0x86, 0x03,
                                          0x7f, 0x01, 0x88, 0x04,
                                          0x7f, 0x01, 0x8a, 0x05,
                                          0x7f, 0x01, 0x8c, 0x06,
                                          0x7f, 0x01, 0x8e, 0x07,
                                          0x7f, 0x01, 0x90, 0x08,
                                          0x1b, 0x01, 0x92, 0x09})));

    // create the row type
    std::unique_ptr<Type> rowType =
      createStructType({createPrimitiveType(STRING)}, {"col0"});
    rowType->assignIds(0);

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    StructVectorBatch batch(2);
    StringVectorBatch *strings = new StringVectorBatch(2);
    batch.fields.push_back(std::unique_ptr<ColumnVectorBatch>(strings));
    reader->next(batch, 2, 0);
    ASSERT_EQ(2, batch.numElements);
    ASSERT_EQ(false, batch.hasNulls);
    ASSERT_EQ(2, strings->numElements);
    ASSERT_EQ(false, strings->hasNulls);
    for(size_t i=0; i < batch.numElements; ++i) {
      ASSERT_EQ(i, strings->length[i]);
      for(size_t j=0; j < i; ++j) {
        EXPECT_EQ(static_cast<char>(j), strings->data[i][j]);
      }
    }
    reader->skip(14);
    reader->next(batch, 2, 0);
    ASSERT_EQ(2, batch.numElements);
    ASSERT_EQ(false, batch.hasNulls);
    ASSERT_EQ(2, strings->numElements);
    ASSERT_EQ(false, strings->hasNulls);
    for(size_t i=0; i < batch.numElements; ++i) {
      ASSERT_EQ(16 + i, strings->length[i]);
      for(size_t j=0; j < 16 + i; ++j) {
        EXPECT_EQ(static_cast<char>(j), strings->data[i][j]);
      }
    }
    reader->skip(1180);
    reader->next(batch, 2, 0);
    ASSERT_EQ(2, batch.numElements);
    ASSERT_EQ(false, batch.hasNulls);
    ASSERT_EQ(2, strings->numElements);
    ASSERT_EQ(false, strings->hasNulls);
    for(size_t i=0; i < batch.numElements; ++i) {
      ASSERT_EQ(1198 + i, strings->length[i]);
      for(size_t j=0; j < 1198 + i; ++j) {
        EXPECT_EQ(static_cast<char>(j), strings->data[i][j]);
      }
    }
  }

  TEST(TestColumnReader, testStringDirectSkipWithNulls) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    bool selectedColumns[] = {true, true};
    EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(nullptr));

    // alternate 4 non-null and 4 null via [0xf0 for x in range(2400 / 8)]
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x7f, 0xf0, 0x7f, 0xf0, 0x25, 0xf0})));

    // sum(range(1200))
    const size_t BLOB_SIZE = 719400;

    // each string is [x % 256 for x in range(r)]
    char blob[BLOB_SIZE];
    size_t posn = 0;
    for(size_t item=0; item < 1200; ++item) {
      for(size_t ch=0; ch < item; ++ch) {
        blob[posn++] = static_cast<char>(ch);
      }
    }
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      (blob, BLOB_SIZE, 200)));

    // range(1200)
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x7f, 0x01, 0x00,
                                          0x7f, 0x01, 0x82, 0x01,
                                          0x7f, 0x01, 0x84, 0x02,
                                          0x7f, 0x01, 0x86, 0x03,
                                          0x7f, 0x01, 0x88, 0x04,
                                          0x7f, 0x01, 0x8a, 0x05,
                                          0x7f, 0x01, 0x8c, 0x06,
                                          0x7f, 0x01, 0x8e, 0x07,
                                          0x7f, 0x01, 0x90, 0x08,
                                          0x1b, 0x01, 0x92, 0x09})));

    // create the row type
    std::unique_ptr<Type> rowType =
      createStructType({createPrimitiveType(STRING)}, {"col0"});
    rowType->assignIds(0);

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    StructVectorBatch batch(2);
    StringVectorBatch *strings = new StringVectorBatch(2);
    batch.fields.push_back(std::unique_ptr<ColumnVectorBatch>(strings));
    reader->next(batch, 2, 0);
    ASSERT_EQ(2, batch.numElements);
    ASSERT_EQ(false, batch.hasNulls);
    ASSERT_EQ(2, strings->numElements);
    ASSERT_EQ(false, strings->hasNulls);
    for(size_t i=0; i < batch.numElements; ++i) {
      ASSERT_EQ(i, strings->length[i]);
      for(size_t j=0; j < i; ++j) {
        EXPECT_EQ(static_cast<char>(j), strings->data[i][j]);
      }
    }
    reader->skip(30);
    reader->next(batch, 2, 0);
    ASSERT_EQ(2, batch.numElements);
    ASSERT_EQ(false, batch.hasNulls);
    ASSERT_EQ(2, strings->numElements);
    ASSERT_EQ(false, strings->hasNulls);
    for(size_t i=0; i < batch.numElements; ++i) {
      ASSERT_EQ(16 + i, strings->length[i]);
      for(size_t j=0; j < 16 + i; ++j) {
        EXPECT_EQ(static_cast<char>(j), strings->data[i][j]);
      }
    }
    reader->skip(2364);
    reader->next(batch, 2, 0);
    ASSERT_EQ(2, batch.numElements);
    ASSERT_EQ(false, batch.hasNulls);
    ASSERT_EQ(2, strings->numElements);
    ASSERT_EQ(true, strings->hasNulls);
    for(size_t i=0; i < batch.numElements; ++i) {
      EXPECT_EQ(false, strings->notNull[i]);
    }
  }

  TEST(TestColumnReader, testList) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    bool selectedColumns[] = {true, true, true};
    EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(testing::_,
                                        proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(nullptr));

    // [2 for x in range(600)]
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x7f, 0x00, 0x02,
                                          0x7f, 0x00, 0x02,
                                          0x7f, 0x00, 0x02,
                                          0x7f, 0x00, 0x02,
                                          0x4d, 0x00, 0x02})));

    // range(1200)
    EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_DATA))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x7f, 0x01, 0x00,
                                          0x7f, 0x01, 0x84, 0x02,
                                          0x7f, 0x01, 0x88, 0x04,
                                          0x7f, 0x01, 0x8c, 0x06,
                                          0x7f, 0x01, 0x90, 0x08,
                                          0x7f, 0x01, 0x94, 0x0a,
                                          0x7f, 0x01, 0x98, 0x0c,
                                          0x7f, 0x01, 0x9c, 0x0e,
                                          0x7f, 0x01, 0xa0, 0x10,
                                          0x1b, 0x01, 0xa4, 0x12})));

    // create the row type
    std::unique_ptr<Type> rowType =
      createStructType({createListType(createPrimitiveType(LONG))}, {"col0"});
    rowType->assignIds(0);

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    StructVectorBatch batch(512);
    ListVectorBatch *lists = new ListVectorBatch(512);
    LongVectorBatch *longs = new LongVectorBatch(512);
    batch.fields.push_back(std::unique_ptr<ColumnVectorBatch>(lists));
    lists->elements = std::unique_ptr<ColumnVectorBatch>(longs);
    reader->next(batch, 512, 0);
    ASSERT_EQ(512, batch.numElements);
    ASSERT_EQ(false, batch.hasNulls);
    ASSERT_EQ(512, lists->numElements);
    ASSERT_EQ(false, lists->hasNulls);
    ASSERT_EQ(1024, longs->numElements);
    ASSERT_EQ(false, longs->hasNulls);
    for(size_t i=0; i <= batch.numElements; ++i) {
      EXPECT_EQ(2*i, lists->offsets[i]);
    }
    for(size_t i=0; i < longs->numElements; ++i) {
      EXPECT_EQ(i, longs->data[i]);
    }
  }

  TEST(TestColumnReader, testListWithNulls) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    bool selectedColumns[] = {true, true, true};
    EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(nullptr));

    // [0xaa for x in range(2048/8)]
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x7f, 0xaa, 0x7b, 0xaa})));

    EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(nullptr));

    // [1 for x in range(260)] +
    // [4 for x in range(260)] +
    // [0 for x in range(260)] +
    // [3 for x in range(243)] +
    // [19]
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x7f, 0x00, 0x01,
                                          0x7f, 0x00, 0x01,
                                          0x7f, 0x00, 0x04,
                                          0x7f, 0x00, 0x04,
                                          0x7f, 0x00, 0x00,
                                          0x7f, 0x00, 0x00,
                                          0x7f, 0x00, 0x03,
                                          0x6e, 0x00, 0x03,
                                          0xff, 0x13})));

    // range(2048)
    EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_DATA))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x7f, 0x01, 0x00,
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
                                          0x5f, 0x01, 0xbc, 0x1e})));

    // create the row type
    std::unique_ptr<Type> rowType =
      createStructType({createListType(createPrimitiveType(LONG))}, {"col0"});
    rowType->assignIds(0);

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    StructVectorBatch batch(512);
    ListVectorBatch *lists = new ListVectorBatch(512);
    LongVectorBatch *longs = new LongVectorBatch(512);
    batch.fields.push_back(std::unique_ptr<ColumnVectorBatch>(lists));
    lists->elements = std::unique_ptr<ColumnVectorBatch>(longs);
    reader->next(batch, 512, 0);
    ASSERT_EQ(512, batch.numElements);
    ASSERT_EQ(false, batch.hasNulls);
    ASSERT_EQ(512, lists->numElements);
    ASSERT_EQ(true, lists->hasNulls);
    ASSERT_EQ(256, longs->numElements);
    ASSERT_EQ(false, longs->hasNulls);
    for(size_t i=0; i < batch.numElements; ++i) {
      EXPECT_EQ(i % 2 == 0, lists->notNull[i]) << "Wrong value at " << i;
      EXPECT_EQ((i + 1) / 2, lists->offsets[i]) << "Wrong value at " << i;
    }
    EXPECT_EQ(256, lists->offsets[512]);
    for(size_t i=0; i < longs->numElements; ++i) {
      EXPECT_EQ(i, longs->data[i]);
    }

    reader->next(batch, 512, 0);
    ASSERT_EQ(512, batch.numElements);
    ASSERT_EQ(false, batch.hasNulls);
    ASSERT_EQ(512, lists->numElements);
    ASSERT_EQ(true, lists->hasNulls);
    ASSERT_EQ(1012, longs->numElements);
    ASSERT_EQ(false, longs->hasNulls);
    for(size_t i=0; i < batch.numElements; ++i) {
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
    for(size_t i=0; i < longs->numElements; ++i) {
      EXPECT_EQ(256 + i, longs->data[i]);
    }

    reader->next(batch, 512, 0);
    ASSERT_EQ(512, batch.numElements);
    ASSERT_EQ(false, batch.hasNulls);
    ASSERT_EQ(512, lists->numElements);
    ASSERT_EQ(true, lists->hasNulls);
    ASSERT_EQ(32, longs->numElements);
    ASSERT_EQ(false, longs->hasNulls);
    for(size_t i=0; i < batch.numElements; ++i) {
      EXPECT_EQ(i % 2 == 0, lists->notNull[i]) << "Wrong value at " << i;
      if (i < 16) {
        EXPECT_EQ(4 * ((i + 1) / 2), lists->offsets[i])
          << "Wrong value at " << i;
      } else {
        EXPECT_EQ(32, lists->offsets[i]) << "Wrong value at " << i;
      }
    }
    EXPECT_EQ(32, lists->offsets[512]);
    for(size_t i=0; i < longs->numElements; ++i) {
      EXPECT_EQ(1268 + i, longs->data[i]);
    }

    reader->next(batch, 512, 0);
    ASSERT_EQ(512, batch.numElements);
    ASSERT_EQ(false, batch.hasNulls);
    ASSERT_EQ(512, lists->numElements);
    ASSERT_EQ(true, lists->hasNulls);
    ASSERT_EQ(748, longs->numElements);
    ASSERT_EQ(false, longs->hasNulls);
    for(size_t i=0; i < batch.numElements; ++i) {
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
    for(size_t i=0; i < longs->numElements; ++i) {
      EXPECT_EQ(1300 + i, longs->data[i]);
    }
  }

  TEST(TestColumnReader, testListSkipWithNulls) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    bool selectedColumns[] = {true, true, true};
    EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(nullptr));

    // [0xaa for x in range(2048/8)]
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x7f, 0xaa, 0x7b, 0xaa})));

    EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(nullptr));

    // [1 for x in range(260)] +
    // [4 for x in range(260)] +
    // [0 for x in range(260)] +
    // [3 for x in range(243)] +
    // [19]
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x7f, 0x00, 0x01,
                                          0x7f, 0x00, 0x01,
                                          0x7f, 0x00, 0x04,
                                          0x7f, 0x00, 0x04,
                                          0x7f, 0x00, 0x00,
                                          0x7f, 0x00, 0x00,
                                          0x7f, 0x00, 0x03,
                                          0x6e, 0x00, 0x03,
                                          0xff, 0x13})));

    // range(2048)
    EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_DATA))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x7f, 0x01, 0x00,
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
                                          0x5f, 0x01, 0xbc, 0x1e})));

    // create the row type
    std::unique_ptr<Type> rowType =
      createStructType({createListType(createPrimitiveType(LONG))}, {"col0"});
    rowType->assignIds(0);

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    StructVectorBatch batch(1);
    ListVectorBatch *lists = new ListVectorBatch(1);
    LongVectorBatch *longs = new LongVectorBatch(1);
    batch.fields.push_back(std::unique_ptr<ColumnVectorBatch>(lists));
    lists->elements = std::unique_ptr<ColumnVectorBatch>(longs);

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
    for(size_t i=0; i < longs->numElements; ++i) {
      EXPECT_EQ(2029 + i, longs->data[i]);
    }
  }

  TEST(TestColumnReader, testListSkipWithNullsNoData) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    bool selectedColumns[] = {true, true, false};
    EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(nullptr));

    // [0xaa for x in range(2048/8)]
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x7f, 0xaa, 0x7b, 0xaa})));

    EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(nullptr));

    // [1 for x in range(260)] +
    // [4 for x in range(260)] +
    // [0 for x in range(260)] +
    // [3 for x in range(243)] +
    // [19]
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x7f, 0x00, 0x01,
                                          0x7f, 0x00, 0x01,
                                          0x7f, 0x00, 0x04,
                                          0x7f, 0x00, 0x04,
                                          0x7f, 0x00, 0x00,
                                          0x7f, 0x00, 0x00,
                                          0x7f, 0x00, 0x03,
                                          0x6e, 0x00, 0x03,
                                          0xff, 0x13})));

    EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_DATA))
      .WillRepeatedly(testing::Return(nullptr));

    // create the row type
    std::unique_ptr<Type> rowType =
      createStructType({createListType(createPrimitiveType(LONG))}, {"col0"});
    rowType->assignIds(0);

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    StructVectorBatch batch(1);
    ListVectorBatch *lists = new ListVectorBatch(1);
    batch.fields.push_back(std::unique_ptr<ColumnVectorBatch>(lists));

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
    bool selectedColumns[] = {true, true, true, true};
    EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(testing::_,
                                        proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(nullptr));

    // [2 for x in range(600)]
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x7f, 0x00, 0x02,
                                          0x7f, 0x00, 0x02,
                                          0x7f, 0x00, 0x02,
                                          0x7f, 0x00, 0x02,
                                          0x4d, 0x00, 0x02})));

    // range(1200)
    EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_DATA))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x7f, 0x01, 0x00,
                                          0x7f, 0x01, 0x84, 0x02,
                                          0x7f, 0x01, 0x88, 0x04,
                                          0x7f, 0x01, 0x8c, 0x06,
                                          0x7f, 0x01, 0x90, 0x08,
                                          0x7f, 0x01, 0x94, 0x0a,
                                          0x7f, 0x01, 0x98, 0x0c,
                                          0x7f, 0x01, 0x9c, 0x0e,
                                          0x7f, 0x01, 0xa0, 0x10,
                                          0x1b, 0x01, 0xa4, 0x12})));

    // range(8, 1208)
    EXPECT_CALL(streams, getStreamProxy(3, proto::Stream_Kind_DATA))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x7f, 0x01, 0x10,
                                          0x7f, 0x01, 0x94, 0x02,
                                          0x7f, 0x01, 0x98, 0x04,
                                          0x7f, 0x01, 0x9c, 0x06,
                                          0x7f, 0x01, 0xa0, 0x08,
                                          0x7f, 0x01, 0xa4, 0x0a,
                                          0x7f, 0x01, 0xa8, 0x0c,
                                          0x7f, 0x01, 0xac, 0x0e,
                                          0x7f, 0x01, 0xb0, 0x10,
                                          0x1b, 0x01, 0xb4, 0x12})));

    // create the row type
    std::unique_ptr<Type> rowType =
      createStructType({createMapType(createPrimitiveType(LONG),
                                      createPrimitiveType(LONG))},
        {"col0", "col1"});
    rowType->assignIds(0);

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    StructVectorBatch batch(512);
    MapVectorBatch *maps = new MapVectorBatch(512);
    LongVectorBatch *keys = new LongVectorBatch(512);
    LongVectorBatch *elements = new LongVectorBatch(512);
    batch.fields.push_back(std::unique_ptr<ColumnVectorBatch>(maps));
    maps->keys = std::unique_ptr<ColumnVectorBatch>(keys);
    maps->elements = std::unique_ptr<ColumnVectorBatch>(elements);
    reader->next(batch, 512, 0);
    ASSERT_EQ(512, batch.numElements);
    ASSERT_EQ(false, batch.hasNulls);
    ASSERT_EQ(512, maps->numElements);
    ASSERT_EQ(false, maps->hasNulls);
    ASSERT_EQ(1024, keys->numElements);
    ASSERT_EQ(false, keys->hasNulls);
    ASSERT_EQ(1024, elements->numElements);
    ASSERT_EQ(false, elements->hasNulls);
    for(size_t i=0; i <= batch.numElements; ++i) {
      EXPECT_EQ(2*i, maps->offsets[i]);
    }
    for(size_t i=0; i < keys->numElements; ++i) {
      EXPECT_EQ(i, keys->data[i]);
      EXPECT_EQ(i + 8, elements->data[i]);
    }
  }

  TEST(TestColumnReader, testMapWithNulls) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    bool selectedColumns[] = {true, true, true, true};
    EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(nullptr));

    // [0xaa for x in range(2048/8)]
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x7f, 0xaa, 0x7b, 0xaa})));

    EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(nullptr));

    // [0x55 for x in range(2048/8)]
    EXPECT_CALL(streams, getStreamProxy(3, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x7f, 0x55, 0x7b, 0x55})));

    // [1 for x in range(260)] +
    // [4 for x in range(260)] +
    // [0 for x in range(260)] +
    // [3 for x in range(243)] +
    // [19]
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x7f, 0x00, 0x01,
                                          0x7f, 0x00, 0x01,
                                          0x7f, 0x00, 0x04,
                                          0x7f, 0x00, 0x04,
                                          0x7f, 0x00, 0x00,
                                          0x7f, 0x00, 0x00,
                                          0x7f, 0x00, 0x03,
                                          0x6e, 0x00, 0x03,
                                          0xff, 0x13})));

    // range(2048)
    EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_DATA))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x7f, 0x01, 0x00,
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
                                          0x5f, 0x01, 0xbc, 0x1e})));

    // range(8, 1032)
    EXPECT_CALL(streams, getStreamProxy(3, proto::Stream_Kind_DATA))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x7f, 0x01, 0x10,
                                          0x7f, 0x01, 0x94, 0x02,
                                          0x7f, 0x01, 0x98, 0x04,
                                          0x7f, 0x01, 0x9c, 0x06,
                                          0x7f, 0x01, 0xa0, 0x08,
                                          0x7f, 0x01, 0xa4, 0x0a,
                                          0x7f, 0x01, 0xa8, 0x0c,
                                          0x6f, 0x01, 0xac, 0x0e})));

    // create the row type
    std::unique_ptr<Type> rowType =
      createStructType({createMapType(createPrimitiveType(LONG),
                                      createPrimitiveType(LONG))},
        {"col0", "col1"});
    rowType->assignIds(0);

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    StructVectorBatch batch(512);
    MapVectorBatch *maps = new MapVectorBatch(512);
    LongVectorBatch *keys = new LongVectorBatch(512);
    LongVectorBatch *elements = new LongVectorBatch(512);
    batch.fields.push_back(std::unique_ptr<ColumnVectorBatch>(maps));
    maps->keys = std::unique_ptr<ColumnVectorBatch>(keys);
    maps->elements = std::unique_ptr<ColumnVectorBatch>(elements);
    reader->next(batch, 512, 0);
    ASSERT_EQ(512, batch.numElements);
    ASSERT_EQ(false, batch.hasNulls);
    ASSERT_EQ(512, maps->numElements);
    ASSERT_EQ(true, maps->hasNulls);
    ASSERT_EQ(256, keys->numElements);
    ASSERT_EQ(false, keys->hasNulls);
    ASSERT_EQ(256, elements->numElements);
    ASSERT_EQ(true, elements->hasNulls);
    for(size_t i=0; i < batch.numElements; ++i) {
      EXPECT_EQ(i % 2 == 0, maps->notNull[i]) << "Wrong value at " << i;
      EXPECT_EQ((i + 1) / 2, maps->offsets[i]) << "Wrong value at " << i;
    }
    EXPECT_EQ(256, maps->offsets[512]);
    for(size_t i=0; i < keys->numElements; ++i) {
      EXPECT_EQ(i, keys->data[i]);
      EXPECT_EQ(i & 1, elements->notNull[i]);
      if (elements->notNull[i]) {
        EXPECT_EQ(i/2 + 8, elements->data[i]);
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
    for(size_t i=0; i < batch.numElements; ++i) {
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
    for(size_t i=0; i < keys->numElements; ++i) {
      EXPECT_EQ(256 + i, keys->data[i]);
      EXPECT_EQ(i & 1, elements->notNull[i]);
      if (elements->notNull[i]) {
        EXPECT_EQ(128 + 8 + i/2, elements->data[i]);
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
    for(size_t i=0; i < batch.numElements; ++i) {
      EXPECT_EQ(i % 2 == 0, maps->notNull[i]) << "Wrong value at " << i;
      if (i < 16) {
        EXPECT_EQ(4 * ((i + 1) / 2), maps->offsets[i])
          << "Wrong value at " << i;
      } else {
        EXPECT_EQ(32, maps->offsets[i]) << "Wrong value at " << i;
      }
    }
    EXPECT_EQ(32, maps->offsets[512]);
    for(size_t i=0; i < keys->numElements; ++i) {
      EXPECT_EQ(1268 + i, keys->data[i]);
      EXPECT_EQ(i & 1, elements->notNull[i]);
      if (elements->notNull[i]) {
        EXPECT_EQ(634 + 8 + i/2, elements->data[i]);
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
    for(size_t i=0; i < batch.numElements; ++i) {
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
    for(size_t i=0; i < keys->numElements; ++i) {
      EXPECT_EQ(1300 + i, keys->data[i]);
      EXPECT_EQ(i & 1, elements->notNull[i]);
      if (elements->notNull[i]) {
        EXPECT_EQ(650 + 8 + i/2, elements->data[i]);
      }
    }
  }

  TEST(TestColumnReader, testMapSkipWithNulls) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    bool selectedColumns[] = {true, true, true, true};
    EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(testing::_,proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(nullptr));

    // [0xaa for x in range(2048/8)]
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x7f, 0xaa, 0x7b, 0xaa})));

    // [1 for x in range(260)] +
    // [4 for x in range(260)] +
    // [0 for x in range(260)] +
    // [3 for x in range(243)] +
    // [19]
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x7f, 0x00, 0x01,
                                          0x7f, 0x00, 0x01,
                                          0x7f, 0x00, 0x04,
                                          0x7f, 0x00, 0x04,
                                          0x7f, 0x00, 0x00,
                                          0x7f, 0x00, 0x00,
                                          0x7f, 0x00, 0x03,
                                          0x6e, 0x00, 0x03,
                                          0xff, 0x13})));

    // range(2048)
    EXPECT_CALL(streams, getStreamProxy(2, proto::Stream_Kind_DATA))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x7f, 0x01, 0x00,
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
                                          0x5f, 0x01, 0xbc, 0x1e})));

    // range(8, 2056)
    EXPECT_CALL(streams, getStreamProxy(3, proto::Stream_Kind_DATA))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x7f, 0x01, 0x10,
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
                                          0x5f, 0x01, 0xcc, 0x1e})));

    // create the row type
    std::unique_ptr<Type> rowType =
      createStructType({createMapType(createPrimitiveType(LONG),
                                      createPrimitiveType(LONG))},
        {"col0", "col1"});
    rowType->assignIds(0);

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    StructVectorBatch batch(1);
    MapVectorBatch *maps = new MapVectorBatch(1);
    LongVectorBatch *keys = new LongVectorBatch(1);
    LongVectorBatch *elements = new LongVectorBatch(1);
    batch.fields.push_back(std::unique_ptr<ColumnVectorBatch>(maps));
    maps->keys = std::unique_ptr<ColumnVectorBatch>(keys);
    maps->elements = std::unique_ptr<ColumnVectorBatch>(elements);

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
    for(size_t i=0; i < keys->numElements; ++i) {
      EXPECT_EQ(2029 + i, keys->data[i]);
      EXPECT_EQ(2029 + 8 + i, elements->data[i]);
    }
  }

  TEST(TestColumnReader, testMapSkipWithNullsNoData) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    bool selectedColumns[] = {true, true, false, false};
    EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(testing::_,proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(nullptr));

    // [0xaa for x in range(2048/8)]
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x7f, 0xaa, 0x7b, 0xaa})));

    // [1 for x in range(260)] +
    // [4 for x in range(260)] +
    // [0 for x in range(260)] +
    // [3 for x in range(243)] +
    // [19]
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH))
      .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                      ({0x7f, 0x00, 0x01,
                                          0x7f, 0x00, 0x01,
                                          0x7f, 0x00, 0x04,
                                          0x7f, 0x00, 0x04,
                                          0x7f, 0x00, 0x00,
                                          0x7f, 0x00, 0x00,
                                          0x7f, 0x00, 0x03,
                                          0x6e, 0x00, 0x03,
                                          0xff, 0x13})));

    // create the row type
    std::unique_ptr<Type> rowType =
      createStructType({createMapType(createPrimitiveType(LONG),
                                      createPrimitiveType(LONG))},
        {"col0", "col1"});
    rowType->assignIds(0);

    std::unique_ptr<ColumnReader> reader = buildReader(*rowType, streams);

    StructVectorBatch batch(1);
    MapVectorBatch *maps = new MapVectorBatch(1);
    batch.fields.push_back(std::unique_ptr<ColumnVectorBatch>(maps));

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
      std::unique_ptr<bool[]> selectedColumns =
        std::unique_ptr<bool[]>(new bool[2]);
      memset(selectedColumns.get(), true, 2);
      EXPECT_CALL(streams, getSelectedColumns())
        .WillRepeatedly(testing::Return(selectedColumns.get()));

      // set getEncoding
      proto::ColumnEncoding directEncoding;
      directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
      EXPECT_CALL(streams, getEncoding(testing::_))
        .WillRepeatedly(testing::Return(directEncoding));

      // set getStream
      EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT))
        .WillRepeatedly(testing::Return(nullptr));

      // 13 non-nulls followed by 19 nulls
      EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT))
        .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                        ({0xfc, 0xff, 0xf8, 0x0, 0x0})));

      float test_vals[] = {1.0f, 2.5f, -100.125f, 10000.0f, 1.234567E23f,
                           -2.3456E-12f, 1.0f/0, 0.0f/0, -1.0f/0,
                           3.4028235E38f, -3.4028235E38f, 1.4e-45f, -1.4e-45f};
      EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA))
        .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                        ({0x00, 0x00, 0x80, 0x3f,
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
        createStructType({createPrimitiveType(FLOAT)}, {"myFloat"});
      rowType->assignIds(0);

      std::unique_ptr<ColumnReader> reader =
        buildReader(*rowType, streams);

      DoubleVectorBatch *doubleBatch = new DoubleVectorBatch(1024);
      StructVectorBatch batch(1024);
      batch.fields.push_back(std::unique_ptr<ColumnVectorBatch>(doubleBatch));
      reader->next(batch, 32, 0);
      ASSERT_EQ(32, batch.numElements);
      ASSERT_EQ(false, batch.hasNulls);
      ASSERT_EQ(32, doubleBatch->numElements);
      ASSERT_EQ(true, doubleBatch->hasNulls);

      for(size_t i=0; i < batch.numElements; ++i) {
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
        std::unique_ptr<bool[]> selectedColumns =
          std::unique_ptr<bool[]>(new bool[2]);
        memset(selectedColumns.get(), true, 2);
        EXPECT_CALL(streams, getSelectedColumns())
          .WillRepeatedly(testing::Return(selectedColumns.get()));

        // set getEncoding
        proto::ColumnEncoding directEncoding;
        directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
        EXPECT_CALL(streams, getEncoding(testing::_))
          .WillRepeatedly(testing::Return(directEncoding));

        // set getStream
        EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT))
          .WillRepeatedly(testing::Return(nullptr));

        // 2 non-nulls, 2 nulls, 2 non-nulls, 2 nulls
        EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT))
          .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                          ({0xff, 0xcc})));

        // 1, 2.5, -100.125, 10000
        EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA))
          .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                          ({0x00, 0x00, 0x80, 0x3f,
                                            0x00, 0x00, 0x20, 0x40,
                                            0x00, 0x40, 0xc8, 0xc2,
                                            0x00, 0x40, 0x1c, 0x46
                                          })));
        // create the row type
        std::unique_ptr<Type> rowType =
          createStructType({createPrimitiveType(FLOAT)}, {"myFloat"});
        rowType->assignIds(0);

        std::unique_ptr<ColumnReader> reader =
          buildReader(*rowType, streams);

        DoubleVectorBatch *doubleBatch = new DoubleVectorBatch(1024);
        StructVectorBatch batch(1024);
        batch.fields.push_back(std::unique_ptr<ColumnVectorBatch>
                               (doubleBatch));

        float test_vals[] = {1.0, 2.5, -100.125, 10000.0 };
        int vals_ix = 0;

        reader->next(batch, 3, 0);
        ASSERT_EQ(3, batch.numElements);
        ASSERT_EQ(false, batch.hasNulls);
        ASSERT_EQ(3, doubleBatch->numElements);
        ASSERT_EQ(true, doubleBatch->hasNulls);

        for(size_t i=0; i < batch.numElements; ++i) {
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
        for(size_t i=0; i < batch.numElements; ++i) {
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
      std::unique_ptr<bool[]> selectedColumns =
        std::unique_ptr<bool[]>(new bool[2]);
      memset(selectedColumns.get(), true, 2);
      EXPECT_CALL(streams, getSelectedColumns())
        .WillRepeatedly(testing::Return(selectedColumns.get()));

      // set getEncoding
      proto::ColumnEncoding directEncoding;
      directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
      EXPECT_CALL(streams, getEncoding(testing::_))
        .WillRepeatedly(testing::Return(directEncoding));

      // set getStream
      EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT))
        .WillRepeatedly(testing::Return(nullptr));

      // 13 non-nulls followed by 19 nulls
      EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT))
        .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                        ({0xfc, 0xff, 0xf8, 0x0, 0x0})));

      double test_vals[] = {1.0, 2.0, -2.0, 100.0, 1.23456789E32,
                            -3.42234E-18, 1.0/0, 0.0/0, -1.0/0,
                            1.7976931348623157e308, -1.7976931348623157E308,
                            4.9e-324, -4.9e-324};
      EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA))
        .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                  ({0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf0, 0x3f,
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
        createStructType({createPrimitiveType(DOUBLE)}, {"myDouble"});
      rowType->assignIds(0);

      std::unique_ptr<ColumnReader> reader =
        buildReader(*rowType, streams);

      DoubleVectorBatch *doubleBatch = new DoubleVectorBatch(1024);
      StructVectorBatch batch(1024);
      batch.fields.push_back(std::unique_ptr<ColumnVectorBatch>(doubleBatch));
      reader->next(batch, 32, 0);
      ASSERT_EQ(32, batch.numElements);
      ASSERT_EQ(false, batch.hasNulls);
      ASSERT_EQ(32, doubleBatch->numElements);
      ASSERT_EQ(true, doubleBatch->hasNulls);

      for(size_t i=0; i < batch.numElements; ++i) {
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
        std::unique_ptr<bool[]> selectedColumns =
          std::unique_ptr<bool[]>(new bool[2]);
        memset(selectedColumns.get(), true, 2);
        EXPECT_CALL(streams, getSelectedColumns())
          .WillRepeatedly(testing::Return(selectedColumns.get()));

        // set getEncoding
        proto::ColumnEncoding directEncoding;
        directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
        EXPECT_CALL(streams, getEncoding(testing::_))
          .WillRepeatedly(testing::Return(directEncoding));

        // set getStream
        EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT))
          .WillRepeatedly(testing::Return(nullptr));

        // 1 non-null, 5 nulls, 2 non-nulls
        EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_PRESENT))
          .WillRepeatedly(testing::Return(new SeekableArrayInputStream
                                          ({0xff, 0x83})));

        // 1, 2, -2
        EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA))
          .WillRepeatedly(testing::Return
                          (new SeekableArrayInputStream
                           ({0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f,
                               0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40,
                               0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xc0
                               })));

        // create the row type
        std::unique_ptr<Type> rowType =
          createStructType({createPrimitiveType(DOUBLE)}, {"myDouble"});
        rowType->assignIds(0);

        std::unique_ptr<ColumnReader> reader =
          buildReader(*rowType, streams);

        DoubleVectorBatch *doubleBatch = new DoubleVectorBatch(1024);
        StructVectorBatch batch(1024);
        batch.fields.push_back(std::unique_ptr<ColumnVectorBatch>
                               (doubleBatch));

        double test_vals[] = {1.0, 2.0, -2.0 };
        int vals_ix = 0;

        reader->next(batch, 2, 0);
        ASSERT_EQ(2, batch.numElements);
        ASSERT_EQ(false, batch.hasNulls);
        ASSERT_EQ(2, doubleBatch->numElements);
        ASSERT_EQ(true, doubleBatch->hasNulls);

        for(size_t i=0; i < batch.numElements; ++i) {
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
        for(size_t i=0; i < batch.numElements; ++i) {
          if (i < 1) {
            EXPECT_EQ(0, doubleBatch->notNull[i]);
          } else {
            EXPECT_EQ(1, doubleBatch->notNull[i]);
            EXPECT_DOUBLE_EQ(test_vals[vals_ix], doubleBatch->data[i]);
            vals_ix++;
          }
        }
      }

  TEST(TestColumnReader, testUnimplementedTypes) {
    MockStripeStreams streams;

    // set getSelectedColumns()
    std::unique_ptr<bool[]> selectedColumns =
      std::unique_ptr<bool[]>(new bool[2]);
    memset(selectedColumns.get(), true, 2);
    EXPECT_CALL(streams, getSelectedColumns())
      .WillRepeatedly(testing::Return(selectedColumns.get()));

    // set getEncoding
    proto::ColumnEncoding directEncoding;
    directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    EXPECT_CALL(streams, getEncoding(testing::_))
      .WillRepeatedly(testing::Return(directEncoding));

    // set getStream
    EXPECT_CALL(streams, getStreamProxy(0, proto::Stream_Kind_PRESENT))
      .WillRepeatedly(testing::Return(nullptr));

    // create the row type
    std::unique_ptr<Type> rowType;
    rowType = createStructType({createPrimitiveType(TIMESTAMP)}, {"col0"});
    rowType->assignIds(0);
    EXPECT_THROW(buildReader(*rowType, streams), NotImplementedYet);

    rowType = createStructType({createPrimitiveType(UNION)}, {"col0"});
    rowType->assignIds(0);
    EXPECT_THROW(buildReader(*rowType, streams), NotImplementedYet);

    rowType = createStructType({createPrimitiveType(DECIMAL)}, {"col0"});
    rowType->assignIds(0);
    EXPECT_THROW(buildReader(*rowType, streams), NotImplementedYet);
  }

}  // namespace orc
