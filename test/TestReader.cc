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

#include "orc/OrcFile.hh"
#include "TestDriver.hh"

#include "wrap/gmock.h"
#include "wrap/gtest-wrapper.h"

#include <sstream>

namespace {

using ::testing::IsEmpty;

TEST(Reader, simpleTest) {
  orc::ReaderOptions opts;
  std::ostringstream filename;
  filename << exampleDirectory << "/demo-11-none.orc";
  std::unique_ptr<orc::Reader> reader =
    orc::createReader(orc::readLocalFile(filename.str()), opts);

  EXPECT_EQ(orc::CompressionKind_NONE, reader->getCompression());
  EXPECT_EQ(256 * 1024, reader->getCompressionSize());
  EXPECT_EQ(385, reader->getNumberOfStripes());
  EXPECT_EQ(1920800, reader->getNumberOfRows());
  EXPECT_EQ(10000, reader->getRowIndexStride());
  EXPECT_EQ(5069718, reader->getContentLength());
  EXPECT_EQ(filename.str(), reader->getStreamName());
  EXPECT_THAT(reader->getMetadataKeys(), IsEmpty());
  EXPECT_FALSE(reader->hasMetadataValue("foo"));
  EXPECT_EQ(18446744073709551615UL, reader->getRowNumber());

  const orc::Type& rootType = reader->getType();
  EXPECT_EQ(0, rootType.getColumnId());
  EXPECT_EQ(orc::STRUCT, rootType.getKind());
  ASSERT_EQ(9, rootType.getSubtypeCount());
  EXPECT_EQ("_col0", rootType.getFieldName(0));
  EXPECT_EQ("_col1", rootType.getFieldName(1));
  EXPECT_EQ("_col2", rootType.getFieldName(2));
  EXPECT_EQ("_col3", rootType.getFieldName(3));
  EXPECT_EQ("_col4", rootType.getFieldName(4));
  EXPECT_EQ("_col5", rootType.getFieldName(5));
  EXPECT_EQ("_col6", rootType.getFieldName(6));
  EXPECT_EQ("_col7", rootType.getFieldName(7));
  EXPECT_EQ("_col8", rootType.getFieldName(8));
  EXPECT_EQ(orc::INT, rootType.getSubtype(0).getKind());
  EXPECT_EQ(orc::STRING, rootType.getSubtype(1).getKind());
  EXPECT_EQ(orc::STRING, rootType.getSubtype(2).getKind());
  EXPECT_EQ(orc::STRING, rootType.getSubtype(3).getKind());
  EXPECT_EQ(orc::INT, rootType.getSubtype(4).getKind());
  EXPECT_EQ(orc::STRING, rootType.getSubtype(5).getKind());
  EXPECT_EQ(orc::INT, rootType.getSubtype(6).getKind());
  EXPECT_EQ(orc::INT, rootType.getSubtype(7).getKind());
  EXPECT_EQ(orc::INT, rootType.getSubtype(8).getKind());
  for(unsigned int i=0; i < 9; ++i) {
    EXPECT_EQ(i + 1, rootType.getSubtype(i).getColumnId()) << "fail on " << i;
  }
  const bool* const selected = reader->getSelectedColumns();
  for (size_t i = 0; i < 10; ++i) {
    EXPECT_EQ(true, selected[i]) << "fail on " << i;
  }

  unsigned long rowCount = 0;
  std::unique_ptr<orc::ColumnVectorBatch> batch = reader->createRowBatch(1024);
  orc::LongVectorBatch* longVector =
    dynamic_cast<orc::LongVectorBatch*>
    (dynamic_cast<orc::StructVectorBatch&>(*batch).fields[0].get());
  long* idCol = longVector->data.get();
  while (reader->next(*batch)) {
    EXPECT_EQ(rowCount, reader->getRowNumber());
    for(unsigned int i=0; i < batch->numElements; ++i) {
      EXPECT_EQ(rowCount + i + 1, idCol[i]) << "Bad id for " << i;
    }
    rowCount += batch->numElements;
  }
  EXPECT_EQ(1920800, rowCount);
  EXPECT_EQ(1920000, reader->getRowNumber());
}

}  // namespace
