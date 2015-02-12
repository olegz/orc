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

using namespace std;
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
  const std::vector<bool> selected = reader->getSelectedColumns();
  for (size_t i = 0; i < 10; ++i) {
    EXPECT_EQ(true, selected[i]) << "fail on " << i;
  }

  unsigned long rowCount = 0;
  std::unique_ptr<orc::ColumnVectorBatch> batch = reader->createRowBatch(1024);
  orc::LongVectorBatch* longVector =
    dynamic_cast<orc::LongVectorBatch*>
    (dynamic_cast<orc::StructVectorBatch&>(*batch).fields[0]);
  int64_t* idCol = longVector->data.data();
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

TEST(Reader, zlibReaderTest) {
  orc::ReaderOptions opts;
  std::ostringstream filename;
  filename << exampleDirectory << "/demo-11-zlib.orc";

  std::unique_ptr<orc::Reader> reader =
    orc::createReader(orc::readLocalFile(filename.str()), opts);

  EXPECT_EQ(orc::CompressionKind_ZLIB, reader->getCompression());
  EXPECT_EQ(256 * 1024, reader->getCompressionSize());
  EXPECT_EQ(385, reader->getNumberOfStripes());
  EXPECT_EQ(1920800, reader->getNumberOfRows());
  EXPECT_EQ(10000, reader->getRowIndexStride());
  EXPECT_EQ(396823, reader->getContentLength());
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
  const std::vector<bool> selected = reader->getSelectedColumns();
  for (size_t i = 0; i < 10; ++i) {
    EXPECT_EQ(true, selected[i]) << "fail on " << i;
  }

  unsigned long rowCount = 0;
  std::unique_ptr<orc::ColumnVectorBatch> batch = reader->createRowBatch(1024);
  orc::LongVectorBatch* longVector =
    dynamic_cast<orc::LongVectorBatch*>
      (dynamic_cast<orc::StructVectorBatch&>(*batch).fields[0]);
  while (reader->next(*batch)) {
    EXPECT_EQ(rowCount, reader->getRowNumber());
    for(unsigned int i=0; i < batch->numElements; ++i) {
      EXPECT_EQ(rowCount + i + 1, longVector->data[i]) << "Bad id for " << i;
    }
    rowCount += batch->numElements;
  }
  EXPECT_EQ(1920800, rowCount);
  EXPECT_EQ(1920000, reader->getRowNumber());
}

TEST(Reader, zlibReaderTestRle2) {
  orc::ReaderOptions opts;
  std::ostringstream filename;
  filename << exampleDirectory << "/demo-12-zlib.orc";

  std::unique_ptr<orc::Reader> reader =
    orc::createReader(orc::readLocalFile(filename.str()), opts);

  EXPECT_EQ(orc::CompressionKind_ZLIB, reader->getCompression());
  EXPECT_EQ(256 * 1024, reader->getCompressionSize());
  EXPECT_EQ(1, reader->getNumberOfStripes());
  EXPECT_EQ(1920800, reader->getNumberOfRows());
  EXPECT_EQ(10000, reader->getRowIndexStride());
  EXPECT_EQ(45592, reader->getContentLength());
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
  const std::vector<bool> selected = reader->getSelectedColumns();
  for (size_t i = 0; i < 10; ++i) {
    EXPECT_EQ(true, selected[i]) << "fail on " << i;
  }

  unsigned long rowCount = 0;
  std::unique_ptr<orc::ColumnVectorBatch> batch = reader->createRowBatch(1024);
  orc::LongVectorBatch* longVector =
    dynamic_cast<orc::LongVectorBatch*>
      (dynamic_cast<orc::StructVectorBatch&>(*batch).fields[0]);
  while (reader->next(*batch)) {
    EXPECT_EQ(rowCount, reader->getRowNumber());
    for(unsigned int i=0; i < batch->numElements; ++i) {
      EXPECT_EQ(rowCount + i + 1, longVector->data[i]) << "Bad id for " << i;
    }
    rowCount += batch->numElements;
  }
  EXPECT_EQ(1920800, rowCount);
  EXPECT_EQ(1920000, reader->getRowNumber());
}

TEST(Reader, columnSelectionTest) {
  orc::ReaderOptions opts;
  std::list<int> includes = {1,3,5,7,9};
  opts.include(includes);
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

  const std::vector<bool> selected = reader->getSelectedColumns();
  EXPECT_EQ(true, selected[0]) << "fail on " << 0;
  for (size_t i = 1; i < 10; ++i) {
    EXPECT_EQ(i%2==1?true:false, selected[i]) << "fail on " << i;
  }

  unsigned long rowCount = 0;
  std::unique_ptr<orc::ColumnVectorBatch> batch = reader->createRowBatch(1024);
  orc::LongVectorBatch* longVector =
    dynamic_cast<orc::LongVectorBatch*>
    (dynamic_cast<orc::StructVectorBatch&>(*batch).fields[0]);
  int64_t* idCol = longVector->data.data();
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

TEST(Reader, stripeInformationTest) {
  orc::ReaderOptions opts;
  std::ostringstream filename;
  filename << exampleDirectory << "/demo-11-none.orc";
  std::unique_ptr<orc::Reader> reader =
    orc::createReader(orc::readLocalFile(filename.str()), opts);

  EXPECT_EQ(385, reader->getNumberOfStripes());

  std::unique_ptr<orc::StripeInformation> stripeInfo = reader->getStripe(7);
  EXPECT_EQ(92143, stripeInfo->getOffset());
  EXPECT_EQ(13176, stripeInfo->getLength());
  EXPECT_EQ(234, stripeInfo->getIndexLength());
  EXPECT_EQ(12673, stripeInfo->getDataLength());
  EXPECT_EQ(269, stripeInfo->getFooterLength());
  EXPECT_EQ(5000, stripeInfo->getNumberOfRows());
}

TEST(Reader, readRangeTest) {
  orc::ReaderOptions fullOpts, lastOpts, oobOpts, offsetOpts;
  // stripes[N-1]
  lastOpts.range(5067085, 1);
  // stripes[N]
  oobOpts.range(5067086, 4096);
  // stripes[7, 16]
  offsetOpts.range(80000, 130722);
  std::ostringstream filename;
  filename << exampleDirectory << "/demo-11-none.orc";
  std::unique_ptr<orc::Reader> fullReader =
    orc::createReader(orc::readLocalFile(filename.str()), fullOpts);
  std::unique_ptr<orc::Reader> lastReader =
    orc::createReader(orc::readLocalFile(filename.str()), lastOpts);
  std::unique_ptr<orc::Reader> oobReader =
    orc::createReader(orc::readLocalFile(filename.str()), oobOpts);
  std::unique_ptr<orc::Reader> offsetReader =
    orc::createReader(orc::readLocalFile(filename.str()), offsetOpts);

  std::unique_ptr<orc::ColumnVectorBatch> oobBatch =
    oobReader->createRowBatch(5000);
  EXPECT_FALSE(oobReader->next(*oobBatch));

  // advance fullReader to align with offsetReader
  std::unique_ptr<orc::ColumnVectorBatch> fullBatch =
    fullReader->createRowBatch(5000);
  for (int i=0; i < 7; ++i) {
    EXPECT_TRUE(fullReader->next(*fullBatch));
    EXPECT_EQ(5000, fullBatch->numElements);
  }

  std::unique_ptr<orc::ColumnVectorBatch> offsetBatch =
    offsetReader->createRowBatch(5000);
  orc::LongVectorBatch* fullLongVector =
    dynamic_cast<orc::LongVectorBatch*>
    (dynamic_cast<orc::StructVectorBatch&>(*fullBatch).fields[0]);
  int64_t* fullId = fullLongVector->data.data();
  orc::LongVectorBatch* offsetLongVector =
    dynamic_cast<orc::LongVectorBatch*>
    (dynamic_cast<orc::StructVectorBatch&>(*offsetBatch).fields[0]);
  int64_t* offsetId = offsetLongVector->data.data();
  for (int i=7; i < 17; ++i) {
    EXPECT_TRUE(fullReader->next(*fullBatch));
    EXPECT_TRUE(offsetReader->next(*offsetBatch));
    EXPECT_EQ(fullBatch->numElements, offsetBatch->numElements);
    for (unsigned j=0; j < fullBatch->numElements; ++j) {
      EXPECT_EQ(fullId[j], offsetId[j]);
    }
  }
  EXPECT_FALSE(offsetReader->next(*offsetBatch));

  // advance fullReader to align with lastReader
  for (int i=17; i < 384; ++i) {
    EXPECT_TRUE(fullReader->next(*fullBatch));
    EXPECT_EQ(5000, fullBatch->numElements);
  }

  std::unique_ptr<orc::ColumnVectorBatch> lastBatch =
    lastReader->createRowBatch(5000);
  orc::LongVectorBatch* lastLongVector =
    dynamic_cast<orc::LongVectorBatch*>
    (dynamic_cast<orc::StructVectorBatch&>(*lastBatch).fields[0]);
  int64_t* lastId = lastLongVector->data.data();
  EXPECT_TRUE(fullReader->next(*fullBatch));
  EXPECT_TRUE(lastReader->next(*lastBatch));
  EXPECT_EQ(fullBatch->numElements, lastBatch->numElements);
  for (unsigned i=0; i < fullBatch->numElements; ++i) {
    EXPECT_EQ(fullId[i], lastId[i]);
  }
  EXPECT_FALSE(fullReader->next(*fullBatch));
  EXPECT_FALSE(lastReader->next(*lastBatch));
}


TEST(Reader, columnStatistics) {
  orc::ReaderOptions opts;
  std::ostringstream filename;
  filename << exampleDirectory << "/demo-11-none.orc";
  std::unique_ptr<orc::Reader> reader =
    orc::createReader(orc::readLocalFile(filename.str()), opts);
  
  // test column statistics
  EXPECT_EQ(9, reader->getStatistics().size());

  // column[5]
  std::unique_ptr<orc::ColumnStatistics> col_5 = reader->getColumnStatistics(5);
  const orc::StringColumnStatistics& strStats = dynamic_cast<const orc::StringColumnStatistics&> (*(col_5.get()));
  EXPECT_EQ("Good", strStats.getMinimum());
  EXPECT_EQ("Unknown", strStats.getMaximum());
  
  // column[6]
  std::unique_ptr<orc::ColumnStatistics> col_6 = reader->getColumnStatistics(6);
  const orc::IntegerColumnStatistics& intStats = dynamic_cast<const orc::IntegerColumnStatistics&> (*(col_6.get()));
  EXPECT_EQ(0, intStats.getMinimum());
  EXPECT_EQ(6, intStats.getMaximum());
  EXPECT_EQ(5762400, intStats.getSum());
}

TEST(Reader, stripeStatistics) {
  orc::ReaderOptions opts;
  std::ostringstream filename;
  filename << exampleDirectory << "/demo-11-none.orc";
  std::unique_ptr<orc::Reader> reader =
    orc::createReader(orc::readLocalFile(filename.str()), opts);
  
  // test stripe statistics
  // stripe[60]
  unsigned long stripeIdx = 60;
  std::unique_ptr<orc::StripeStatistics> stripeStats = reader->getStripeStatistics(stripeIdx);

  EXPECT_EQ(9, stripeStats->getNumberOfColumnStatistics());

  // column[5]
  std::unique_ptr<orc::ColumnStatistics> col_5 = stripeStats->getColumnStatisticsInStripe(5);
  const orc::StringColumnStatistics& strStats = dynamic_cast<const orc::StringColumnStatistics&> (*(col_5.get()));
  EXPECT_EQ("Good", strStats.getMinimum());
  EXPECT_EQ("Unknown", strStats.getMaximum());
  
  // column[6]
  std::unique_ptr<orc::ColumnStatistics> col_6 = stripeStats->getColumnStatisticsInStripe(6);
  const orc::IntegerColumnStatistics& intStats = dynamic_cast<const orc::IntegerColumnStatistics&> (*(col_6.get()));
  EXPECT_EQ(5, intStats.getMinimum());
  EXPECT_EQ(6, intStats.getMaximum());
  EXPECT_EQ(27000, intStats.getSum());
}

}  // namespace
