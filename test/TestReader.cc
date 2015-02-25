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

#ifdef __clang__
#pragma clang diagnostic ignored "-Wmissing-variable-declarations"
#endif

namespace orc {

  class OrcFileDescription {
  public:
    std::string filename;
    std::string typeString;
    uint64_t rowCount;
    uint64_t contentLength;
    uint64_t stripeCount;
    CompressionKind compression;
    size_t compressionSize;
    uint64_t rowIndexStride;

    OrcFileDescription(const std::string& _filename,
                       const std::string& _typeString,
                       uint64_t _rowCount,
                       uint64_t _contentLength,
                       uint64_t _stripeCount,
                       CompressionKind _compression,
                       size_t _compressionSize,
                       uint64_t _rowIndexStride
                       ): filename(_filename),
                          typeString(_typeString),
                          rowCount(_rowCount),
                          contentLength(_contentLength),
                          stripeCount(_stripeCount),
                          compression(_compression),
                          compressionSize(_compressionSize),
                          rowIndexStride(_rowIndexStride) {
      // PASS
    }

    friend std::ostream& operator<< (std::ostream& stream,
                                     OrcFileDescription const& obj);
  };

  std::ostream& operator<< (std::ostream& stream,
                            OrcFileDescription const& obj) {
    stream << obj.filename;
    return stream;
  }

  class MatchTest: public testing::TestWithParam<OrcFileDescription> {
  public:
    virtual ~MatchTest();

    std::string getFilename() {
      std::ostringstream filename;
      filename << exampleDirectory << "/" << GetParam().filename << ".orc";
      return filename.str();
    }

    std::string getJsonFilename() {
      std::ostringstream filename;
      filename << exampleDirectory << "/" << GetParam().filename << ".jsn.gz";
      return filename.str();
    }
  };

  MatchTest::~MatchTest() {
    // PASS
  }

  TEST_P(MatchTest, Metadata) {
    orc::ReaderOptions opts;
    std::unique_ptr<Reader> reader =
      createReader(readLocalFile(getFilename()), opts);

    EXPECT_EQ(GetParam().compression, reader->getCompression());
    EXPECT_EQ(GetParam().compressionSize, reader->getCompressionSize());
    EXPECT_EQ(GetParam().stripeCount, reader->getNumberOfStripes());
    EXPECT_EQ(GetParam().rowCount, reader->getNumberOfRows());
    EXPECT_EQ(GetParam().rowIndexStride, reader->getRowIndexStride());
    EXPECT_EQ(GetParam().contentLength, reader->getContentLength());
    EXPECT_EQ(getFilename(), reader->getStreamName());
    EXPECT_THAT(reader->getMetadataKeys(), testing::IsEmpty());
    EXPECT_FALSE(reader->hasMetadataValue("foo"));
    EXPECT_EQ(18446744073709551615UL, reader->getRowNumber());

    EXPECT_EQ(GetParam().typeString, reader->getType().toString());
  }

  TEST_P(MatchTest, Contents) {
    orc::ReaderOptions opts;
    std::unique_ptr<Reader> reader =
      createReader(readLocalFile(getFilename()), opts);
    unsigned long rowCount = 0;
    std::unique_ptr<ColumnVectorBatch> batch = reader->createRowBatch(1024);
    while (reader->next(*batch)) {
      EXPECT_EQ(rowCount, reader->getRowNumber());
      rowCount += batch->numElements;
    }
    EXPECT_EQ(GetParam().rowCount, rowCount);
    EXPECT_EQ(GetParam().rowCount, reader->getRowNumber());
  }

  INSTANTIATE_TEST_CASE_P(TestReader, MatchTest,
    testing::Values(OrcFileDescription("demo-11-none",
                                       ("struct<_col0:int,_col1:string,"
                                        "_col2:string,_col3:string,_col4:int,"
                                        "_col5:string,_col6:int,_col7:int,"
                                        "_col8:int>"),
                                       1920800,
                                       5069718,
                                       385,
                                       CompressionKind_NONE,
                                       262144,
                                       10000),
                    OrcFileDescription("demo-11-zlib",
                                       ("struct<_col0:int,_col1:string,"
                                        "_col2:string,_col3:string,_col4:int,"
                                        "_col5:string,_col6:int,_col7:int,"
                                        "_col8:int>"),
                                       1920800,
                                       396823,
                                       385,
                                       CompressionKind_ZLIB,
                                       262144,
                                       10000),
                    OrcFileDescription("demo-12-zlib",
                                       ("struct<_col0:int,_col1:string,"
                                        "_col2:string,_col3:string,_col4:int,"
                                        "_col5:string,_col6:int,_col7:int,"
                                        "_col8:int>"),
                                       1920800,
                                       45592,
                                       1,
                                       CompressionKind_ZLIB,
                                       262144,
                                       10000),
                    OrcFileDescription("nulls-at-end-snappy",
                                       ("struct<_col0:tinyint,_col1:smallint,"
                                        "_col2:int,_col3:bigint,_col4:float,"
                                        "_col5:double,_col6:boolean>"),
                                       70000,
                                       366347,
                                       1,
                                       CompressionKind_SNAPPY,
                                       262144,
                                       10000),
                    OrcFileDescription("orc-file-11-format",
                                       ("struct<boolean1:boolean,"
                                        "byte1:tinyint,short1:smallint,"
                                        "int1:int,long1:bigint,float1:float,"
                                        "double1:double,bytes1:binary,"
                                        "string1:string,middle:struct<list:"
                                        "array<struct<int1:int,"
                                        "string1:string>>>,list:array<struct"
                                        "<int1:int,string1:string>>,map:map"
                                        "<string,struct<int1:int,string1:"
                                        "string>>,ts:timestamp,"
                                        "decimal1:decimal(0,0)>"),
                                       7500,
                                       372542,
                                       2,
                                       CompressionKind_NONE,
                                       262144,
                                       10000),
                    OrcFileDescription("orc_split_elim",
                                       ("struct<userid:bigint,string1:string,"
                                        "subtype:double,decimal1:decimal(0,0),"
                                        "ts:timestamp>"),
                                       25000,
                                       245568,
                                       5,
                                       CompressionKind_NONE,
                                       262144,
                                       10000),
                    OrcFileDescription("decimal",
                                       "struct<_col0:decimal(10,5)>",
                                       6000,
                                       16186,
                                       1,
                                       CompressionKind_NONE,
                                       262144,
                                       10000)
                    ));

  TEST(Reader, columnSelectionTest) {
    ReaderOptions opts;
    std::list<int> includes = {1,3,5,7,9};
    opts.include(includes);
    std::ostringstream filename;
    filename << exampleDirectory << "/demo-11-none.orc";
    std::unique_ptr<Reader> reader =
      createReader(readLocalFile(filename.str()), opts);

    EXPECT_EQ(CompressionKind_NONE, reader->getCompression());
    EXPECT_EQ(256 * 1024, reader->getCompressionSize());
    EXPECT_EQ(385, reader->getNumberOfStripes());
    EXPECT_EQ(1920800, reader->getNumberOfRows());
    EXPECT_EQ(10000, reader->getRowIndexStride());
    EXPECT_EQ(5069718, reader->getContentLength());
    EXPECT_EQ(filename.str(), reader->getStreamName());
    EXPECT_THAT(reader->getMetadataKeys(), testing::IsEmpty());
    EXPECT_FALSE(reader->hasMetadataValue("foo"));
    EXPECT_EQ(18446744073709551615UL, reader->getRowNumber());

    const Type& rootType = reader->getType();
    EXPECT_EQ(0, rootType.getColumnId());
    EXPECT_EQ(STRUCT, rootType.getKind());
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
    EXPECT_EQ(INT, rootType.getSubtype(0).getKind());
    EXPECT_EQ(STRING, rootType.getSubtype(1).getKind());
    EXPECT_EQ(STRING, rootType.getSubtype(2).getKind());
    EXPECT_EQ(STRING, rootType.getSubtype(3).getKind());
    EXPECT_EQ(INT, rootType.getSubtype(4).getKind());
    EXPECT_EQ(STRING, rootType.getSubtype(5).getKind());
    EXPECT_EQ(INT, rootType.getSubtype(6).getKind());
    EXPECT_EQ(INT, rootType.getSubtype(7).getKind());
    EXPECT_EQ(INT, rootType.getSubtype(8).getKind());
    for(unsigned int i=0; i < 9; ++i) {
      EXPECT_EQ(i + 1, rootType.getSubtype(i).getColumnId())
        << "fail on " << i;
    }

    const std::vector<bool> selected = reader->getSelectedColumns();
    EXPECT_EQ(true, selected[0]) << "fail on " << 0;
    for (size_t i = 1; i < 10; ++i) {
      EXPECT_EQ(i%2==1?true:false, selected[i]) << "fail on " << i;
    }

    unsigned long rowCount = 0;
    std::unique_ptr<ColumnVectorBatch> batch = reader->createRowBatch(1024);
    LongVectorBatch* longVector =
      dynamic_cast<LongVectorBatch*>
      (dynamic_cast<StructVectorBatch&>(*batch).fields[0]);
    int64_t* idCol = longVector->data.data();
    while (reader->next(*batch)) {
      EXPECT_EQ(rowCount, reader->getRowNumber());
      for(unsigned int i=0; i < batch->numElements; ++i) {
        EXPECT_EQ(rowCount + i + 1, idCol[i]) << "Bad id for " << i;
      }
      rowCount += batch->numElements;
    }
    EXPECT_EQ(1920800, rowCount);
    EXPECT_EQ(1920800, reader->getRowNumber());
  }

  TEST(Reader, stripeInformationTest) {
    ReaderOptions opts;
    std::ostringstream filename;
    filename << exampleDirectory << "/demo-11-none.orc";
    std::unique_ptr<Reader> reader =
      createReader(readLocalFile(filename.str()), opts);

    EXPECT_EQ(385, reader->getNumberOfStripes());

    std::unique_ptr<StripeInformation> stripeInfo = reader->getStripe(7);
    EXPECT_EQ(92143, stripeInfo->getOffset());
    EXPECT_EQ(13176, stripeInfo->getLength());
    EXPECT_EQ(234, stripeInfo->getIndexLength());
    EXPECT_EQ(12673, stripeInfo->getDataLength());
    EXPECT_EQ(269, stripeInfo->getFooterLength());
    EXPECT_EQ(5000, stripeInfo->getNumberOfRows());
  }

  TEST(Reader, readRangeTest) {
    ReaderOptions fullOpts, lastOpts, oobOpts, offsetOpts;
    // stripes[N-1]
    lastOpts.range(5067085, 1);
    // stripes[N]
    oobOpts.range(5067086, 4096);
    // stripes[7, 16]
    offsetOpts.range(80000, 130722);
    std::ostringstream filename;
    filename << exampleDirectory << "/demo-11-none.orc";
    std::unique_ptr<Reader> fullReader =
      createReader(readLocalFile(filename.str()), fullOpts);
    std::unique_ptr<Reader> lastReader =
      createReader(readLocalFile(filename.str()), lastOpts);
    std::unique_ptr<Reader> oobReader =
      createReader(readLocalFile(filename.str()), oobOpts);
    std::unique_ptr<Reader> offsetReader =
      createReader(readLocalFile(filename.str()), offsetOpts);

    std::unique_ptr<ColumnVectorBatch> oobBatch =
      oobReader->createRowBatch(5000);
    EXPECT_FALSE(oobReader->next(*oobBatch));

    // advance fullReader to align with offsetReader
    std::unique_ptr<ColumnVectorBatch> fullBatch =
      fullReader->createRowBatch(5000);
    for (int i=0; i < 7; ++i) {
      EXPECT_TRUE(fullReader->next(*fullBatch));
      EXPECT_EQ(5000, fullBatch->numElements);
    }

    std::unique_ptr<ColumnVectorBatch> offsetBatch =
      offsetReader->createRowBatch(5000);
    LongVectorBatch* fullLongVector =
      dynamic_cast<LongVectorBatch*>
      (dynamic_cast<StructVectorBatch&>(*fullBatch).fields[0]);
    int64_t* fullId = fullLongVector->data.data();
    LongVectorBatch* offsetLongVector =
      dynamic_cast<LongVectorBatch*>
      (dynamic_cast<StructVectorBatch&>(*offsetBatch).fields[0]);
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

    std::unique_ptr<ColumnVectorBatch> lastBatch =
      lastReader->createRowBatch(5000);
    LongVectorBatch* lastLongVector =
      dynamic_cast<LongVectorBatch*>
      (dynamic_cast<StructVectorBatch&>(*lastBatch).fields[0]);
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
  std::unique_ptr<orc::Statistics> stats = reader->getStatistics();
  EXPECT_EQ(10, stats->getNumberOfColumns());

  // column[5]
  std::unique_ptr<orc::ColumnStatistics> col_5 =
    reader->getColumnStatistics(6);
  const orc::StringColumnStatistics& strStats =
    dynamic_cast<const orc::StringColumnStatistics&> (*(col_5.get()));
  EXPECT_EQ("Good", strStats.getMinimum());
  EXPECT_EQ("Unknown", strStats.getMaximum());

  // column[6]
  std::unique_ptr<orc::ColumnStatistics> col_6 =
    reader->getColumnStatistics(7);
  const orc::IntegerColumnStatistics& intStats =
    dynamic_cast<const orc::IntegerColumnStatistics&> (*(col_6.get()));
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
  std::unique_ptr<orc::Statistics> stripeStats =
    reader->getStripeStatistics(stripeIdx);
  EXPECT_EQ(10, stripeStats->getNumberOfColumns());

  // column[5]
  const StringColumnStatistics* col_5 =
    dynamic_cast<const StringColumnStatistics*>
    (stripeStats->getColumnStatistics(6));
  EXPECT_EQ("Good", col_5->getMinimum());
  EXPECT_EQ("Unknown", col_5->getMaximum());

  // column[6]
  const IntegerColumnStatistics* col_6 =
    dynamic_cast<const IntegerColumnStatistics*>
    (stripeStats->getColumnStatistics(7));
  EXPECT_EQ(4, col_6->getMinimum());
  EXPECT_EQ(5, col_6->getMaximum());
  EXPECT_EQ(22600, col_6->getSum());
}

}  // namespace
