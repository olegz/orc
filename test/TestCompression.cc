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


#include "Compression.hh"
#include "Exceptions.hh"
#include "wrap/gtest-wrapper.h"

#include <cstdio>
#include <fstream>
#include <iostream>
#include <sstream>

namespace orc {

  class TestCompression : public ::testing::Test {
  public:
    ~TestCompression();
  protected:
    // Per-test-case set-up.
    static void SetUpTestCase() {
      simpleFile = "simple-file.binary";
      remove(simpleFile);
      std::ofstream file;
      file.exceptions(std::ofstream::failbit | std::ofstream::badbit);
      file.open(simpleFile,
                std::ios::out | std::ios::binary | std::ios::trunc);
      for(unsigned int i = 0; i < 200; ++i) {
        file.put(static_cast<char>(i));
      }
      file.close();
    }

    // Per-test-case tear-down.
    static void TearDownTestCase() {
      simpleFile = 0;
    }

    static const char *simpleFile;
  };

  const char *TestCompression::simpleFile;

  TestCompression::~TestCompression() {
    // PASS
  }

  TEST_F(TestCompression, testPrintBufferEmpty) {
    std::ostringstream str;
    printBuffer(str, 0, 0);
    EXPECT_EQ("", str.str());
  }

  TEST_F(TestCompression, testPrintBufferSmall) {
    std::vector<char> buffer(10);
    std::ostringstream str;
    for(size_t i=0; i < 10; ++i) {
      buffer[i] = static_cast<char>(i);
    }
    printBuffer(str, buffer.data(), 10);
    EXPECT_EQ("0000000 00 01 02 03 04 05 06 07 08 09\n", str.str());
  }

  TEST_F(TestCompression, testPrintBufferLong) {
    std::vector<char> buffer(300);
    std::ostringstream str;
    for(size_t i=0; i < 300; ++i) {
      buffer[i] = static_cast<char>(i);
    }
    printBuffer(str, buffer.data(), 300);
    std::ostringstream expected;
    expected << "0000000 00 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f 10"
             << " 11 12 13 14 15 16 17\n"
             << "0000018 18 19 1a 1b 1c 1d 1e 1f 20 21 22 23 24 25 26 27 28"
             << " 29 2a 2b 2c 2d 2e 2f\n"
             << "0000030 30 31 32 33 34 35 36 37 38 39 3a 3b 3c 3d 3e 3f 40"
             << " 41 42 43 44 45 46 47\n"
             << "0000048 48 49 4a 4b 4c 4d 4e 4f 50 51 52 53 54 55 56 57 58"
             << " 59 5a 5b 5c 5d 5e 5f\n"
             << "0000060 60 61 62 63 64 65 66 67 68 69 6a 6b 6c 6d 6e 6f 70"
             << " 71 72 73 74 75 76 77\n"
             << "0000078 78 79 7a 7b 7c 7d 7e 7f 80 81 82 83 84 85 86 87 88"
             << " 89 8a 8b 8c 8d 8e 8f\n"
             << "0000090 90 91 92 93 94 95 96 97 98 99 9a 9b 9c 9d 9e 9f a0"
             << " a1 a2 a3 a4 a5 a6 a7\n"
             << "00000a8 a8 a9 aa ab ac ad ae af b0 b1 b2 b3 b4 b5 b6 b7 b8"
             << " b9 ba bb bc bd be bf\n"
             << "00000c0 c0 c1 c2 c3 c4 c5 c6 c7 c8 c9 ca cb cc cd ce cf d0"
             << " d1 d2 d3 d4 d5 d6 d7\n"
             << "00000d8 d8 d9 da db dc dd de df e0 e1 e2 e3 e4 e5 e6 e7 e8"
             << " e9 ea eb ec ed ee ef\n"
             << "00000f0 f0 f1 f2 f3 f4 f5 f6 f7 f8 f9 fa fb fc fd fe ff 00"
             << " 01 02 03 04 05 06 07\n"
             << "0000108 08 09 0a 0b 0c 0d 0e 0f 10 11 12 13 14 15 16 17 18"
             << " 19 1a 1b 1c 1d 1e 1f\n"
             << "0000120 20 21 22 23 24 25 26 27 28 29 2a 2b\n";
    EXPECT_EQ(expected.str(), str.str());
  }

  TEST_F(TestCompression, testArrayBackup) {
    std::vector<char> bytes(200);
    for(size_t i=0; i < bytes.size(); ++i) {
      bytes[i] = static_cast<char>(i);
    }
    SeekableArrayInputStream stream(bytes.data(), bytes.size(), 20);
    const void *ptr;
    int len;
    ASSERT_THROW(stream.BackUp(10), std::logic_error);
    EXPECT_EQ(true, stream.Next(&ptr, &len));
    EXPECT_EQ(bytes.data(), static_cast<const char *>(ptr));
    EXPECT_EQ(20, len);
    stream.BackUp(0);
    EXPECT_EQ(true, stream.Next(&ptr, &len));
    EXPECT_EQ(bytes.data() + 20, static_cast<const char *>(ptr));
    EXPECT_EQ(20, len);
    stream.BackUp(10);
    for(unsigned int i=0; i < 8; ++i) {
      EXPECT_EQ(true, stream.Next(&ptr, &len));
      unsigned int consumedBytes = 30 + 20 * i;
      EXPECT_EQ(bytes.data() + consumedBytes, static_cast<const char *>(ptr));
      EXPECT_EQ(consumedBytes + 20, stream.ByteCount());
      EXPECT_EQ(20, len);
    }
    EXPECT_EQ(true, stream.Next(&ptr, &len));
    EXPECT_EQ(bytes.data() + 190, static_cast<const char *>(ptr));
    EXPECT_EQ(10, len);
    EXPECT_EQ(false, stream.Next(&ptr, &len));
    EXPECT_EQ(0, len);
    ASSERT_THROW(stream.BackUp(30), std::logic_error);
    EXPECT_EQ(200, stream.ByteCount());
  }

  TEST_F(TestCompression, testArraySkip) {
    std::vector<char> bytes(200);
    for(size_t i=0; i < bytes.size(); ++i) {
      bytes[i] = static_cast<char>(i);
    }
    SeekableArrayInputStream stream(bytes.data(), bytes.size(), 20);
    const void *ptr;
    int len;
    ASSERT_EQ(true, stream.Next(&ptr, &len));
    EXPECT_EQ(bytes.data(), static_cast<const char *>(ptr));
    EXPECT_EQ(20, len);
    ASSERT_EQ(false, stream.Skip(-10));
    ASSERT_EQ(true, stream.Skip(80));
    ASSERT_EQ(true, stream.Next(&ptr, &len));
    EXPECT_EQ(bytes.data() + 100, static_cast<const char *>(ptr));
    EXPECT_EQ(20, len);
    ASSERT_EQ(true, stream.Skip(80));
    ASSERT_EQ(false, stream.Next(&ptr, &len));
    ASSERT_EQ(false, stream.Skip(181));
    std::ostringstream result;
    result << "memory from " << std::hex << bytes.data() << " for 200";
    EXPECT_EQ(result.str(), stream.getName());
  }

  TEST_F(TestCompression, testArrayCombo) {
    std::vector<char> bytes(200);
    for(size_t i=0; i < bytes.size(); ++i) {
      bytes[i] = static_cast<char>(i);
    }
    SeekableArrayInputStream stream(bytes.data(), bytes.size(), 20);
    const void *ptr;
    int len;
    ASSERT_EQ(true, stream.Next(&ptr, &len));
    EXPECT_EQ(bytes.data(), static_cast<const char *>(ptr));
    EXPECT_EQ(20, len);
    stream.BackUp(10);
    EXPECT_EQ(10, stream.ByteCount());
    stream.Skip(4);
    EXPECT_EQ(14, stream.ByteCount());
    ASSERT_EQ(true, stream.Next(&ptr, &len));
    EXPECT_EQ(bytes.data() + 14, static_cast<const char *>(ptr));
    EXPECT_EQ(false, stream.Skip(320));
    EXPECT_EQ(200, stream.ByteCount());
    EXPECT_EQ(false, stream.Next(&ptr, &len));
  }

  // this checks to make sure that a given set of bytes are ascending
  void checkBytes(const char*data, int length,
                  unsigned int startValue) {
    for(unsigned int i=0; static_cast<int>(i) < length; ++i) {
      EXPECT_EQ(startValue + i, static_cast<unsigned char>(data[i])) 
        << "Output wrong at " << startValue << " + " << i;
    }
  }

  TEST_F(TestCompression, testFileBackup) {
    SCOPED_TRACE("testFileBackup");
    std::unique_ptr<InputStream> file = readLocalFile(simpleFile);
    SeekableFileInputStream stream(file.get(), 0, 200, 20);
    const void *ptr;
    int len;
    ASSERT_THROW(stream.BackUp(10), std::logic_error);
    EXPECT_EQ(true, stream.Next(&ptr, &len));
    EXPECT_EQ(20, len);
    checkBytes(static_cast<const char*>(ptr), len, 0);
    stream.BackUp(0);
    EXPECT_EQ(true, stream.Next(&ptr, &len));
    EXPECT_EQ(20, len);
    checkBytes(static_cast<const char*>(ptr), len, 20);
    stream.BackUp(10);
    for(unsigned int i=0; i < 8; ++i) {
      EXPECT_EQ(true, stream.Next(&ptr, &len));
      unsigned int consumedBytes = 30 + 20 * i;
      EXPECT_EQ(consumedBytes + 20, stream.ByteCount());
      EXPECT_EQ(20, len);
      checkBytes(static_cast<const char*>(ptr), len, consumedBytes);
    }
    EXPECT_EQ(true, stream.Next(&ptr, &len));
    EXPECT_EQ(10, len);
    checkBytes(static_cast<const char*>(ptr), len, 190);
    EXPECT_EQ(false, stream.Next(&ptr, &len));
    EXPECT_EQ(0, len);
    ASSERT_THROW(stream.BackUp(30), std::logic_error);
    EXPECT_EQ(200, stream.ByteCount());
  }

  TEST_F(TestCompression, testFileSkip) {
    SCOPED_TRACE("testFileSkip");
    std::unique_ptr<InputStream> file = readLocalFile(simpleFile);
    SeekableFileInputStream stream(file.get(), 0, 200, 20);
    const void *ptr;
    int len;
    ASSERT_EQ(true, stream.Next(&ptr, &len));
    checkBytes(static_cast<const char*>(ptr), len, 0);
    EXPECT_EQ(20, len);
    ASSERT_EQ(false, stream.Skip(-10));
    ASSERT_EQ(true, stream.Skip(80));
    ASSERT_EQ(true, stream.Next(&ptr, &len));
    checkBytes(static_cast<const char*>(ptr), len, 100);
    EXPECT_EQ(20, len);
    ASSERT_EQ(true, stream.Skip(80));
    ASSERT_EQ(false, stream.Next(&ptr, &len));
    ASSERT_EQ(false, stream.Skip(181));
    EXPECT_EQ("simple-file.binary from 0 for 200", stream.getName());
  }

  TEST_F(TestCompression, testFileCombo) {
    SCOPED_TRACE("testFileCombo");
    std::unique_ptr<InputStream> file = readLocalFile(simpleFile);
    SeekableFileInputStream stream(file.get(), 0, 200, 20);
    const void *ptr;
    int len;
    ASSERT_EQ(true, stream.Next(&ptr, &len));
    checkBytes(static_cast<const char*>(ptr), len, 0);
    EXPECT_EQ(20, len);
    stream.BackUp(10);
    EXPECT_EQ(10, stream.ByteCount());
    stream.Skip(4);
    EXPECT_EQ(14, stream.ByteCount());
    ASSERT_EQ(true, stream.Next(&ptr, &len));
    checkBytes(static_cast<const char*>(ptr), len, 14);
    EXPECT_EQ(false, stream.Skip(320));
    EXPECT_EQ(200, stream.ByteCount());
    EXPECT_EQ(false, stream.Next(&ptr, &len));
  }

  TEST_F(TestCompression, testFileSeek) {
    SCOPED_TRACE("testFileSeek");
    std::unique_ptr<InputStream> file = readLocalFile(simpleFile);
    SeekableFileInputStream stream(file.get(), 0, 200, 20);
    const void *ptr;
    int len;
    EXPECT_EQ(0, stream.ByteCount());
    ASSERT_EQ(true, stream.Next(&ptr, &len));
    checkBytes(static_cast<const char*>(ptr), len, 0);
    EXPECT_EQ(20, len);
    EXPECT_EQ(20, stream.ByteCount());
    {
      std::list<unsigned long> offsets({100});
      PositionProvider posn(offsets);
      stream.seek(posn);
    }
    EXPECT_EQ(100, stream.ByteCount());
    {
      std::list<unsigned long> offsets({5});
      PositionProvider posn(offsets);
      stream.seek(posn);
    }
    EXPECT_EQ(5, stream.ByteCount());
    ASSERT_EQ(true, stream.Next(&ptr, &len));
    checkBytes(static_cast<const char*>(ptr), len, 5);
    EXPECT_EQ(20, len);
    {
      std::list<unsigned long> offsets({201});
      PositionProvider posn(offsets);
      EXPECT_THROW(stream.seek(posn), std::logic_error);
      EXPECT_EQ(200, stream.ByteCount());
    }
  }

  TEST_F(TestCompression, testCreateCodec) {
    std::vector<char> bytes(10);
    for(unsigned int i=0; i < bytes.size(); ++i) {
      bytes[i] = static_cast<char>(i);
    }
    std::unique_ptr<SeekableInputStream> result =
      createCodec(CompressionKind_NONE,
                  std::unique_ptr<SeekableInputStream>
                    (new SeekableArrayInputStream(bytes.data(), bytes.size())),
                  32768);
    const void *ptr;
    int length;
    result->Next(&ptr, &length);
    for(unsigned int i=0; i < bytes.size(); ++i) {
      EXPECT_EQ(static_cast<char>(i), static_cast<const char*>(ptr)[i]);
    }
    EXPECT_THROW(createCodec(CompressionKind_ZLIB,
                             std::unique_ptr<SeekableInputStream>
                             (new SeekableArrayInputStream(bytes.data(),
                                                           bytes.size())),
                             32768), NotImplementedYet);
    EXPECT_THROW(createCodec(CompressionKind_SNAPPY,
                             std::unique_ptr<SeekableInputStream>
                             (new SeekableArrayInputStream(bytes.data(),
                                                           bytes.size())),
                             32768), NotImplementedYet);
    EXPECT_THROW(createCodec(CompressionKind_LZO,
                             std::unique_ptr<SeekableInputStream>
                             (new SeekableArrayInputStream(bytes.data(),
                                                           bytes.size())),
                             32768), NotImplementedYet);
  }
}
