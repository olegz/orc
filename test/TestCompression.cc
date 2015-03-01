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
#include "TestDriver.hh"

#include <cstdio>
#include <fstream>
#include <iostream>
#include <sstream>

#include "zlib.h"
#include "snappy.h"

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
    EXPECT_EQ("SeekableArrayInputStream 200 of 200", stream.getName());
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
    EXPECT_EQ(30, stream.ByteCount());
    EXPECT_EQ(true, stream.Next(&ptr, &len));
    EXPECT_EQ(10, len);
    checkBytes(static_cast<const char*>(ptr), len, 30);
    for(unsigned int i=0; i < 8; ++i) {
      EXPECT_EQ(20 * i + 40, stream.ByteCount());
      EXPECT_EQ(true, stream.Next(&ptr, &len));
      EXPECT_EQ(20, len);
      checkBytes(static_cast<const char*>(ptr), len, 20 * i + 40);
    }
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
    ASSERT_EQ(false, stream.Skip(80));
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
      std::list<unsigned long> offsets {100};
      PositionProvider posn(offsets);
      stream.seek(posn);
    }
    EXPECT_EQ(100, stream.ByteCount());
    {
      std::list<unsigned long> offsets {5};
      PositionProvider posn(offsets);
      stream.seek(posn);
    }
    EXPECT_EQ(5, stream.ByteCount());
    ASSERT_EQ(true, stream.Next(&ptr, &len));
    checkBytes(static_cast<const char*>(ptr), len, 5);
    EXPECT_EQ(20, len);
    {
      std::list<unsigned long> offsets {201};
      PositionProvider posn(offsets);
      EXPECT_THROW(stream.seek(posn), std::logic_error);
      EXPECT_EQ(200, stream.ByteCount());
    }
  }

  TEST_F(TestCompression, testCreateNone) {
    std::vector<char> bytes(10);
    for(unsigned int i=0; i < bytes.size(); ++i) {
      bytes[i] = static_cast<char>(i);
    }
    std::unique_ptr<SeekableInputStream> result =
      createDecompressor(CompressionKind_NONE,
                  std::unique_ptr<SeekableInputStream>
                    (new SeekableArrayInputStream(bytes.data(), bytes.size())),
                  32768);
    const void *ptr;
    int length;
    result->Next(&ptr, &length);
    for(unsigned int i=0; i < bytes.size(); ++i) {
      EXPECT_EQ(static_cast<char>(i), static_cast<const char*>(ptr)[i]);
    }
  }

  TEST_F(TestCompression, testCreateLzo) {
    EXPECT_THROW(createDecompressor(CompressionKind_LZO,
                             std::unique_ptr<SeekableInputStream>
                                    (new SeekableArrayInputStream( { } )),
                             32768), NotImplementedYet);
  }

  TEST(Zlib, testCreateZlib) {
    std::unique_ptr<SeekableInputStream> result =
      createDecompressor(CompressionKind_ZLIB,
                         std::unique_ptr<SeekableInputStream>
                         (new SeekableArrayInputStream
                          ({0x0b, 0x0, 0x0, 0x0, 0x1, 0x2, 0x3, 0x4})),
                  32768);
    EXPECT_EQ("zlib(SeekableArrayInputStream 0 of 8)", result->getName());
    const void *ptr;
    int length;
    ASSERT_EQ(true, result->Next(&ptr, &length));
    ASSERT_EQ(5, length);
    for(unsigned int i=0; i < 5; ++i) {
      EXPECT_EQ(static_cast<char>(i), static_cast<const char*>(ptr)[i]);
    }
    EXPECT_EQ("zlib(SeekableArrayInputStream 8 of 8)", result->getName());
    EXPECT_EQ(5, result->ByteCount());
    result->BackUp(3);
    EXPECT_EQ(2, result->ByteCount());
    ASSERT_EQ(true, result->Next(&ptr, &length));
    ASSERT_EQ(3, length);
    for(unsigned int i=0; i < 3; ++i) {
      EXPECT_EQ(static_cast<char>(i+2), static_cast<const char*>(ptr)[i]);
    }
  }

  TEST(Zlib, testLiteralBlocks) {
    std::unique_ptr<SeekableInputStream> result =
      createDecompressor(CompressionKind_ZLIB,
                         std::unique_ptr<SeekableInputStream>
                         (new SeekableArrayInputStream
                          ({0x19, 0x0, 0x0, 0x0, 0x1,
                              0x2, 0x3, 0x4, 0x5, 0x6,
                              0x7, 0x8, 0x9, 0xa, 0xb,
                              0xb, 0x0, 0x0, 0xc, 0xd, 
                              0xe, 0xf, 0x10}, 5)),
                         5);
    EXPECT_EQ("zlib(SeekableArrayInputStream 0 of 23)", result->getName());
    const void *ptr;
    int length;
    ASSERT_EQ(true, result->Next(&ptr, &length));
    ASSERT_EQ(2, length);
    EXPECT_EQ(0, static_cast<const char*>(ptr)[0]);
    EXPECT_EQ(1, static_cast<const char*>(ptr)[1]);
    ASSERT_EQ(true, result->Next(&ptr, &length));
    ASSERT_EQ(5, length);
    EXPECT_EQ(2, static_cast<const char*>(ptr)[0]);
    EXPECT_EQ(3, static_cast<const char*>(ptr)[1]);
    EXPECT_EQ(4, static_cast<const char*>(ptr)[2]);
    EXPECT_EQ(5, static_cast<const char*>(ptr)[3]);
    EXPECT_EQ(6, static_cast<const char*>(ptr)[4]);
    ASSERT_EQ(true, result->Next(&ptr, &length));
    ASSERT_EQ(5, length);
    EXPECT_EQ(7, static_cast<const char*>(ptr)[0]);
    EXPECT_EQ(8, static_cast<const char*>(ptr)[1]);
    EXPECT_EQ(9, static_cast<const char*>(ptr)[2]);
    EXPECT_EQ(10, static_cast<const char*>(ptr)[3]);
    EXPECT_EQ(11, static_cast<const char*>(ptr)[4]);
    ASSERT_EQ(true, result->Next(&ptr, &length));
    ASSERT_EQ(2, length);
    EXPECT_EQ(12, static_cast<const char*>(ptr)[0]);
    EXPECT_EQ(13, static_cast<const char*>(ptr)[1]);
    ASSERT_EQ(true, result->Next(&ptr, &length));
    ASSERT_EQ(3, length);
    EXPECT_EQ(14, static_cast<const char*>(ptr)[0]);
    EXPECT_EQ(15, static_cast<const char*>(ptr)[1]);
    EXPECT_EQ(16, static_cast<const char*>(ptr)[2]);
  }

  TEST(Zlib, testInflate) {
    std::unique_ptr<SeekableInputStream> result =
      createDecompressor(CompressionKind_ZLIB,
                         std::unique_ptr<SeekableInputStream>
                         (new SeekableArrayInputStream
                          ({0xe, 0x0, 0x0, 0x63, 0x60, 0x64, 0x62, 0xc0,
                              0x8d, 0x0})), 1000);
    const void *ptr;
    int length;
    ASSERT_EQ(true, result->Next(&ptr, &length));
    ASSERT_EQ(30, length);
    for(int i=0; i < 10; ++i) {
      for(int j=0; j < 3; ++j) {
        EXPECT_EQ(j, static_cast<const char*>(ptr)[i * 3 + j]);
      }
    }
  }

  TEST(Zlib, testInflateSequence) {
    std::unique_ptr<SeekableInputStream> result =
      createDecompressor(CompressionKind_ZLIB,
                         std::unique_ptr<SeekableInputStream>
                         (new SeekableArrayInputStream
                          ({0xe, 0x0, 0x0, 0x63, 0x60,
                              0x64, 0x62, 0xc0, 0x8d, 0x0,
                              0xe, 0x0, 0x0, 0x63, 0x60,
                              0x64, 0x62, 0xc0, 0x8d, 0x0}, 3)), 1000);
    const void *ptr;
    int length;
    ASSERT_THROW(result->BackUp(20), std::logic_error);
    ASSERT_EQ(true, result->Next(&ptr, &length));
    ASSERT_EQ(30, length);
    for(int i=0; i < 10; ++i) {
      for(int j=0; j < 3; ++j) {
        EXPECT_EQ(j, static_cast<const char*>(ptr)[i * 3 + j]);
      }
    }
    result->BackUp(10);
    ASSERT_THROW(result->BackUp(2), std::logic_error);
    ASSERT_EQ(true, result->Next(&ptr, &length));
    ASSERT_EQ(10, length);
    for(int i=0; i < 10; ++i) {
      EXPECT_EQ((i + 2) % 3, static_cast<const char*>(ptr)[i]);
    }
    ASSERT_EQ(true, result->Next(&ptr, &length));
    ASSERT_EQ(30, length);
    for(int i=0; i < 10; ++i) {
      for(int j=0; j < 3; ++j) {
        EXPECT_EQ(j, static_cast<const char*>(ptr)[i * 3 + j]);
      }
    }
  }

  TEST(Zlib, testSkip) {
    std::unique_ptr<SeekableInputStream> result =
      createDecompressor(CompressionKind_ZLIB,
                         std::unique_ptr<SeekableInputStream>
                         (new SeekableArrayInputStream
                          ({0x19, 0x0, 0x0, 0x0, 0x1,
                              0x2, 0x3, 0x4, 0x5, 0x6,
                              0x7, 0x8, 0x9, 0xa, 0xb,
                              0xb, 0x0, 0x0, 0xc, 0xd, 
                              0xe, 0xf, 0x10}, 5)),
                         5);
    const void *ptr;
    int length;
    ASSERT_EQ(true, result->Next(&ptr, &length));
    ASSERT_EQ(2, length);
    result->Skip(2);
    ASSERT_EQ(true, result->Next(&ptr, &length));
    ASSERT_EQ(3, length);
    EXPECT_EQ(4, static_cast<const char*>(ptr)[0]);
    EXPECT_EQ(5, static_cast<const char*>(ptr)[1]);
    EXPECT_EQ(6, static_cast<const char*>(ptr)[2]);
    result->BackUp(2);
    ASSERT_EQ(true, result->Next(&ptr, &length));
    ASSERT_EQ(2, length);
    EXPECT_EQ(5, static_cast<const char*>(ptr)[0]);
    EXPECT_EQ(6, static_cast<const char*>(ptr)[1]);
    result->Skip(8);
    ASSERT_EQ(true, result->Next(&ptr, &length));
    ASSERT_EQ(2, length);
    EXPECT_EQ(15, static_cast<const char*>(ptr)[0]);
    EXPECT_EQ(16, static_cast<const char*>(ptr)[1]);
  }

#define HEADER_SIZE 3

  class CompressBuffer {
      std::vector<char> buf;

  public:
    CompressBuffer(size_t capacity) :
      buf(capacity + HEADER_SIZE)
    {}

    char *getCompressed() {
      return buf.data() + HEADER_SIZE;
    }
    char *getBuffer() {
      return buf.data();
    }

    void writeHeader(size_t compressedSize) {
      buf[0] = static_cast<char>(compressedSize << 1);
      buf[1] = static_cast<char>(compressedSize >> 7);
      buf[2] = static_cast<char>(compressedSize >> 15);
    }

    size_t getCompressedSize() const {
      size_t header = static_cast<unsigned char>(buf[0]);
      header |= static_cast<size_t>(static_cast<unsigned char>(buf[1])) << 8;
      header |= static_cast<size_t>(static_cast<unsigned char>(buf[2])) << 16;
      return header >> 1;
    }

    size_t getBufferSize() const {
      return getCompressedSize() + HEADER_SIZE;
    }
  };

  TEST(Snappy, testBasic) {
    const int N = 1024;
    std::vector<char> buf(N * sizeof(int));
    for (int i=0; i < N; ++i) {
      (reinterpret_cast<int *>(buf.data()))[i] = i % 8;
    }

    CompressBuffer compressBuffer(snappy::MaxCompressedLength(buf.size()));
    size_t compressedSize;
    snappy::RawCompress(buf.data(), buf.size(), compressBuffer.getCompressed(),
                        &compressedSize);
    // compressed size must be < original
    ASSERT_LT(compressedSize, buf.size());
    compressBuffer.writeHeader(compressedSize);

    const long blockSize = 3;
    std::unique_ptr<SeekableInputStream> result = createDecompressor
        (CompressionKind_SNAPPY,
         std::unique_ptr<SeekableInputStream>
           (new SeekableArrayInputStream(compressBuffer.getBuffer(),
                                         compressBuffer.getBufferSize(),
                                         blockSize)),
         buf.size());
    const void *data;
    int length;
    ASSERT_TRUE(result->Next(&data, &length));
    ASSERT_EQ(N * sizeof(int), length);
    for (int i=0; i < N; ++i) {
      EXPECT_EQ(i % 8, (reinterpret_cast<const int *>(data))[i]);
    }
  }

  TEST(Snappy, testMultiBuffer) {
    const int N = 1024;
    std::vector<char> buf(N * sizeof(int));
    for (int i=0; i < N; ++i) {
      (reinterpret_cast<int *>(buf.data()))[i] = i % 8;
    }

    CompressBuffer compressBuffer(snappy::MaxCompressedLength(buf.size()));
    size_t compressedSize;
    snappy::RawCompress(buf.data(), buf.size(), compressBuffer.getCompressed(),
                        &compressedSize);
    // compressed size must be < original
    ASSERT_LT(compressedSize, buf.size());
    compressBuffer.writeHeader(compressedSize);

    std::vector<char> input(compressBuffer.getBufferSize() * 4);
    ::memcpy(input.data(), compressBuffer.getBuffer(),
             compressBuffer.getBufferSize());
    ::memcpy(input.data() + compressBuffer.getBufferSize(),
             compressBuffer.getBuffer(), compressBuffer.getBufferSize());
    ::memcpy(input.data() + 2 * compressBuffer.getBufferSize(),
             compressBuffer.getBuffer(), compressBuffer.getBufferSize());
    ::memcpy(input.data() + 3 * compressBuffer.getBufferSize(),
             compressBuffer.getBuffer(), compressBuffer.getBufferSize());

    const long blockSize = 3;
    std::unique_ptr<SeekableInputStream> result = createDecompressor
        (CompressionKind_SNAPPY,
         std::unique_ptr<SeekableInputStream>
         (new SeekableArrayInputStream(input.data(), input.size(), blockSize)),
           buf.size());
    for (int i=0; i < 4; ++i) {
      const void *data;
      int length;
      ASSERT_TRUE(result->Next(&data, &length));
      for (int j=0; j < N; ++j) {
          EXPECT_EQ(j % 8, (reinterpret_cast<const int *>(data))[j]);
      }
    }
  }

  TEST(Snappy, testSkip) {
    const int N = 1024;
    std::vector<char> buf(N * sizeof(int));
    for (int i=0; i < N; ++i) {
      (reinterpret_cast<int *>(buf.data()))[i] = i % 8;
    }

    CompressBuffer compressBuffer(snappy::MaxCompressedLength(buf.size()));
    size_t compressedSize;
    snappy::RawCompress(buf.data(), buf.size(), compressBuffer.getCompressed(),
                        &compressedSize);
    // compressed size must be < original
    ASSERT_LT(compressedSize, buf.size());
    compressBuffer.writeHeader(compressedSize);

    const long blockSize = 3;
    std::unique_ptr<SeekableInputStream> result = createDecompressor
        (CompressionKind_SNAPPY,
         std::unique_ptr<SeekableInputStream>
           (new SeekableArrayInputStream(compressBuffer.getBuffer(),
                                         compressBuffer.getBufferSize(),
                                         blockSize)),
         buf.size());
    const void *data;
    int length;
    // skip 1/2; in 2 jumps
    ASSERT_TRUE(result->Skip(((N / 2) - 2) * sizeof(int)));
    ASSERT_TRUE(result->Skip(2 * sizeof(int)));
    ASSERT_TRUE(result->Next(&data, &length));
    ASSERT_EQ((N / 2) * sizeof(int), length);
    for (int i=N/2; i < N; ++i) {
      EXPECT_EQ(i % 8, (reinterpret_cast<const int *>(data))[i - N/2]);
    }
  }

}
