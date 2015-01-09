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

#include <algorithm>
#include <iomanip>
#include <iostream>
#include <sstream>

namespace orc {

  void printBuffer(std::ostream& out,
                   const char *buffer,
                   unsigned long length) {
    const unsigned long width = 24;
    out << std::hex;
    for(unsigned long line = 0; line < (length + width - 1) / width; ++line) {
      out << std::setfill('0') << std::setw(7) << (line * width);
      for(unsigned long byte = 0;
          byte < width && line * width + byte < length; ++byte) {
        out << " " << std::setfill('0') << std::setw(2)
                  << static_cast<unsigned int>(0xff & buffer[line * width +
                                                             byte]);
      }
      out << "\n";
    }
    out << std::dec;
  }

  PositionProvider::PositionProvider(const std::list<unsigned long>& posns) {
    position = posns.cbegin();
  }

  unsigned long PositionProvider::next() {
    unsigned long result = *position;
    ++position;
    return result;
  }

  SeekableInputStream::~SeekableInputStream() {
    // PASS
  }

  SeekableArrayInputStream::~SeekableArrayInputStream() {
    // PASS
  }

  SeekableArrayInputStream::SeekableArrayInputStream
     (std::initializer_list<unsigned char> values,
      long blkSize): ownedData(values.size()), data(0) {
    length = values.size();
    char *ptr = ownedData.data();
    for(unsigned char ch: values) {
      *(ptr++) = static_cast<char>(ch);
    }
    position = 0;
    blockSize = blkSize == -1 ? length : static_cast<unsigned long>(blkSize);
  }

  SeekableArrayInputStream::SeekableArrayInputStream(char* values, 
                                                     unsigned long size,
                                                     long blkSize
                                                     ): ownedData(0),
                                                        data(values) {
    length = size;
    position = 0;
    blockSize = blkSize == -1 ? length : static_cast<unsigned long>(blkSize);
  }

  bool SeekableArrayInputStream::Next(const void** buffer, int*size) {
    unsigned long currentSize = std::min(length - position, blockSize);
    if (currentSize > 0) {
      *buffer = (data ? data : ownedData.data()) + position;
      *size = static_cast<int>(currentSize);
      position += currentSize;
      return true;
    }
    *size = 0;
    return false;
  }

  void SeekableArrayInputStream::BackUp(int count) {
    if (count >= 0) {
      unsigned long unsignedCount = static_cast<unsigned long>(count);
      if (unsignedCount <= blockSize && unsignedCount <= position) {
        position -= unsignedCount;
      } else {
        throw std::logic_error("Can't backup that much!");
      }
    }
  }

  bool SeekableArrayInputStream::Skip(int count) {
    if (count >= 0) {
      unsigned long unsignedCount = static_cast<unsigned long>(count);
      if (unsignedCount + position <= length) {
        position += unsignedCount;
        return true;
      } else {
        position = length;
      }
    }
    return false;
  }

  google::protobuf::int64 SeekableArrayInputStream::ByteCount() const {
    return static_cast<google::protobuf::int64>(position);
  }

  void SeekableArrayInputStream::seek(PositionProvider& seekPosition) {
    position = seekPosition.next();
  }

  bool ZlibCodec::compress(SeekableInputStream* in, SeekableInputStream* out) {
      throw string("Zlib compression not implemented yet!");
  }

  void ZlibCodec::decompress(SeekableInputStream* in, SeekableInputStream* out) {
      throw string("Zlib decompress not implemented yet!");
  }

  string ZlibCodec::compress(string& in, int compr_level /* = Z_BEST_COMPRESSION */) {
      // zlib control struct
      z_stream zs;
      zs.zalloc = Z_NULL;
      zs.zfree = Z_NULL;
      zs.opaque = Z_NULL;
      zs.next_in = (Bytef*)in.data();
      zs.avail_in = in.size();

      // zip, refer to zlib manual for params
      if (deflateInit2(&zs, compr_level, Z_DEFLATED/*default*/, -15, 8 /*default*/, Z_DEFAULT_STRATEGY /*default*/ ) != Z_OK)
          throw(std::string("deflateInit failed while compressing."));

      int ret;
      char buf[getBlockSize()];
      string out; // output string

      do {
          zs.next_out = reinterpret_cast<Bytef*>(buf);
          zs.avail_out = sizeof(buf);

          ret = deflate(&zs, Z_FINISH);

          // take everything out of output buf every call
          int have = sizeof(buf) - zs.avail_out; 
          out.append(buf, have);
      } while (ret == Z_OK) ;

      deflateEnd(&zs);

      // did not finish (reach EOF) properly
      if (ret != Z_STREAM_END) 
          throw(std::string("Exception during Zlib compression"));

      //addORCCompressionHeader(in, out); // add header

      return out;
  }

  void ZlibCodec::addORCCompressionHeader(string& in, string& out) {
      bool isOriginal = out.size() < in.size(); // TODO: how to tell exactly?
      unsigned long compressedLen = out.size();
      compressedLen *= 2;
      if(isOriginal) 
          compressedLen += 1;

      string header;
      for(int i = 0; i < 3; i++) 
          header = header + static_cast<char> ( * ((char*) (&compressedLen) + i));

      out = header + out;

  }

  string ZlibCodec::decompress(string& in) {
      // zlib control struct
      z_stream zs;
      zs.zalloc = Z_NULL;
      zs.zfree = Z_NULL;
      zs.opaque = Z_NULL;
      zs.next_in = (Bytef*)in.data();
      zs.avail_in = in.size();

      //if (inflateInit(&zs) != Z_OK)
      if (inflateInit2(&zs, -15) != Z_OK) // Hive use zip compression
          throw(std::string("inflateInit failed while decompressing."));

      int ret;
      char buf[getBlockSize()];
      string out; // output string

      // TODO: decompress whole thing
      do {
          zs.next_out = reinterpret_cast<Bytef*>(buf);
          zs.avail_out = sizeof(buf);

          ret = inflate(&zs, 0);

          // take everything out of output buf every call
          int have = sizeof(buf) - zs.avail_out; 
          out.append(buf, have);
      } while (ret == Z_OK) ;

      inflateEnd(&zs);

      // did not finish (reach EOF) properly
      if (ret != Z_STREAM_END) 
          throw(std::string("Exception during Zlib decompression"));

      return out;
  }

  std::string SeekableArrayInputStream::getName() const {
    std::ostringstream result;
    result << "memory from " << std::hex << (data ? data : ownedData.data())
           << std::dec << " for " << length;
    return result.str();
  }

  SeekableFileInputStream::SeekableFileInputStream(InputStream* _input,
                                                   unsigned long _offset,
                                                   unsigned long _length,
                                                   long _blockSize) {
    input = _input;
    offset = _offset;
    length = _length;
    position = 0;
    blockSize = std::min(length,
                         static_cast<unsigned long>(_blockSize < 0 ? 
                                                    256 * 1024 : _blockSize));
    buffer.reset(new char[blockSize]);
    remainder = 0;
  }

  SeekableFileInputStream::~SeekableFileInputStream() {
    // PASS
  }

  bool SeekableFileInputStream::Next(const void** data, int*size) {
    unsigned long bytesRead = std::min(length - position, blockSize);
    if (bytesRead > 0) {
      *data = buffer.get();
      // read from the file, skipping over the remainder
      input->read(buffer.get() + remainder, offset + position + remainder, 
                  bytesRead - remainder);
      position += bytesRead;
      remainder = 0;
    }
    *size = static_cast<int>(bytesRead);
    return bytesRead != 0;
  }

  void SeekableFileInputStream::BackUp(int count) {
    if (position == 0 || remainder > 0) {
      throw std::logic_error("can't backup unless we just called Next");
    }
    if (static_cast<unsigned long>(count) > blockSize) {
      throw std::logic_error("can't backup that far");
    }
    remainder = static_cast<unsigned long>(count);
    position -= remainder;
    memmove(buffer.get(), 
            buffer.get() + blockSize - static_cast<size_t>(count), 
            static_cast<size_t>(count));
  }

  bool SeekableFileInputStream::Skip(int _count) {
    if (_count < 0) {
      return false;
    }
    unsigned long count = static_cast<unsigned long>(_count);
    position += count;
    if (position > length) {
      position = length;
      remainder = 0;
      return false;
    }
    if (remainder > count) {
      remainder -= count;
      memmove(buffer.get(), buffer.get() + count, remainder);
    } else {
      remainder = 0;
    }
    return true;
  }
  
  google::protobuf::int64 SeekableFileInputStream::ByteCount() const {
    return static_cast<google::protobuf::int64>(position);
  }

  void SeekableFileInputStream::seek(PositionProvider& location) {
    position = location.next();
    if (position > length) {
      position = length;
      throw std::logic_error("seek too far");
    }
    remainder = 0;
  }

  std::string SeekableFileInputStream::getName() const {
    std::ostringstream result;
    result << input->getName() << " from " << offset << " for "
           << length;
    return result.str();
  }

  std::unique_ptr<SeekableInputStream> 
     createCodec(CompressionKind kind,
                 std::unique_ptr<SeekableInputStream> input,
                 unsigned long blockSize) {
    switch (kind) {
    case CompressionKind_NONE:
      return std::move(input);
    case CompressionKind_LZO:
      break;
    case CompressionKind_SNAPPY:
      break;
    case CompressionKind_ZLIB: {
      // PASS
      //return new ZlibCodec2(input, blockSize);
      return std::unique_ptr<SeekableInputStream> ( new ZlibCodec2(move(input), blockSize));
    }
    }
    throw NotImplementedYet("compression codec");
  }
}
