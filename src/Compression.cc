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

#include <algorithm>
#include <iostream>

#include "Compression.hh"

namespace orc {

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
      long blkSize) {
    length = values.size();
    data = std::unique_ptr<char[]>(new char[length]);
    char *ptr = data.get();
    for(unsigned char ch: values) {
      *(ptr++) = static_cast<char>(ch);
    }
    position = 0;
    blockSize = blkSize == -1 ? length : static_cast<unsigned long>(blkSize);
  }

  // return as much data as possible (returned size in size var), stream still owns buffer
  bool SeekableArrayInputStream::Next(const void** buffer, int*size) {
    unsigned long currentSize = std::min(length - position, blockSize);
    if (currentSize > 0) {
      *buffer = data.get() + position;
      *size = static_cast<int>(currentSize);
      position += currentSize;
      return true;
    }
    return false;
  }

  void SeekableArrayInputStream::BackUp(int count) {
    if (count >= 0) {
      unsigned long unsignedCount = static_cast<unsigned long>(count);
      if (unsignedCount <= blockSize && unsignedCount <= position) {
        position -= unsignedCount;
      }
    }
  }

  bool SeekableArrayInputStream::Skip(int count) {
    if (count >= 0) {
      unsigned long unsignedCount = static_cast<unsigned long>(count);
      if (unsignedCount + position <= length) {
        position += unsignedCount;
        return true;
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

  // decompress 1 block
  void ZlibCodec::decompress(string in, vector<char>& out) {
    // zlib control struct
    z_stream infstream;
    infstream.zalloc = Z_NULL;
    infstream.zfree = Z_NULL;
    infstream.opaque = Z_NULL;

    // check how much left in input stream

    infstream.avail_in = (uInt) in.size(); // size of input
    infstream.next_in = (Bytef*) in.c_str(); // input char array
    infstream.avail_out = (uInt) out.size(); // size of output
    infstream.next_out = (reinterpret_cast<Bytef*> (&out[0])); // output char array
    // do actual work
    inflateInit(&infstream);
    inflate(&infstream, Z_NO_FLUSH);
    inflateEnd(&infstream);
  }
}
