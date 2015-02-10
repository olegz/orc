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

#include "zlib.h"

// Temporarily disable snappy in Windows.
// TODO: in the long term, we should put snappy in libs
// and use the snappy from there.
#ifndef _WIN32
#include "snappy.h"
#endif

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
    position = posns.begin();
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

  #if __cplusplus >= 201103L
    SeekableArrayInputStream::SeekableArrayInputStream
       (std::initializer_list<unsigned char> values,
        long blkSize): ownedData(values.size()), data(0) {
      length = values.size();
      memcpy(ownedData.data(), values.begin(), values.size());
      position = 0;
      blockSize = blkSize == -1 ? length : static_cast<unsigned long>(blkSize);
    }
  #endif // __cplusplus

  SeekableArrayInputStream::SeekableArrayInputStream
     (const unsigned char* values, unsigned long size,
      long blkSize): ownedData(size), data(0) {
    length = size;
    char *ptr = ownedData.data();
    for(unsigned long i = 0; i < size; ++i) {
      ptr[i] = static_cast<char>(values[i]);
    }
    position = 0;
    blockSize = blkSize == -1 ? length : static_cast<unsigned long>(blkSize);
  }

  SeekableArrayInputStream::SeekableArrayInputStream(const char* values,
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

  std::string SeekableArrayInputStream::getName() const {
    std::ostringstream result;
    result << "SeekableArrayInputStream " << position << " of " << length;
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
    buffer.resize(blockSize);
    remainder = 0;
  }

  SeekableFileInputStream::~SeekableFileInputStream() {
    // PASS
  }

  bool SeekableFileInputStream::Next(const void** data, int*size) {
    unsigned long bytesRead = std::min(length - position, blockSize);
    if (bytesRead > 0) {
      *data = buffer.data();
      // read from the file, skipping over the remainder
      input->read(buffer.data() + remainder, offset + position + remainder,
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
    memmove(buffer.data(),
            buffer.data() + blockSize - static_cast<size_t>(count),
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
      memmove(buffer.data(), buffer.data() + count, remainder);
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

  enum DecompressState { DECOMPRESS_HEADER,
                         DECOMPRESS_START,
                         DECOMPRESS_CONTINUE,
                         DECOMPRESS_ORIGINAL,
                         DECOMPRESS_EOF};

  class ZlibDecompressionStream: public SeekableInputStream {
  public:
    ZlibDecompressionStream(std::unique_ptr<SeekableInputStream> inStream,
                            size_t blockSize);
//    ZlibDecompressionStream(const ZlibDecompressionStream&) = delete;
//    ZlibDecompressionStream& operator=(const ZlibDecompressionStream&)
//      = delete;
    virtual ~ZlibDecompressionStream();
    virtual bool Next(const void** data, int*size) override;
    virtual void BackUp(int count) override;
    virtual bool Skip(int count) override;
    virtual int64_t ByteCount() const override;
    virtual void seek(PositionProvider& position) override;
    virtual std::string getName() const override;

  private:
    void readBuffer(bool failOnEof) {
      int length;
      if (!input->Next(reinterpret_cast<const void**>(&inputBuffer),
                       &length)) {
        if (failOnEof) {
          throw ParseError("Read past EOF in "
                           "ZlibDecompressionStream::readBuffer");
        }
        state = DECOMPRESS_EOF;
        inputBuffer = nullptr;
        inputBufferEnd = nullptr;
      } else {
        inputBufferEnd = inputBuffer + length;
      }
    }

    uint32_t readByte(bool failOnEof) {
      if (inputBuffer == inputBufferEnd) {
        readBuffer(failOnEof);
        if (state == DECOMPRESS_EOF) {
          return 0;
        }
      }
      return static_cast<unsigned char>(*(inputBuffer++));
    }

    void readHeader() {
      uint32_t header = readByte(false);
      if (state != DECOMPRESS_EOF) {
        header |= readByte(true) << 8;
        header |= readByte(true) << 16;
        if (header & 1) {
          state = DECOMPRESS_ORIGINAL;
        } else {
          state = DECOMPRESS_START;
        }
        remainingLength = header >> 1;
      } else {
        remainingLength = 0;
      }
    }

    const size_t blockSize;
    const std::unique_ptr<SeekableInputStream> input;
    z_stream zstream;
    std::vector<char> buffer;

    // the current state
    DecompressState state;

    // the start of the current buffer
    // This pointer is not owned by us. It is either owned by zstream or
    // the underlying stream.
    const char* outputBuffer;
    // the size of the current buffer
    size_t outputBufferLength;
    // the size of the current chunk
    size_t remainingLength;

    // the last buffer returned from the input
    const char *inputBuffer;
    const char *inputBufferEnd;

    // roughly the number of bytes returned
    off_t bytesReturned;
  };

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wold-style-cast"

  ZlibDecompressionStream::ZlibDecompressionStream
                   (std::unique_ptr<SeekableInputStream> inStream,
                    size_t _blockSize
                    ): blockSize(_blockSize),
                       input(std::move(inStream)),
                       buffer(_blockSize) {
    zstream.next_in = Z_NULL;
    zstream.avail_in = 0;
    zstream.zalloc = Z_NULL;
    zstream.zfree = Z_NULL;
    zstream.opaque = Z_NULL;
    zstream.next_out = reinterpret_cast<Bytef*>(buffer.data());
    zstream.avail_out = static_cast<uInt>(blockSize);
    int result = inflateInit2(&zstream, -15);
    switch (result) {
    case Z_OK:
      break;
    case Z_MEM_ERROR:
      throw std::logic_error("Memory error from inflateInit2");
    case Z_VERSION_ERROR:
      throw std::logic_error("Version error from inflateInit2");
    case Z_STREAM_ERROR:
      throw std::logic_error("Stream error from inflateInit2");
    default:
      throw std::logic_error("Unknown error from inflateInit2");
    }
    outputBuffer = nullptr;
    outputBufferLength = 0;
    remainingLength = 0;
    state = DECOMPRESS_HEADER;
    inputBuffer = nullptr;
    inputBufferEnd = nullptr;
    bytesReturned = 0;
  }

#pragma GCC diagnostic pop

  ZlibDecompressionStream::~ZlibDecompressionStream() {
    int result = inflateEnd(&zstream);
    if (result != Z_OK) {
      // really can't throw in destructors
      std::cout << "Error in ~ZlibDecompressionStream() " << result << "\n";
    }
  }

  bool ZlibDecompressionStream::Next(const void** data, int*size) {
    // if the user pushed back, return them the partial buffer
    if (outputBufferLength) {
      *data = outputBuffer;
      *size = static_cast<int>(outputBufferLength);
      outputBuffer += outputBufferLength;
      outputBufferLength = 0;
      return true;
    }
    if (state == DECOMPRESS_HEADER || remainingLength == 0) {
      readHeader();
    }
    if (state == DECOMPRESS_EOF) {
      return false;
    }
    if (inputBuffer == inputBufferEnd) {
      readBuffer(true);
    }
    size_t availSize =
      std::min(static_cast<size_t>(inputBufferEnd - inputBuffer),
               remainingLength);
    // std::cout << "State: " << state << " remaining = " << remainingLength
    //          << " Buffer = " << (inputBufferEnd - inputBuffer)
    //          << " Avail = " << availSize << " on " << getName() << "\n";
    if (state == DECOMPRESS_ORIGINAL) {
      *data = inputBuffer;
      *size = static_cast<int>(availSize);
      outputBuffer = inputBuffer + availSize;
      outputBufferLength = 0;
    } else if (state == DECOMPRESS_START) {
      zstream.next_in =
        reinterpret_cast<Bytef*>(const_cast<char*>(inputBuffer));
      zstream.avail_in = static_cast<uInt>(availSize);
      outputBuffer = buffer.data();
      zstream.next_out =
        reinterpret_cast<Bytef*>(const_cast<char*>(outputBuffer));
      zstream.avail_out = static_cast<uInt>(blockSize);
      if (inflateReset(&zstream) != Z_OK) {
        throw std::logic_error("Bad inflateReset in "
                               "ZlibDecompressionStream::Next");
      }
      int result;
      do {
        result = inflate(&zstream, availSize == remainingLength ? Z_FINISH :
                         Z_SYNC_FLUSH);
        switch (result) {
        case Z_OK:
          remainingLength -= availSize;
          inputBuffer += availSize;
          readBuffer(true);
          availSize =
            std::min(static_cast<size_t>(inputBufferEnd - inputBuffer),
                     remainingLength);
          zstream.next_in =
            reinterpret_cast<Bytef*>(const_cast<char*>(inputBuffer));
          zstream.avail_in = static_cast<uInt>(availSize);
          break;
        case Z_STREAM_END:
          break;
        case Z_BUF_ERROR:
          throw std::logic_error("Buffer error in "
                                 "ZlibDecompressionStream::Next");
        case Z_DATA_ERROR:
          throw std::logic_error("Data error in "
                                 "ZlibDecompressionStream::Next");
        case Z_STREAM_ERROR:
          throw std::logic_error("Stream error in "
                                 "ZlibDecompressionStream::Next");
        default:
          throw std::logic_error("Unknown error in "
                                 "ZlibDecompressionStream::Next");
        }
      } while (result != Z_STREAM_END);
      *size = static_cast<int>(blockSize - zstream.avail_out);
      *data = outputBuffer;
      outputBufferLength = 0;
      outputBuffer += *size;
    } else {
      throw std::logic_error("Unknown compression state in "
                             "ZlibDecompressionStream::Next");
    }
    inputBuffer += availSize;
    remainingLength -= availSize;
    bytesReturned += *size;
    return true;
  }

  void ZlibDecompressionStream::BackUp(int count) {
    if (outputBuffer == nullptr || outputBufferLength != 0) {
      throw std::logic_error("Backup without previous Next in "
                             "ZlibDecompressionStream");
    }
    outputBuffer -= static_cast<size_t>(count);
    outputBufferLength = static_cast<size_t>(count);
    bytesReturned -= count;
  }

  bool ZlibDecompressionStream::Skip(int count) {
    bytesReturned += count;
    // this is a stupid implementation for now.
    // should skip entire blocks without decompressing
    while (count > 0) {
      const void *ptr;
      int len;
      if (!Next(&ptr, &len)) {
        return false;
      }
      if (len > count) {
        BackUp(len - count);
        count = 0;
      } else {
        count -= len;
      }
    }
    return true;
  }

  int64_t ZlibDecompressionStream::ByteCount() const {
    return bytesReturned;
  }

  void ZlibDecompressionStream::seek(PositionProvider& position) {
    input->seek(position);
    bytesReturned = input->ByteCount();
    if (!Skip(static_cast<int>(position.next()))) {
      throw ParseError("Bad skip in ZlibDecompressionStream::seek");
    }
  }

  std::string ZlibDecompressionStream::getName() const {
    std::ostringstream result;
    result << "zlib(" << input->getName() << ")";
    return result.str();
  }

#ifndef _WIN32
  class SnappyDecompressionStream: public SeekableInputStream {
  public:
    SnappyDecompressionStream(std::unique_ptr<SeekableInputStream> inStream,
                              size_t blockSize);

    virtual ~SnappyDecompressionStream() {}
    virtual bool Next(const void** data, int*size) override;
    virtual void BackUp(int count) override;
    virtual bool Skip(int count) override;
    virtual int64_t ByteCount() const override;
    virtual void seek(PositionProvider& position) override;
    virtual std::string getName() const override;

  private:
    void readBuffer(bool failOnEof) {
      int length;
      if (!input->Next(reinterpret_cast<const void**>(&inputBufferPtr),
                       &length)) {
        if (failOnEof) {
          throw ParseError("SnappyDecompressionStream read past EOF");
        }
        state = DECOMPRESS_EOF;
        inputBufferPtr = nullptr;
        inputBufferPtrEnd = nullptr;
      } else {
        inputBufferPtrEnd = inputBufferPtr + length;
      }
    }

    uint32_t readByte(bool failOnEof) {
      if (inputBufferPtr == inputBufferPtrEnd) {
        readBuffer(failOnEof);
        if (state == DECOMPRESS_EOF) {
          return 0;
        }
      }
      return static_cast<unsigned char>(*(inputBufferPtr++));
    }

    void readHeader() {
      uint32_t header = readByte(false);
      if (state != DECOMPRESS_EOF) {
        header |= readByte(true) << 8;
        header |= readByte(true) << 16;
        if (header & 1) {
          state = DECOMPRESS_ORIGINAL;
        } else {
          state = DECOMPRESS_START;
        }
        remainingLength = header >> 1;
      } else {
        remainingLength = 0;
      }
    }

    const std::unique_ptr<SeekableInputStream> input;

    // may need to stitch together multiple input buffers;
    // to give snappy a contiguous block
    std::vector<char> inputBuffer;

    // uncompressed output
    std::vector<char> outputBuffer;

    // the current state
    DecompressState state;

    // the start of the current output buffer
    const char* outputBufferPtr;
    // the size of the current output buffer
    size_t outputBufferLength;

    // the size of the current chunk
    size_t remainingLength;

    // the last buffer returned from the input
    const char *inputBufferPtr;
    const char *inputBufferPtrEnd;

    // bytes returned by this stream
    off_t bytesReturned;
  };

  SnappyDecompressionStream::SnappyDecompressionStream(
                    std::unique_ptr<SeekableInputStream> inStream,
                    size_t blockSize) :
      input(std::move(inStream)),
      outputBuffer(blockSize),
      state(DECOMPRESS_HEADER),
      outputBufferPtr(0),
      outputBufferLength(0),
      remainingLength(0),
      inputBufferPtr(0),
      inputBufferPtrEnd(0),
      bytesReturned(0)
  {
  }

  bool SnappyDecompressionStream::Next(const void** data, int*size) {
    // if the user pushed back, return them the partial buffer
    if (outputBufferLength) {
      *data = outputBufferPtr;
      *size = static_cast<int>(outputBufferLength);
      outputBufferPtr += outputBufferLength;
      bytesReturned += outputBufferLength;
      outputBufferLength = 0;
      return true;
    }
    if (state == DECOMPRESS_HEADER || remainingLength == 0) {
      readHeader();
    }
    if (state == DECOMPRESS_EOF) {
      return false;
    }
    if (inputBufferPtr == inputBufferPtrEnd) {
      readBuffer(true);
    }

    size_t availSize =
      std::min(static_cast<size_t>(inputBufferPtrEnd - inputBufferPtr),
               remainingLength);
    if (state == DECOMPRESS_ORIGINAL) {
      *data = inputBufferPtr;
      *size = static_cast<int>(availSize);
      outputBufferPtr = inputBufferPtr + availSize;
      outputBufferLength = 0;
      inputBufferPtr += availSize;
      remainingLength -= availSize;
    } else if (state == DECOMPRESS_START) {
      // Get contiguous bytes of compressed block.
      const char *compressed = inputBufferPtr;
      if (remainingLength > availSize) {
        // Did not read enough from input.
        if (inputBuffer.capacity() < remainingLength) {
          inputBuffer.resize(remainingLength);
        }
        ::memcpy(inputBuffer.data(), inputBufferPtr, availSize);
        inputBufferPtr += availSize;
        compressed = inputBuffer.data();

        for (size_t pos = availSize; pos < remainingLength; ) {
          readBuffer(true);
          size_t avail =
              std::min(static_cast<size_t>(inputBufferPtrEnd - inputBufferPtr),
                       remainingLength - pos);
          ::memcpy(inputBuffer.data() + pos, inputBufferPtr, avail);
          pos += avail;
          inputBufferPtr += avail;
        }
      }

      if (!snappy::GetUncompressedLength(compressed, remainingLength,
                                         &outputBufferLength)) {
        throw ParseError("SnappyDecompressionStream choked on corrupt input");
      }

      if (outputBufferLength > outputBuffer.capacity()) {
        throw std::logic_error("uncompressed length exceeds block size");
      }

      if (!snappy::RawUncompress(compressed, remainingLength,
                                 outputBuffer.data())) {
        throw ParseError("SnappyDecompressionStream choked on corrupt input");
      }

      remainingLength = 0;
      state = DECOMPRESS_HEADER;
      *data = outputBuffer.data();
      *size = static_cast<int>(outputBufferLength);
      outputBufferPtr = outputBuffer.data() + outputBufferLength;
    }

    bytesReturned += *size;
    return true;
  }

  void SnappyDecompressionStream::BackUp(int count) {
    if (static_cast<unsigned long>(count) > outputBufferLength) {
      throw std::logic_error("can't backup that far");
    }
    outputBufferPtr -= static_cast<size_t>(count);
    outputBufferLength = static_cast<size_t>(count);
    bytesReturned -= count;
  }

  bool SnappyDecompressionStream::Skip(int count) {
    bytesReturned += count;
    // this is a stupid implementation for now.
    // should skip entire blocks without decompressing
    while (count > 0) {
      const void *ptr;
      int len;
      if (!Next(&ptr, &len)) {
        return false;
      }
      if (len > count) {
        BackUp(len - count);
        count = 0;
      } else {
        count -= len;
      }
    }
    return true;
  }

  int64_t SnappyDecompressionStream::ByteCount() const {
    return bytesReturned;
  }

  void SnappyDecompressionStream::seek(PositionProvider& position) {
    input->seek(position);
    if (!Skip(static_cast<int>(position.next()))) {
      throw ParseError("Bad skip in SnappyDecompressionStream::seek");
    }
  }

  std::string SnappyDecompressionStream::getName() const {
    std::ostringstream result;
    result << "snappy(" << input->getName() << ")";
    return result.str();
  }
#endif // _WIN32
  std::unique_ptr<SeekableInputStream>
     createDecompressor(CompressionKind kind,
                        std::unique_ptr<SeekableInputStream> input,
                        unsigned long blockSize) {
    switch (static_cast<int>(kind)) {
    case CompressionKind_NONE:
      return std::move(input);
    case CompressionKind_ZLIB:
      return std::unique_ptr<SeekableInputStream>
        (new ZlibDecompressionStream(std::move(input), blockSize));
#ifndef _WIN32
    case CompressionKind_SNAPPY:
      return std::unique_ptr<SeekableInputStream>
        (new SnappyDecompressionStream(std::move(input), blockSize));
#endif // _WIN32
    case CompressionKind_LZO:
    default:
      throw NotImplementedYet("compression codec");
    }
  }

}
