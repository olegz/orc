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

#ifndef ORC_COMPRESSION_HH
#define ORC_COMPRESSION_HH

#include "orc/OrcFile.hh"
#include "wrap/zero-copy-stream-wrapper.h"

#include <list>
#include <vector>
#include <fstream>
#include <iostream>
#include <sstream>
#include <memory>
#include <vector>
#if __cplusplus >= 201103L
  #include <initializer_list>
#endif


#include "zlib.h"

using namespace std;

namespace orc {

  void printBuffer(std::ostream& out,
                   const char *buffer,
                   unsigned long length);

  class PositionProvider {
  private:
    std::list<unsigned long>::iterator position;
  public:
    PositionProvider(std::list<unsigned long>& positions);
    unsigned long next();
  };

  /**
   * A subclass of Google's ZeroCopyInputStream that supports seek.
   * By extending Google's class, we get the ability to pass it directly
   * to the protobuf readers.
   */
  class SeekableInputStream: public google::protobuf::io::ZeroCopyInputStream {
  public:
    virtual ~SeekableInputStream();
    virtual void seek(PositionProvider& position) = 0;
    virtual std::string getName() const = 0;
  };

  /**
   * Create a seekable input stream based on a memory range.
   */
  class SeekableArrayInputStream: public SeekableInputStream {
  private:
    std::vector<char> ownedData;
    const char* data;
    unsigned long length;
    unsigned long position;
    unsigned long blockSize;

  public:
    #if __cplusplus >= 201103L
    SeekableArrayInputStream(std::initializer_list<unsigned char> list,
                             long block_size = -1);
    #endif

    SeekableArrayInputStream(const unsigned char* list,
                             unsigned long length,
                             long block_size = -1);

    SeekableArrayInputStream(const char* list,
                             unsigned long length,
                             long block_size = -1);
    virtual ~SeekableArrayInputStream();
    virtual bool Next(const void** data, int*size)  ;
    virtual void BackUp(int count)  ;
    virtual bool Skip(int count)  ;
    virtual google::protobuf::int64 ByteCount() const  ;
    virtual void seek(PositionProvider& position)  ;
    virtual std::string getName() const  ;
  };

  /**
   * Create a seekable input stream based on an input stream.
   */
  class SeekableFileInputStream: public SeekableInputStream {
  private:
    InputStream* input;
    std::vector<char> buffer;
    unsigned long offset;
    unsigned long length;
    unsigned long position;
    unsigned long blockSize;
    unsigned long remainder;

  public:
    SeekableFileInputStream(InputStream* input,
                            unsigned long offset,
                            unsigned long length,
                            long blockSize = -1);
    virtual ~SeekableFileInputStream();

    virtual bool Next(const void** data, int*size)  ;
    virtual void BackUp(int count)  ;
    virtual bool Skip(int count)  ;
    virtual google::protobuf::int64 ByteCount() const  ;
    virtual void seek(PositionProvider& position)  ;
    virtual std::string getName() const  ;
  };

  /**
   * Compression base class
   */
  class SeekableCompressionInputStream;
  class CompressionCodec {
  public:

  /**
   * Compress the in buffer to the out buffer.
   * @param in the bytes to compress
   * @param out the uncompressed bytes
   * @return true if the output is smaller than input
   */
  virtual bool compress(SeekableInputStream* in, SeekableInputStream* out) = 0;

  /**
   * Decompress the in buffer to the out buffer.
   * @param in the bytes to decompress
   * @param out the decompressed bytes
   */
  virtual void decompress(SeekableInputStream* in, SeekableCompressionInputStream* out) = 0;

  };

  /**
   * Zlib codec
   */
  class ZlibCodec: public CompressionCodec {
  public:
      size_t blk_sz; // max uncompressed buffer size per block

      ZlibCodec(int blksz) : blk_sz (blksz) {};

      bool compress(SeekableInputStream* in, SeekableInputStream* out);

      void decompress(SeekableInputStream* in, SeekableCompressionInputStream* out);

      // utility functions
      // TODO: maybe make more sense to move these into some ORC-gen object other than ZlibCodec...
      string compressToOrcBlocks(string& in);

      string compressToZlibBlock(string& in);

      string addORCCompressionHeader(string& in, string& out);

      string decompressZlibBlock(string& in);

      int getBlockSize() { return blk_sz; }
  };

  class SeekableCompressionInputStream: public SeekableInputStream{
  public:
      std::auto_ptr<SeekableInputStream> input; // dont care if it's an array stream, or file stream
      std::auto_ptr<CompressionCodec> codec; // use it to keep ptr to the real underlying codec
      const unsigned long blockSize;
      std::vector<char> buffer;
      unsigned long offset;
      unsigned long size; // current # of bytes on buffer
      unsigned long capacity; // max size
      unsigned long byteCount; // count effective bytes seen so far
      bool isOriginal; // literal or not
      unsigned long compressedLen; // default 256K, max 2^23, i.e. 8MB

    bool Next(const void** data, int*size) ;
    void BackUp(int count) ;
    bool Skip(int count) ;
    google::protobuf::int64 ByteCount() const ;
    std::string getName() const ;

    // Oops.. forgot this one..
    void seek(PositionProvider& position) {}

     SeekableCompressionInputStream(int bs) : blockSize(bs) {}

     SeekableCompressionInputStream( std::auto_ptr<SeekableInputStream> in,
         CompressionKind kind, int blksz) : input (in), blockSize(blksz),
             offset(0), size(0), byteCount(0) {
         capacity = 2 * blockSize;// double allocate
         buffer.resize(capacity);
         if ( kind == CompressionKind_ZLIB) {
            codec = std::auto_ptr<CompressionCodec> (new ZlibCodec(blockSize));
         }
         else {
         throw std::string("Only ZLIB decompression is implemented");
         }
     }

     unsigned long getBlockSize() { return blockSize; }

    // need contiguous buffer space of len, move leftover to beginning if necessary
    void alignBuffer(size_t len) { 
        // safe guard, should never happen
        if ( capacity - size < len)
            throw string("Not enough space in SeekableCompressionInputStream's internal buffer!");

        if ( capacity - (offset + size) < len ) { // if not enough space available at end of buffer, move things to beginning first
            size_t count = 0; 
            while( count < size) {
                buffer[count] = buffer[offset + count];
                count ++;
            }
            offset = 0;
        }
    }

    void copyToBuffer(const void *ptr, size_t len) {
        alignBuffer(len);

        // now copy again
        for(size_t i = 0; i < len; i++)
            buffer[offset + size +i] =  (((char*)ptr)[i]);
        size += len; 
    }

    void parseCompressionHeader(const void* ptr, int& len) {
        memset(&compressedLen, 0, sizeof(unsigned long) );
        memcpy(&compressedLen, ptr, 3);
        isOriginal = compressedLen % 2;
        compressedLen /= 2;
    }

  };

  /**
   * Create a codec for the given compression kind.
   * @param kind the compression type to implement
   * @param input the input stream that is the underlying source
   * @param bufferSize the maximum size of the buffer
   */
  std::auto_ptr<SeekableInputStream> 
     createCodec(CompressionKind kind,
                 std::auto_ptr<SeekableInputStream> input,
                 unsigned long bufferSize);
}

#endif
