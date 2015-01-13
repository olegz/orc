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

#include <initializer_list>
#include <list>
#include <vector>
#include <fstream>
#include <iostream>
#include <sstream>
#include <memory>

#include "zlib.h"

using namespace std;

namespace orc {

  void printBuffer(std::ostream& out,
                   const char *buffer,
                   unsigned long length);

  class PositionProvider {
  private:
    std::list<unsigned long>::const_iterator position;
  public:
    PositionProvider(const std::list<unsigned long>& positions);
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
    char* data;
    unsigned long length;
    unsigned long position;
    unsigned long blockSize;

  public:
    SeekableArrayInputStream(std::initializer_list<unsigned char> list,
                             long block_size = -1);
    SeekableArrayInputStream(char* list,
                             unsigned long length,
                             long block_size = -1);
    virtual ~SeekableArrayInputStream();
    virtual bool Next(const void** data, int*size) override;
    virtual void BackUp(int count) override;
    virtual bool Skip(int count) override;
    virtual google::protobuf::int64 ByteCount() const override;
    virtual void seek(PositionProvider& position) override;
    virtual std::string getName() const override;
  };

  /**
   * Create a seekable input stream based on an input stream.
   */
  class SeekableFileInputStream: public SeekableInputStream {
  private:
    InputStream* input;
    std::unique_ptr<char[]> buffer;
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

    virtual bool Next(const void** data, int*size) override;
    virtual void BackUp(int count) override;
    virtual bool Skip(int count) override;
    virtual google::protobuf::int64 ByteCount() const override;
    virtual void seek(PositionProvider& position) override;
    virtual std::string getName() const override;
  };

  /**
   * Compression base class
   */
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
  virtual void decompress(SeekableInputStream* in, SeekableInputStream* out) = 0;

  };

  /**
   * Zlib codec
   */
  class ZlibCodec: public CompressionCodec {
  public:
      size_t blk_sz; // max uncompressed buffer size per block

      // ctor takes max uncompressed size per block
      ZlibCodec(int blksz) : blk_sz (blksz) {};

      int getBlockSize() { return blk_sz; }

      // compress need input/output, and compression level
      // impl need a vector<char> buf(blk_size) to hold each pass, see http://panthema.net/2007/0328-ZLibString.html
      bool compress(SeekableInputStream* in, SeekableInputStream* out);

      void decompress(SeekableInputStream* in, SeekableInputStream* out);

      string compressBlock(string& in);

      void addORCCompressionHeader(string& in, string& out);

      // unit functions
      string compress(string& in);
      string decompress(string& in);
  };

  class SeekableCompressionInputStream: public SeekableInputStream{
  private:
      std::unique_ptr<SeekableInputStream> input; // dont care if it's an array stream, or file stream
      std::unique_ptr<CompressionCodec> codec; // use it to keep ptr to the real underlying codec
      const unsigned long blockSize;
      std::unique_ptr<char[]> buffer;
      unsigned long offset;
      unsigned long position;
      unsigned long length;
      bool isOriginal; // literal or not
      unsigned long compressedLen; // default 256K, max 2^23, i.e. 8MB

  public:

    virtual bool Skip(int count) { return true;}
    virtual google::protobuf::int64 ByteCount() const { return -1;}
    virtual void seek(PositionProvider& position) {}
    virtual std::string getName() const {return string("getName not implemented!");}

     SeekableCompressionInputStream(int bs) : blockSize(bs) {}
     SeekableCompressionInputStream( std::unique_ptr<SeekableInputStream> in, int blksz) : input (std::move(in)), blockSize(blksz), position(0), length(0) {
         buffer.reset(new char[2* blockSize]); // double allocate
     }

     SeekableCompressionInputStream( std::unique_ptr<SeekableInputStream> in, CompressionKind kind, int blksz) : input (std::move(in)), blockSize(blksz), position(0), length(0) {
         buffer.reset(new char[2* blockSize]); // double allocate
         if ( kind == CompressionKind_ZLIB) {
            codec = std::unique_ptr<CompressionCodec> (new ZlibCodec(blockSize));
         }
         else {
         throw std::string("Only ZLIB decompression is implemented");
         }
     }

     unsigned long getBlockSize() { return blockSize; }

    virtual bool Next(const void** data, int*size) {
        // there are a few cases: 
        // 1) existing buffer has enough available (i.e. >block size). In this case, just return those;
        // 2) if not enough available, then we need to decompress some: check if it is original
        //      a) if original, copy to zlib buffer and return (optimization: if current zlib buffer empty, can return input directly)
        //      b) if not, decompress a block and copy to zlib buffer

        // if 1)
        if( length - position >= blockSize ) {
            *data = &buffer[position];
            *size = static_cast<int>(blockSize);
            position += blockSize;
            return true;
        }
        // decompress header to see if it is original
        const void *ptr;
        int length;
        input->Next(&ptr, &length);

        if(length < 3) 
            return false; // can't get a basic header

        cout << "first Next() read " << length << " bytes" << endl;

        parseCompressionHeader(ptr, length); // read 3 bytes header
        input->BackUp(length - 3); // back to begin of compressed block

        // if is original, return input itself (its position etc would be updated accordingly, too)
        if(isOriginal) {
            // TODO: copy block to zlib buffer and return on zlib buffer
            //return input->Next(data, size);
            const void *ptr;
            int length;

            input->Next(&ptr, &length);

            for(int i = 0; i < length; i++)
                buffer[length+i] =  (((char*)ptr)[i]);
            // check if have enough for a block 
            unsigned long currentSize = std::min(length - position, blockSize);
            if (currentSize > 0) {
                *data = &buffer[position];
                *size = static_cast<int>(currentSize);
                position += currentSize;
                return true;
            }
            // TODO: otherwise keep asking for more from input?
            *size = 0;
            return false;
        }
        else { // else, need to uncompress
            string in;
            int ret = true;
            do {
                ret = input->Next(&ptr, &length);
                //if (!ret) return false;
                in.append(static_cast<const char*>(ptr), length);
                cout << "ret = " << ret << "read another " << length << " bytes in the loop... " << endl;
            } while (ret && in.size() < compressedLen);

            int extra = in.size() - compressedLen;
            input->BackUp(extra);
            in.erase(compressedLen);
            
            //cout << "gonna decom now, in.size() =  " << in.size() << ", content is:" << in <<  endl;
            // now decomp
            string out = decompress(in);
            
            // copy output to data
            for(size_t i =0; i < out.size(); i++) {
                // TODO: check out of bound
                buffer[length+i] = out[i];
            }
            length += out.size();

            unsigned long currentSize = std::min(length - position, blockSize);
            if (currentSize > 0) {
                *data = &buffer[position];
                //*buffer = (data ? data : ownedData.data()) + position;
                *size = static_cast<int>(currentSize);
                position += currentSize;
                return true;
            }
            *size = 0;
            return false;
        }

        return false;
    }

    void BackUp(int count) {
        if (count >= 0) {
            unsigned long unsignedCount = static_cast<unsigned long>(count);
            cout << "zlib backup(): count = " << count << ", position = " << position << endl;
            if (unsignedCount <= blockSize && unsignedCount <= position) {
                position -= unsignedCount;
            } else {
                throw std::logic_error("Can't backup that much!");
            }
        }
    }

    void parseCompressionHeader(const void* ptr, int& length) {
        memset(&compressedLen, 0, sizeof(unsigned long) );
        memcpy(&compressedLen, ptr, 3);
        isOriginal = compressedLen % 2;
        compressedLen /= 2;

        std::cout << "isOriginal = " << isOriginal << ", compress len = " << compressedLen << std::endl;
    }

  string decompress(string& in) {
      // zlib control struct
      z_stream zs;
      zs.zalloc = Z_NULL;
      zs.zfree = Z_NULL;
      zs.opaque = Z_NULL;
      zs.next_in = (Bytef*)in.data();
      /*
      vector<char> vecbuf(in.size());
      for(size_t i = 0; i < in.size(); i++) vecbuf[i] = in[i];
      zs.next_in = (Bytef*)vecbuf.data();
      */

      zs.avail_in = in.size();

      //if (inflateInit(&zs) != Z_OK)
      if (inflateInit2(&zs, -15) != Z_OK) // Hive use zip compression
          throw(std::string("inflateInit failed while decompressing."));

      int ret;
      char buf[getBlockSize()];
      string out; // output string

      // TODO: break inflate loop on input/output availability (since input is a stream)
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

      cout << "jfu: decompress() string done, out.size() = " << out.size() << ", ret = " << ret << endl;

      return out;
  }
  };

  /**
   * Create a codec for the given compression kind.
   * @param kind the compression type to implement
   * @param input the input stream that is the underlying source
   * @param bufferSize the maximum size of the buffer
   */
  std::unique_ptr<SeekableInputStream> 
     createCodec(CompressionKind kind,
                 std::unique_ptr<SeekableInputStream> input,
                 unsigned long bufferSize);
}

#endif
