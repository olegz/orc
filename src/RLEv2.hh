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

#ifndef ORC_RLEV2_HH
#define ORC_RLEV2_HH

#include "RLE.hh"

namespace orc {

class RleDecoderV2 : public RleDecoder {
public:

  enum EncodingType { SHORT_REPEAT=0, DIRECT=1, PATCHED_BASE=2, DELTA=3 };

  RleDecoderV2(std::unique_ptr<SeekableInputStream> input,
               bool isSigned);

  /**
  * Seek to a particular spot.
  */
  void seek(PositionProvider&) override;

  /**
  * Seek over a given number of values.
  */
  void skip(unsigned long numValues) override;

  /**
  * Read a number of values into the batch.
  */
  void next(long* data, unsigned long numValues,
            const char* notNull) override;

private:

  signed char readByte();
  unsigned long readLongBE();
  unsigned long readVslong();
  unsigned long readVulong();
  void readLongs(long *data, unsigned long offset, unsigned len);

  unsigned long nextShortRepeats(long* data, unsigned long offset,
                                 unsigned long numValues,
                                 const char* notNull);
  unsigned long nextDirect(long* data, unsigned long offset,
                           unsigned long numValues,
                           const char* notNull);
  unsigned long nextDelta(long* data, unsigned long offset,
                          unsigned long numValues,
                          const char* notNull);

  const std::unique_ptr<SeekableInputStream> inputStream;
  const bool isSigned;

  signed char firstByte;
  signed char prevByte;
  unsigned long runLength;
  unsigned long runRead;
  const char *bufferStart;
  const char *bufferEnd;
  long deltaBase; // Used by DELTA
  int byteSize; // Used by SHORT_REPEAT
  long firstValue; // Used by SHORT_REPEAT and DELTA
  long prevValue; // Used by DELTA
  int bitSize; // Used by DIRECT and DELTA
  int bitsLeft; // Used by DIRECT
  int curByte; // Used by DIRECT
};
}  // namespace orc

#endif  // ORC_RLEV2_HH
