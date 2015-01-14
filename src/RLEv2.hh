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

#include <vector>
#include <stddef.h>

namespace orc {

class RleDecoderV2 : public RleDecoder {
public:

  enum EncodingType { SHORT_REPEAT=0, DIRECT=1, PATCHED_BASE=2, DELTA=3 };

  RleDecoderV2(std::auto_ptr<SeekableInputStream> input,
               bool isSigned);

  /**
  * Seek to a particular spot.
  */
  void seek(PositionProvider&);

  /**
  * Seek over a given number of values.
  */
  void skip(unsigned long numValues);

  /**
  * Read a number of values into the batch.
  */
  void next(long* data, unsigned long numValues,
            const char* notNull);

private:

  // Used by PATCHED_BASE
  void adjustGapAndPatch() {
    curGap = static_cast<unsigned long>(unpackedPatch[patchIdx]) >> patchBitSize;
    curPatch = unpackedPatch[patchIdx] & patchMask;
    actualGap = 0;

    // special case: gap is >255 then patch value will be 0.
    // if gap is <=255 then patch value cannot be 0
    while (curGap == 255 && curPatch == 0) {
      actualGap += 255;
      ++patchIdx;
      curGap = static_cast<unsigned long>(unpackedPatch[patchIdx]) >> patchBitSize;
      curPatch = unpackedPatch[patchIdx] & patchMask;
    }
    // add the left over gap
    actualGap += curGap;
  }

  unsigned char readByte();
  unsigned long readLongBE(unsigned bsz);
  unsigned long readVslong();
  unsigned long readVulong();
  unsigned long readLongs(long *data, unsigned long offset, unsigned len,
                          unsigned fb, const char* notNull = NULL);

  unsigned long nextShortRepeats(long* data, unsigned long offset,
                                 unsigned long numValues,
                                 const char* notNull);
  unsigned long nextDirect(long* data, unsigned long offset,
                           unsigned long numValues,
                           const char* notNull);
  unsigned long nextPatched(long* data, unsigned long offset,
                            unsigned long numValues,
                            const char* notNull);
  unsigned long nextDelta(long* data, unsigned long offset,
                          unsigned long numValues,
                          const char* notNull);

  const std::auto_ptr<SeekableInputStream> inputStream;
  const bool isSigned;

  unsigned char firstByte;
  unsigned long runLength;
  unsigned long runRead;
  const char *bufferStart;
  const char *bufferEnd;
  long deltaBase; // Used by DELTA
  int byteSize; // Used by SHORT_REPEAT and PATCHED_BASE
  long firstValue; // Used by SHORT_REPEAT and DELTA
  long prevValue; // Used by DELTA
  int bitSize; // Used by DIRECT, PATCHED_BASE and DELTA
  int bitsLeft; // Used by anything that uses readLongs
  int curByte; // Used by anything that uses readLongs
  // TODO: Allow allocator for buffer.
  std::vector<long> unpacked; // Used by PATCHED_BASE
  std::vector<long> unpackedPatch; // Used by PATCHED_BASE
  int patchBitSize; // Used by PATCHED_BASE
  unsigned long unpackedIdx; // Used by PATCHED_BASE
  unsigned long patchIdx; // Used by PATCHED_BASE
  long base; // Used by PATCHED_BASE
  long curGap; // Used by PATCHED_BASE
  long curPatch; // Used by PATCHED_BASE
  long patchMask; // Used by PATCHED_BASE
  long actualGap; // Used by PATCHED_BASE
};
}  // namespace orc

#endif  // ORC_RLEV2_HH
