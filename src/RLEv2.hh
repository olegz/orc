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
  void next(int64_t* data, unsigned long numValues,
            const char* notNull) override;

private:

  // Used by PATCHED_BASE
  void adjustGapAndPatch() {
    curGap = static_cast<unsigned long>((*unpackedPatch)[patchIdx]) >>
      patchBitSize;
    curPatch = (*unpackedPatch)[patchIdx] & patchMask;
    actualGap = 0;

    // special case: gap is >255 then patch value will be 0.
    // if gap is <=255 then patch value cannot be 0
    while (curGap == 255 && curPatch == 0) {
      actualGap += 255;
      ++patchIdx;
      curGap = static_cast<unsigned long>((*unpackedPatch)[patchIdx]) >>
        patchBitSize;
      curPatch = (*unpackedPatch)[patchIdx] & patchMask;
    }
    // add the left over gap
    actualGap += curGap;
  }

  void resetReadLongs() {
    bitsLeft = 0;
    curByte = 0;
  }

  void resetRun() {
    resetReadLongs();
    bitSize = 0;
  }

  unsigned char readByte();
  int64_t readLongBE(uint32_t bsz);
  int64_t readVslong();
  uint64_t readVulong();
  uint64_t readLongs(int64_t *data, uint64_t offset, uint64_t len,
                     uint64_t fb, const char* notNull = nullptr);

  uint64_t nextShortRepeats(int64_t* data, uint64_t offset, uint64_t numValues,
                            const char* notNull);
  uint64_t nextDirect(int64_t* data, uint64_t offset, uint64_t numValues,
                      const char* notNull);
  uint64_t nextPatched(int64_t* data, uint64_t offset, uint64_t numValues,
                       const char* notNull);
  uint64_t nextDelta(int64_t* data, uint64_t offset, uint64_t numValues,
                     const char* notNull);

  const std::unique_ptr<SeekableInputStream> inputStream;
  const bool isSigned;

  unsigned char firstByte;
  uint64_t runLength;
  uint64_t runRead;
  const char *bufferStart;
  const char *bufferEnd;
  long deltaBase; // Used by DELTA
  unsigned int byteSize; // Used by SHORT_REPEAT and PATCHED_BASE
  int64_t firstValue; // Used by SHORT_REPEAT and DELTA
  long prevValue; // Used by DELTA
  uint32_t bitSize; // Used by DIRECT, PATCHED_BASE and DELTA
  uint32_t bitsLeft; // Used by anything that uses readLongs
  uint32_t curByte; // Used by anything that uses readLongs
  uint32_t patchBitSize; // Used by PATCHED_BASE
  unsigned long unpackedIdx; // Used by PATCHED_BASE
  unsigned long patchIdx; // Used by PATCHED_BASE
  long base; // Used by PATCHED_BASE
  unsigned long curGap; // Used by PATCHED_BASE
  long curPatch; // Used by PATCHED_BASE
  long patchMask; // Used by PATCHED_BASE
  long actualGap; // Used by PATCHED_BASE
  // TODO: Allow allocator for buffer.
//  std::vector<int64_t> unpacked; // Used by PATCHED_BASE
//  std::vector<int64_t> unpackedPatch; // Used by PATCHED_BASE
  std::unique_ptr<DataBuffer<int64_t> > unpacked; // Used by PATCHED_BASE
  std::unique_ptr<DataBuffer<int64_t> > unpackedPatch; // Used by PATCHED_BASE

};
}  // namespace orc

#endif  // ORC_RLEV2_HH
