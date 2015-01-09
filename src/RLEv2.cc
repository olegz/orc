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

#include "RLEv2.hh"
#include "Compression.hh"
#include "Exceptions.hh"

#define MIN_REPEAT 3

namespace orc {

inline long unZigZag(unsigned long value) {
  return value >> 1 ^ -(value & 1);
}

struct FixedBitSizes {
  enum FBS {
    ONE = 0, TWO, THREE, FOUR, FIVE, SIX, SEVEN, EIGHT, NINE, TEN, ELEVEN, TWELVE,
    THIRTEEN, FOURTEEN, FIFTEEN, SIXTEEN, SEVENTEEN, EIGHTEEN, NINETEEN,
    TWENTY, TWENTYONE, TWENTYTWO, TWENTYTHREE, TWENTYFOUR, TWENTYSIX,
    TWENTYEIGHT, THIRTY, THIRTYTWO, FORTY, FORTYEIGHT, FIFTYSIX, SIXTYFOUR
  };
};

inline int decodeBitWidth(int n) {
  if (n >= FixedBitSizes::ONE &&
      n <= FixedBitSizes::TWENTYFOUR) {
    return n + 1;
  } else if (n == FixedBitSizes::TWENTYSIX) {
    return 26;
  } else if (n == FixedBitSizes::TWENTYEIGHT) {
    return 28;
  } else if (n == FixedBitSizes::THIRTY) {
    return 30;
  } else if (n == FixedBitSizes::THIRTYTWO) {
    return 32;
  } else if (n == FixedBitSizes::FORTY) {
    return 40;
  } else if (n == FixedBitSizes::FORTYEIGHT) {
    return 48;
  } else if (n == FixedBitSizes::FIFTYSIX) {
    return 56;
  } else {
    return 64;
  }
}

inline int getClosestFixedBits(int n) {
  if (n == 0) {
    return 1;
  }

  if (n >= 1 && n <= 24) {
    return n;
  } else if (n > 24 && n <= 26) {
    return 26;
  } else if (n > 26 && n <= 28) {
    return 28;
  } else if (n > 28 && n <= 30) {
    return 30;
  } else if (n > 30 && n <= 32) {
    return 32;
  } else if (n > 32 && n <= 40) {
    return 40;
  } else if (n > 40 && n <= 48) {
    return 48;
  } else if (n > 48 && n <= 56) {
    return 56;
  } else {
    return 64;
  }
}

void RleDecoderV2::readLongs(long *data, unsigned long offset, unsigned len,
                             unsigned fb) {
  // TODO: unroll to improve performance
  for(unsigned long i = offset; i < (offset + len); i++) {
    unsigned long result = 0;
    int bitsLeftToRead = fb;
    while (bitsLeftToRead > bitsLeft) {
      result <<= bitsLeft;
      result |= curByte & ((1 << bitsLeft) - 1);
      bitsLeftToRead -= bitsLeft;
      curByte = readByte();
      bitsLeft = 8;
    }

    // handle the left over bits
    if (bitsLeftToRead > 0) {
      result <<= bitsLeftToRead;
      bitsLeft -= bitsLeftToRead;
      result |= (curByte >> bitsLeft) & ((1 << bitsLeftToRead) - 1);
    }
    data[i] = static_cast<long>(result);
  }
}

unsigned char RleDecoderV2::readByte() {
  if (bufferStart == bufferEnd) {
    int bufferLength;
    const void* bufferPointer;
    if (!inputStream->Next(&bufferPointer, &bufferLength)) {
      throw ParseError("bad read in readByte");
    }
    bufferStart = static_cast<const char*>(bufferPointer);
    bufferEnd = bufferStart + bufferLength;
  }

  return *bufferStart++;
}

unsigned long RleDecoderV2::readLongBE(unsigned bsz) {
  unsigned long ret = 0, val;
  unsigned n = bsz;
  while (n > 0) {
    n--;
    val = readByte();
    ret |= (val << (n * 8));
  }
  return ret;
}

unsigned long RleDecoderV2::readVslong() {
  unsigned long ret = readVulong();
  return (ret >> 1) ^ -(ret & 1);
}

unsigned long RleDecoderV2::readVulong() {
  unsigned long ret = 0, b;
  unsigned int offset = 0;
  do {
    b = readByte();
    ret |= (0x7f & b) << offset;
    offset += 7;
  } while (b >= 0x80);
  return ret;
}

RleDecoderV2::RleDecoderV2(std::unique_ptr<SeekableInputStream> input,
                           bool isSigned)
  : inputStream(std::move(input)),
    isSigned(isSigned),
    firstByte(0),
    runLength(0),
    runRead(0),
    bufferStart(nullptr),
    bufferEnd(bufferStart),
    deltaBase(0),
    byteSize(0),
    firstValue(0),
    prevValue(0),
    bitSize(0),
    bitsLeft(0),
    curByte(0),
    patchBitSize(0),
    base(0),
    curGap(0),
    patchMask(0),
    actualGap(0) {
}

void RleDecoderV2::seek(PositionProvider& location) {
  // TODO: implement
}

void RleDecoderV2::skip(unsigned long numValues) {
  // TODO: implement
}

void RleDecoderV2::next(long* const data,
                        const unsigned long numValues,
                        const char* const notNull) {
  // TODO: handle nulls
  unsigned long nRead = 0;
  while (nRead < numValues) {
    if (runRead == runLength) {
      firstByte = readByte();
    }

    unsigned long offset = nRead, length = numValues - nRead;

    EncodingType enc = static_cast<EncodingType>
        ((firstByte >> 6) & 0x03);
    switch(enc) {
    case SHORT_REPEAT:
      nRead += nextShortRepeats(data, offset, length, notNull);
      break;
    case DIRECT:
      nRead += nextDirect(data, offset, length, notNull);
      break;
    case PATCHED_BASE:
      nRead += nextPatched(data, offset, length, notNull);
      break;
    case DELTA:
      nRead += nextDelta(data, offset, length, notNull);
      break;
    default:
      throw ParseError("unknown encoding");
    }
  }
}

unsigned long RleDecoderV2::nextShortRepeats(long* const data,
                                             unsigned long offset,
                                             unsigned long numValues,
                                             const char* const notNull) {
  if (runRead == runLength) {
    // extract the number of fixed bytes
    byteSize = (firstByte >> 3) & 0x07;
    byteSize += 1;

    runLength = firstByte & 0x07;
    // run lengths values are stored only after MIN_REPEAT value is met
    runLength += MIN_REPEAT;
    runRead = 0;

    // read the repeated value which is store using fixed bytes
    firstValue = readLongBE(byteSize);

    if (isSigned) {
      firstValue = unZigZag(firstValue);
    }
  }

  unsigned long nRead = std::min(runLength - runRead, numValues);
  for(unsigned long pos = offset; pos < offset + nRead; ++pos) {
    data[pos] = firstValue;
  }

  runRead += nRead;
  return nRead;
}

unsigned long RleDecoderV2::nextDirect(long* const data,
                                       unsigned long offset,
                                       unsigned long numValues,
                                       const char* const notNull) {
  if (runRead == runLength) {
    // extract the number of fixed bits
    unsigned char fbo = (firstByte >> 1) & 0x1f;
    bitSize = decodeBitWidth(fbo);
    bitsLeft = 0;
    curByte = 0;

    // extract the run length
    runLength = (firstByte & 0x01) << 8;
    runLength |= readByte();
    // runs are one off
    runLength += 1;
    runRead = 0;
  }

  unsigned long nRead = std::min(runLength - runRead, numValues);

  readLongs(data, offset, nRead, bitSize);
  if (isSigned) { 
    // write the unpacked values and zigzag decode to result buffer
    for(unsigned long pos = offset; pos < offset + nRead; ++pos) {
      data[pos] = unZigZag(data[pos]);
    }
  }

  runRead += nRead;
  return nRead;
}

unsigned long RleDecoderV2::nextPatched(long* const data,
                                        unsigned long offset,
                                        unsigned long numValues,
                                        const char* const notNull) {
  if (runRead == runLength) {
    // extract the number of fixed bits
    unsigned char fbo = (firstByte >> 1) & 0x1f;
    bitSize = decodeBitWidth(fbo);

    // extract the run length
    runLength = (firstByte & 0x01) << 8;
    runLength |= readByte();
    // runs are one off
    runLength += 1;
    runRead = 0;

    // extract the number of bytes occupied by base
    unsigned int thirdByte = readByte();
    byteSize = (thirdByte >> 5) & 0x07;
    // base width is one off
    byteSize += 1;

    // extract patch width
    unsigned int pwo = thirdByte & 0x1f;
    patchBitSize = decodeBitWidth(pwo);

    // read fourth byte and extract patch gap width
    unsigned int fourthByte = readByte();
    int pgw = (fourthByte >> 5) & 0x07;
    // patch gap width is one off
    pgw += 1;

    // extract the length of the patch list
    int pl = fourthByte & 0x1f;

    // read the next base width number of bytes to extract base value
    base = readLongBE(byteSize);
    long mask = (1L << ((byteSize * 8) - 1));
    // if mask of base value is 1 then base is negative value else positive
    if ((base & mask) != 0) {
      base = base & ~mask;
      base = -base;
    }

    // TODO: something more efficient than resize
    unpacked.resize(runLength);
    unpackedIdx = 0;
    readLongs(unpacked.data(), 0, runLength, bitSize);
    // any remaining bits are thrown out
    bitsLeft = 0;

    // TODO: something more efficient than resize
    unpackedPatch.resize(pl);
    patchIdx = 0;
    // TODO: Skip corrupt?
    //    if ((patchBitSize + pgw) > 64 && !skipCorrupt) {
    if ((patchBitSize + pgw) > 64) {
      throw ParseError("Corrupt PATCHED_BASE encoded data!");
    }
    int cfb = getClosestFixedBits(patchBitSize + pgw);
    readLongs(unpackedPatch.data(), 0, pl, cfb);
    // any remaining bits are thrown out
    bitsLeft = 0;

    // apply the patch directly when decoding the packed data
    patchMask = ((1L << patchBitSize) - 1);

    adjustGapAndPatch();
  }

  unsigned long nRead = std::min(runLength - runRead, numValues);

  for(unsigned long pos = offset; pos < offset + nRead; ++pos, ++unpackedIdx) {
    if (static_cast<long>(unpackedIdx) != actualGap) {
      // no patching required. add base to unpacked value to get final value
      data[pos] = base + unpacked[unpackedIdx];
    } else {
      // extract the patch value
      long patchedVal = unpacked[unpackedIdx] | (curPatch << bitSize);

      // add base to patched value
      data[pos] = base + patchedVal;

      // increment the patch to point to next entry in patch list
      ++patchIdx;

      if (patchIdx < unpackedPatch.size()) {
        adjustGapAndPatch();

        // next gap is relative to the current gap
        actualGap += unpackedIdx;
      }
    }
  }

  runRead += nRead;
  return nRead;
}

unsigned long RleDecoderV2::nextDelta(long* const data,
                                      unsigned long offset,
                                      unsigned long numValues,
                                      const char* const notNull) {

  if (runRead == runLength) {
    // extract the number of fixed bits
    unsigned char fbo = (firstByte >> 1) & 0x1f;
    if (fbo != 0) {
      bitSize = decodeBitWidth(fbo);
    }

    // extract the run length
    runLength = (firstByte & 0x01) << 8;
    runLength |= readByte();
    ++runLength; // account for first value
    runRead = deltaBase = 0;

    // read the first value stored as vint
    if (isSigned) {
      firstValue = static_cast<long>(readVslong());
    } else {
      firstValue = static_cast<long>(readVulong());
    }

    data[offset++] = prevValue = firstValue;
  }

  unsigned long nRead = std::min(runLength - runRead, numValues);
  unsigned long remaining = runRead == 0 ? nRead - 1 : nRead;

  if (remaining == 0) {
    ; // already read the first value
  } else if (bitSize == 0) {
    if (runRead < 2) {
      // read the fixed delta value stored as vint (deltas can be negative even
      // if all number are positive)
      deltaBase = static_cast<long>(readVslong());
    }

    // add fixed deltas to adjacent values
    for (unsigned long pos = offset; pos < offset + remaining; ++pos) {
      prevValue = data[pos] = prevValue + deltaBase;
    }
  } else {
    if (runRead < 2) {
      deltaBase = static_cast<long>(readVslong());
      // add delta base and first value
      prevValue = data[offset++] = firstValue + deltaBase;
      --remaining;
    }

    // write the unpacked values, add it to previous value and store final
    // value to result buffer. if the delta base value is negative then it
    // is a decreasing sequence else an increasing sequence
    readLongs(data, offset, remaining, bitSize);
    if (deltaBase < 0) {
      for (unsigned long pos = offset; pos < offset + remaining; ++pos) {
        prevValue = data[pos] = prevValue - data[pos];
      }
    } else {
      for (unsigned long pos = offset; pos < offset + remaining; ++pos) {
        prevValue = data[pos] = prevValue + data[pos];
      }
    }
  }

  runRead += nRead;
  return nRead;
}

}  // namespace orc
