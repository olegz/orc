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

void RleDecoderV2::readInts(long *data, unsigned long offset, unsigned len) {
  // TODO: unroll to improve performance
  for(unsigned long i = offset; i < (offset + len); i++) {
      long result = 0;
      int bitsLeftToRead = bitSize;
      while (bitsLeftToRead > bitsLeft) {
        result <<= bitsLeft;
        result |= current & ((1 << bitsLeft) - 1);
        bitsLeftToRead -= bitsLeft;
        current = readByte();
        bitsLeft = 8;
      }

      // handle the left over bits
      if (bitsLeftToRead > 0) {
        result <<= bitsLeftToRead;
        bitsLeft -= bitsLeftToRead;
        result |= (current >> bitsLeft) & ((1 << bitsLeftToRead) - 1);
      }
      data[i] = result;
  }
}

signed char RleDecoderV2::readByte() {
  if (bufferStart == bufferEnd) {
    int bufferLength;
    const void* bufferPointer;
    if (!inputStream->Next(&bufferPointer, &bufferLength)) {
      throw ParseError("bad read in readByte");
    }
    bufferStart = static_cast<const char*>(bufferPointer);
    bufferEnd = bufferStart + bufferLength;
  }

  prevByte = *bufferStart;
  ++bufferStart;
  return prevByte;
}

long RleDecoderV2::readLongBE() {
  long ret = 0, val;
  int n = byteSize;
  while (n > 0) {
    n--;
    val = readByte();
    ret |= (val << (n * 8));
  }
  return ret;
}

RleDecoderV2::RleDecoderV2(std::unique_ptr<SeekableInputStream> input,
                           bool isSigned)
  : inputStream(std::move(input)),
    isSigned(isSigned),
    firstByte(0),
    prevByte(0),
    runLength(0),
    runRead(0),
    bufferStart(nullptr),
    bufferEnd(bufferStart),
    byteSize(0),
    value(0),
    bitSize(0),
    bitsLeft(0),
    current(0) {
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
        ((((unsigned char) firstByte) >> 6) & 0x03);
    switch(enc) {
    case SHORT_REPEAT:
      nRead += nextShortRepeats(data, offset, length, notNull);
      break;
    case DIRECT:
      nRead += nextDirect(data, offset, length, notNull);
      break;
    case PATCHED_BASE:
      throw ParseError("PATCHED_BASE encoding is not yet supported");
      break;
    case DELTA:
      throw ParseError("DELTA encoding is not yet supported");
      break;
    default:
      throw ParseError("unknown encoding");
    }
  }
}

unsigned long RleDecoderV2::nextShortRepeats(long* const data,
                                             const unsigned long offset,
                                             const unsigned long numValues,
                                             const char* const notNull) {
  if (runRead == runLength) {
    // extract the number of fixed bytes
    byteSize = ((static_cast<unsigned char>(firstByte)) >> 3) & 0x07;
    byteSize += 1;

    runLength = firstByte & 0x07;
    // run lengths values are stored only after MIN_REPEAT value is met
    runLength += MIN_REPEAT;
    runRead = 0;

    // read the repeated value which is store using fixed bytes
    value = readLongBE();

    if (isSigned) {
      value = unZigZag(value);
    }
  }

  unsigned long nRead = std::min(runLength - runRead, numValues);
  for(unsigned long pos = offset; pos < offset + nRead; ++pos) {
    data[pos] = value;
  }

  runRead += nRead;
  return nRead;
}

unsigned long RleDecoderV2::nextDirect(long* const data,
                                       const unsigned long offset,
                                       const unsigned long numValues,
                                       const char* const notNull) {
  if (runRead == runLength) {
    // extract the number of fixed bits
    unsigned char fbo = (((unsigned char) firstByte) >> 1) & 0x1f;
    bitSize = decodeBitWidth(fbo);
    bitsLeft = 0;
    current = 0;

    // extract the run length
    runLength = (firstByte & 0x01) << 8;
    runLength |= readByte();
    // runs are one off
    runLength += 1;
    runRead = 0;
  }

  unsigned long nRead = std::min(runLength - runRead, numValues);

  readInts(data, offset, nRead);
  if (isSigned) { 
    // write the unpacked values and zigzag decode to result buffer
    for(unsigned long pos = offset; pos < offset + nRead; ++pos) {
      data[pos] = unZigZag(data[pos]);
    }
  }

  runRead += nRead;
  return nRead;
}

}  // namespace orc
