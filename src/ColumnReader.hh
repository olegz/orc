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

#ifndef ORC_COLUMN_READER_HH
#define ORC_COLUMN_READER_HH

#include "orc/Vector.hh"
#include "ByteRLE.hh"
#include "Compression.hh"
#include "wrap/orc-proto-wrapper.hh"

namespace orc {

  class StripeStreams {
  public:
    virtual ~StripeStreams();

    /**
     * Get the array of booleans for which columns are selected.
     * @return the address of an array which contains true at the index of
     *    each columnId is selected.
     */
    virtual const bool* getSelectedColumns() const = 0;

    /**
     * Get the encoding for the given column for this stripe.
     */
    virtual proto::ColumnEncoding getEncoding(int columnId) const = 0;

    /**
     * Get the stream for the given column/kind in this stripe.
     */
    virtual std::unique_ptr<SeekableInputStream> 
                    getStream(int columnId,
                              proto::Stream_Kind kind) const = 0;
  };

  /**
   * The interface for reading ORC data types.
   */
  class ColumnReader {
  protected:
    std::unique_ptr<ByteRleDecoder> notNullDecoder;
    int columnId;

  public:
    ColumnReader(const Type& type, StripeStreams& stipe);

    virtual ~ColumnReader();

    /**
     * Skip number of specified rows.
     * @param numValues the number of values to skip
     * @return the number of non-null values skipped
     */
    virtual unsigned long skip(unsigned long numValues);

    /**
     * Read the next group of values into this rowBatch.
     * @param rowBatch the memory to read into.
     * @param numValues the number of values to read
     * @param notNull if null, all values are not null. Otherwise, it is 
     *           a mask (with at least numValues bytes) for which values to 
     *           set.
     */
    virtual void next(ColumnVectorBatch& rowBatch, 
                      unsigned long numValues,
                      char* notNull);
  };

  /**
   * Create a reader for the given stripe.
   */
  std::unique_ptr<ColumnReader> buildReader(const Type& type,
                                            StripeStreams& stripe);
}

#endif
