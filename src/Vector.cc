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

#include "Exceptions.hh"
#include "orc/Vector.hh"

#include <iostream>
#include <sstream>

namespace orc {

  ColumnVectorBatch::ColumnVectorBatch(uint64_t cap
                                       ): notNull(cap) {
    capacity = cap;
    numElements = 0;
    hasNulls = false;
  }

  ColumnVectorBatch::~ColumnVectorBatch() {
    // PASS
  }

  void ColumnVectorBatch::resize(uint64_t cap) {
    if (capacity < cap) {
      capacity = cap;
      notNull.resize(cap);
    }
  }

  ColumnVectorBatch::ColumnVectorBatch(const ColumnVectorBatch&) {
    throw NotImplementedYet("should not call");
  }

  ColumnVectorBatch& ColumnVectorBatch::operator=(const ColumnVectorBatch&) {
    throw NotImplementedYet("should not call");
  }

  LongVectorBatch::LongVectorBatch(uint64_t capacity
                                   ): ColumnVectorBatch(capacity),
                                      data(capacity) {
    // PASS
  }

  LongVectorBatch::~LongVectorBatch() {
    // PASS
  }
  
  std::string LongVectorBatch::toString() const {
    std::ostringstream buffer;
    buffer << "Long vector <" << numElements << " of " << capacity << ">";
    return buffer.str();
  }

  void LongVectorBatch::resize(uint64_t cap) {
    if (capacity < cap) {
      ColumnVectorBatch::resize(cap);
      data.resize(cap);
    }
  }

  DoubleVectorBatch::DoubleVectorBatch(uint64_t capacity
                                       ): ColumnVectorBatch(capacity),
                                          data(capacity) {
    // PASS
  }

  DoubleVectorBatch::~DoubleVectorBatch() {
    // PASS
  }

  std::string DoubleVectorBatch::toString() const {
    std::ostringstream buffer;
    buffer << "Double vector <" << numElements << " of " << capacity << ">";
    return buffer.str();
  }

  void DoubleVectorBatch::resize(uint64_t cap) {
    if (capacity < cap) {
      ColumnVectorBatch::resize(cap);
      data.resize(cap);
    }
  }

  StringVectorBatch::StringVectorBatch(uint64_t capacity
                                       ): ColumnVectorBatch(capacity),
                                          data(capacity),
                                          length(capacity) {
    // PASS
  }

  StringVectorBatch::~StringVectorBatch() {
    // PASS
  }

  std::string StringVectorBatch::toString() const {
    std::ostringstream buffer;
    buffer << "Byte vector <" << numElements << " of " << capacity << ">";
    return buffer.str();
  }

  void StringVectorBatch::resize(uint64_t cap) {
    if (capacity < cap) {
      ColumnVectorBatch::resize(cap);
      data.resize(cap);
      length.resize(cap);
    }
  }

  StructVectorBatch::StructVectorBatch(uint64_t capacity
                                       ): ColumnVectorBatch(capacity) {
    // PASS
  }

  StructVectorBatch::~StructVectorBatch() {
    // PASS
  }

  std::string StructVectorBatch::toString() const {
    std::ostringstream buffer;
    buffer << "Struct vector <" << numElements << " of " << capacity 
           << "; ";
    for(auto ptr=fields.cbegin(); ptr != fields.cend(); ++ptr) {
      buffer << ptr->get()->toString() << "; ";
    }
    buffer << ">";
    return buffer.str();
  }


  void StructVectorBatch::resize(uint64_t cap) {
    ColumnVectorBatch::resize(cap);
  }

  ListVectorBatch::ListVectorBatch(uint64_t cap
                                   ): ColumnVectorBatch(cap),
                                      offsets(cap+1) {
    // PASS
  }

  ListVectorBatch::~ListVectorBatch() {
    // PASS
  }

  std::string ListVectorBatch::toString() const {
    std::ostringstream buffer;
    buffer << "List vector <" << elements->toString() << " with "
           << numElements << " of " << capacity << ">";
    return buffer.str();
  }

  void ListVectorBatch::resize(uint64_t cap) {
    if (capacity < cap) {
      ColumnVectorBatch::resize(cap);
      offsets.resize(cap + 1);
    }
  }

  MapVectorBatch::MapVectorBatch(uint64_t cap
                                 ): ColumnVectorBatch(cap),
                                    offsets(cap+1) {
    // PASS
  }

  MapVectorBatch::~MapVectorBatch() {
    // PASS
  }

  std::string MapVectorBatch::toString() const {
    std::ostringstream buffer;
    buffer << "Map vector <" << keys->toString() << ", "
           << elements->toString() << " with "
           << numElements << " of " << capacity << ">";
    return buffer.str();
  }

  void MapVectorBatch::resize(uint64_t cap) {
    if (capacity < cap) {
      ColumnVectorBatch::resize(cap);
      offsets.resize(cap + 1);
    }
  }
}
