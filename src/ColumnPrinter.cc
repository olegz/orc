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

#include "ColumnPrinter.hh"
#include <typeinfo>
#include <sstream>
#include <stdexcept>

namespace orc {

  class LongColumnPrinter: public ColumnPrinter {
  private:
    const int64_t* data;
  public:
    LongColumnPrinter(const ColumnVectorBatch& batch);
    ~LongColumnPrinter() {}
    void printRow(unsigned long rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  class DoubleColumnPrinter: public ColumnPrinter {
  private:
    const double* data;
  public:
    DoubleColumnPrinter(const ColumnVectorBatch& batch);
    virtual ~DoubleColumnPrinter() {}
    void printRow(unsigned long rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  class Decimal64ColumnPrinter: public ColumnPrinter {
  private:
    const int64_t* data;
    int32_t scale;
  public:
    Decimal64ColumnPrinter(const ColumnVectorBatch& batch);
    ~Decimal64ColumnPrinter() {}
    void printRow(unsigned long rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  class Decimal128ColumnPrinter: public ColumnPrinter {
  private:
    const Int128* data;
    int32_t scale;
  public:
    Decimal128ColumnPrinter(const ColumnVectorBatch& batch);
    ~Decimal128ColumnPrinter() {}
    void printRow(unsigned long rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  class StringColumnPrinter: public ColumnPrinter {
  private:
    const char* const * start;
    const int64_t* length;
  public:
    StringColumnPrinter(const ColumnVectorBatch& batch);
    virtual ~StringColumnPrinter() {}
    void printRow(unsigned long rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  class ListColumnPrinter: public ColumnPrinter {
  private:
    const int64_t* offsets;
    std::unique_ptr<ColumnPrinter> elementPrinter;

  public:
    ListColumnPrinter(const ColumnVectorBatch& batch);
    virtual ~ListColumnPrinter();
    void printRow(unsigned long rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  class MapColumnPrinter: public ColumnPrinter {
  private:
    const int64_t* offsets;
    std::unique_ptr<ColumnPrinter> keyPrinter;
    std::unique_ptr<ColumnPrinter> elementPrinter;

  public:
    MapColumnPrinter(const ColumnVectorBatch& batch);
    virtual ~MapColumnPrinter();
    void printRow(unsigned long rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  class StructColumnPrinter: public ColumnPrinter {
  private:
    std::vector<ColumnPrinter*> fields;
  public:
    StructColumnPrinter(const ColumnVectorBatch& batch);
    virtual ~StructColumnPrinter();
    void printRow(unsigned long rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  ColumnPrinter::~ColumnPrinter() {
    // PASS
  }

  void ColumnPrinter::reset(const ColumnVectorBatch& batch) {
    hasNulls = batch.hasNulls;
    if (hasNulls) {
      notNull = batch.notNull.data();
    } else {
      notNull = nullptr ;
    }
  }

  std::unique_ptr<ColumnPrinter> createColumnPrinter
                                             (const ColumnVectorBatch& batch) {
    ColumnPrinter *result;
    if (typeid(batch) == typeid(LongVectorBatch)) {
      result = new LongColumnPrinter(batch);
    } else if (typeid(batch) == typeid(DoubleVectorBatch)) {
      result = new DoubleColumnPrinter(batch);
    } else if (typeid(batch) == typeid(StringVectorBatch)) {
      result = new StringColumnPrinter(batch);
    } else if (typeid(batch) == typeid(Decimal128VectorBatch)) {
      result = new Decimal128ColumnPrinter(batch);
    } else if (typeid(batch) == typeid(Decimal64VectorBatch)) {
      result = new Decimal64ColumnPrinter(batch);
    } else if (typeid(batch) == typeid(StructVectorBatch)) {
      result = new StructColumnPrinter(batch);
    } else if (typeid(batch) == typeid(ListVectorBatch)) {
      result = new ListColumnPrinter(batch);
    } else if (typeid(batch) == typeid(MapVectorBatch)) {
      result = new MapColumnPrinter(batch);
    } else {
      throw std::logic_error("unknown batch type");
    }
    return std::unique_ptr<ColumnPrinter>(result);
  }

  LongColumnPrinter::LongColumnPrinter(const  ColumnVectorBatch&) {
    // pass
  }

  void LongColumnPrinter::reset(const  ColumnVectorBatch& batch) {
    ColumnPrinter::reset(batch);
    data = dynamic_cast<const LongVectorBatch&>(batch).data.data();
  }

  void LongColumnPrinter::printRow(unsigned long rowId) {
    if (hasNulls && !notNull[rowId]) {
      std::cout << "NULL";
    } else {
      std::cout << data[rowId];
    }
  }

  DoubleColumnPrinter::DoubleColumnPrinter(const  ColumnVectorBatch&) {
    // PASS
  }

  void DoubleColumnPrinter::reset(const  ColumnVectorBatch& batch) {
    ColumnPrinter::reset(batch);
    data = dynamic_cast<const DoubleVectorBatch&>(batch).data.data();
  }

  void DoubleColumnPrinter::printRow(unsigned long rowId) {
    if (hasNulls && !notNull[rowId]) {
      std::cout << "NULL";
    } else {
      std::cout << data[rowId];
    }
  }

  Decimal64ColumnPrinter::Decimal64ColumnPrinter(const  ColumnVectorBatch&) {
    // PASS
  }

  void Decimal64ColumnPrinter::reset(const  ColumnVectorBatch& batch) {
    ColumnPrinter::reset(batch);
    data = dynamic_cast<const Decimal64VectorBatch&>(batch).values.data();
    scale = dynamic_cast<const Decimal64VectorBatch&>(batch).scale;
  }

  std::string toDecimalString(int64_t value, int32_t scale) {
    std::stringstream buffer;
    if (scale == 0) {
      buffer << value;
      return buffer.str();
    }
    std::string sign = "";
    if (value < 0) {
      sign = "-";
      value = -value;
    }
    buffer << value;
    std::string str = buffer.str();
    int32_t len = static_cast<int32_t>(str.length());
    if (len > scale) {
      return sign + str.substr(0, static_cast<size_t>(len - scale)) + "." +
        str.substr(static_cast<size_t>(len - scale),
                   static_cast<size_t>(scale));
    } else if (len == scale) {
      return sign + "0." + str;
    } else {
      std::string result = sign + "0.";
      for(int32_t i=0; i < scale - len; ++i) {
        result += "0";
      }
      return result + str;
    }
  }

  void Decimal64ColumnPrinter::printRow(unsigned long rowId) {
    if (hasNulls && !notNull[rowId]) {
      std::cout << "NULL";
    } else {
      std::cout << toDecimalString(data[rowId], scale);
    }
  }

  Decimal128ColumnPrinter::Decimal128ColumnPrinter(const  ColumnVectorBatch&) {
     // PASS
   }

   void Decimal128ColumnPrinter::reset(const  ColumnVectorBatch& batch) {
     ColumnPrinter::reset(batch);
     data = dynamic_cast<const Decimal128VectorBatch&>(batch).values.data();
     scale =dynamic_cast<const Decimal128VectorBatch&>(batch).scale;
   }

   void Decimal128ColumnPrinter::printRow(unsigned long rowId) {
     if (hasNulls && !notNull[rowId]) {
       std::cout << "NULL";
     } else {
       std::cout << data[rowId].toDecimalString(scale);
     }
   }

  StringColumnPrinter::StringColumnPrinter(const ColumnVectorBatch&) {
    // PASS
  }

  void StringColumnPrinter::reset(const ColumnVectorBatch& batch) {
    ColumnPrinter::reset(batch);
    start = dynamic_cast<const StringVectorBatch&>(batch).data.data();
    length = dynamic_cast<const StringVectorBatch&>(batch).length.data();
  }

  void StringColumnPrinter::printRow(unsigned long rowId) {
    if (hasNulls && !notNull[rowId]) {
      std::cout << "NULL";
    } else {
      std::cout.write(start[rowId], length[rowId]);
    }
  }

  ListColumnPrinter::ListColumnPrinter(const  ColumnVectorBatch& batch) {
    elementPrinter = createColumnPrinter
      (*dynamic_cast<const ListVectorBatch&>(batch).elements);
  }

  ListColumnPrinter::~ListColumnPrinter() {
    // PASS
  }

  void ListColumnPrinter::reset(const  ColumnVectorBatch& batch) {
    ColumnPrinter::reset(batch);
    offsets = dynamic_cast<const ListVectorBatch&>(batch).offsets.data();
    elementPrinter->reset(*dynamic_cast<const ListVectorBatch&>(batch).
                          elements);
  }

  void ListColumnPrinter::printRow(unsigned long rowId) {
    if (hasNulls && !notNull[rowId]) {
      std::cout << "NULL";
    } else {
      std::cout << "[";
      for(int64_t i=offsets[rowId]; i < offsets[rowId+1]; ++i) {
        elementPrinter->printRow(static_cast<unsigned long>(i));
      }
      std::cout << "]";
    }
  }

  MapColumnPrinter::MapColumnPrinter(const  ColumnVectorBatch& batch) {
    const MapVectorBatch& myBatch = dynamic_cast<const MapVectorBatch&>(batch);
    keyPrinter = createColumnPrinter(*myBatch.keys);
    elementPrinter = createColumnPrinter(*myBatch.elements);
  }

  MapColumnPrinter::~MapColumnPrinter() {
    // PASS
  }

  void MapColumnPrinter::reset(const  ColumnVectorBatch& batch) {
    ColumnPrinter::reset(batch);
    const MapVectorBatch& myBatch = dynamic_cast<const MapVectorBatch&>(batch);
    offsets = myBatch.offsets.data();
    keyPrinter->reset(*myBatch.keys);
    elementPrinter->reset(*myBatch.elements);
  }

  void MapColumnPrinter::printRow(unsigned long rowId) {
    if (hasNulls && !notNull[rowId]) {
      std::cout << "NULL";
    } else {
      std::cout << "{";
      for(int64_t i=offsets[rowId]; i < offsets[rowId+1]; ++i) {
        keyPrinter->printRow(static_cast<unsigned long>(i));
        std::cout << "->";
        elementPrinter->printRow(static_cast<unsigned long>(i));
      }
      std::cout << "}";
    }
  }

  StructColumnPrinter::StructColumnPrinter(const ColumnVectorBatch& batch) {
    const StructVectorBatch& structBatch =
      dynamic_cast<const StructVectorBatch&>(batch);
    for(std::vector<ColumnVectorBatch*>::const_iterator ptr=
          structBatch.fields.begin();
        ptr != structBatch.fields.end(); ++ptr) {
      fields.push_back(createColumnPrinter(**ptr).release());
    }
  }

  StructColumnPrinter::~StructColumnPrinter() {
    for (size_t i = 0; i < fields.size(); i++) {
      delete fields[i];
    }
  }

  void StructColumnPrinter::reset(const ColumnVectorBatch& batch) {
    ColumnPrinter::reset(batch);
    const StructVectorBatch& structBatch =
      dynamic_cast<const StructVectorBatch&>(batch);
    for(size_t i=0; i < fields.size(); ++i) {
      fields[i]->reset(*(structBatch.fields[i]));
    }
  }

  void StructColumnPrinter::printRow(unsigned long rowId) {
    if (fields.size() > 0) {
      for (std::vector<ColumnPrinter*>::iterator ptr =
             fields.begin(); ptr != fields.end(); ++ptr) {
        (*ptr)->printRow(rowId);
        std::cout << "\t";
      }
      std::cout << "\n";
    }
  }
}
