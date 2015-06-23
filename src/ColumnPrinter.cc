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

#include "orc/ColumnPrinter.hh"

#include <limits>
#include <sstream>
#include <stdexcept>
#include <time.h>
#include <typeinfo>

namespace orc {

  class BooleanColumnPrinter: public ColumnPrinter {
  private:
    const int64_t* data;
  public:
    BooleanColumnPrinter(std::ostream&, const Type&);
    ~BooleanColumnPrinter() {}
    void printRow(unsigned long rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  class LongColumnPrinter: public ColumnPrinter {
  private:
    const int64_t* data;
  public:
    LongColumnPrinter(std::ostream&, const Type&);
    ~LongColumnPrinter() {}
    void printRow(unsigned long rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  class DoubleColumnPrinter: public ColumnPrinter {
  private:
    const double* data;
    const int32_t digits;

  public:
    DoubleColumnPrinter(std::ostream&, const Type&);
    virtual ~DoubleColumnPrinter() {}
    void printRow(unsigned long rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  class TimestampColumnPrinter: public ColumnPrinter {
  private:
    const int64_t* data;
    time_t epoch;

  public:
    TimestampColumnPrinter(std::ostream&, const Type&);
    ~TimestampColumnPrinter() {}
    void printRow(unsigned long rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  class DateColumnPrinter: public ColumnPrinter {
  private:
    const int64_t* data;

  public:
    DateColumnPrinter(std::ostream&, const Type& type);
    ~DateColumnPrinter() {}
    void printRow(unsigned long rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  class Decimal64ColumnPrinter: public ColumnPrinter {
  private:
    const int64_t* data;
    int32_t scale;
  public:
    Decimal64ColumnPrinter(std::ostream&, const Type& type);
    ~Decimal64ColumnPrinter() {}
    void printRow(unsigned long rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  class Decimal128ColumnPrinter: public ColumnPrinter {
  private:
    const Int128* data;
    int32_t scale;
  public:
    Decimal128ColumnPrinter(std::ostream&, const Type& type);
    ~Decimal128ColumnPrinter() {}
    void printRow(unsigned long rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  class StringColumnPrinter: public ColumnPrinter {
  private:
    const char* const * start;
    const int64_t* length;
  public:
    StringColumnPrinter(std::ostream&, const Type& type);
    virtual ~StringColumnPrinter() {}
    void printRow(unsigned long rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  class BinaryColumnPrinter: public ColumnPrinter {
  private:
    const char* const * start;
    const int64_t* length;
  public:
    BinaryColumnPrinter(std::ostream&, const Type& type);
    virtual ~BinaryColumnPrinter() {}
    void printRow(unsigned long rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  class ListColumnPrinter: public ColumnPrinter {
  private:
    const int64_t* offsets;
    std::unique_ptr<ColumnPrinter> elementPrinter;

  public:
    ListColumnPrinter(std::ostream&, const Type& type);
    virtual ~ListColumnPrinter() {}
    void printRow(unsigned long rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  class MapColumnPrinter: public ColumnPrinter {
  private:
    const int64_t* offsets;
    std::unique_ptr<ColumnPrinter> keyPrinter;
    std::unique_ptr<ColumnPrinter> elementPrinter;

  public:
    MapColumnPrinter(std::ostream&, const Type& type);
    virtual ~MapColumnPrinter() {}
    void printRow(unsigned long rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  class StructColumnPrinter: public ColumnPrinter {
  private:
    std::vector<ColumnPrinter*> fieldPrinter;
  public:
    StructColumnPrinter(std::ostream&, const Type& type);
    virtual ~StructColumnPrinter();
    void printRow(unsigned long rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  ColumnPrinter::ColumnPrinter(std::ostream& _stream, const Type& _type
                               ): stream(_stream),
                                  type(_type) {
    notNull = nullptr;
    hasNulls = false;
  }

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

  std::unique_ptr<ColumnPrinter> createColumnPrinter(std::ostream& stream,
                                                     const Type& type) {
    ColumnPrinter *result;
    switch(static_cast<int>(type.getKind())) {
    case BOOLEAN:
      result = new BooleanColumnPrinter(stream, type);
      break;

    case BYTE:
    case SHORT:
    case INT:
    case LONG:
      result = new LongColumnPrinter(stream, type);
      break;

    case FLOAT:
    case DOUBLE:
      result = new DoubleColumnPrinter(stream, type);
      break;

    case STRING:
    case VARCHAR :
    case CHAR:
      result = new StringColumnPrinter(stream, type);
      break;

    case BINARY:
      result = new BinaryColumnPrinter(stream, type);
      break;

    case TIMESTAMP:
      result = new TimestampColumnPrinter(stream, type);
      break;

    case LIST:
      result = new ListColumnPrinter(stream, type);
      break;

    case MAP:
      result = new MapColumnPrinter(stream, type);
      break;

    case STRUCT:
      result = new StructColumnPrinter(stream, type);
      break;

    case DECIMAL:
      if (type.getPrecision() == 0 || type.getPrecision() > 18) {
        result = new Decimal128ColumnPrinter(stream, type);
      } else {
        result = new Decimal64ColumnPrinter(stream, type);
      }
      break;

    case DATE:
      result = new DateColumnPrinter(stream, type);
      break;

    default:
    case UNION:
      throw std::logic_error("unknown batch type");
    }
    return std::unique_ptr<ColumnPrinter>(result);
  }

  LongColumnPrinter::LongColumnPrinter(std::ostream& stream,
                                       const Type& type
                                       ): ColumnPrinter(stream, type) {
    // pass
  }

  void LongColumnPrinter::reset(const  ColumnVectorBatch& batch) {
    ColumnPrinter::reset(batch);
    data = dynamic_cast<const LongVectorBatch&>(batch).data.data();
  }

  void LongColumnPrinter::printRow(unsigned long rowId) {
    if (hasNulls && !notNull[rowId]) {
      stream << "null";
    } else {
      stream << data[rowId];
    }
  }

  DoubleColumnPrinter::DoubleColumnPrinter(std::ostream& stream,
                                           const Type& type
                                           ): ColumnPrinter(stream, type),
                                              digits(type.getKind() == FLOAT
                                                     ? 7 : 15) {
    // PASS
  }

  void DoubleColumnPrinter::reset(const  ColumnVectorBatch& batch) {
    ColumnPrinter::reset(batch);
    data = dynamic_cast<const DoubleVectorBatch&>(batch).data.data();
  }

  void DoubleColumnPrinter::printRow(unsigned long rowId) {
    if (hasNulls && !notNull[rowId]) {
      stream << "null";
    } else {
      std::streamsize oldPrecision = stream.precision(digits);
      stream << data[rowId];
      stream.precision(oldPrecision);
    }
  }

  Decimal64ColumnPrinter::Decimal64ColumnPrinter(std::ostream& stream,
                                                 const  Type& type
                                                 ): ColumnPrinter(stream,
                                                                  type) {
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
      stream << "null";
    } else {
      stream << toDecimalString(data[rowId], scale);
    }
  }

  Decimal128ColumnPrinter::Decimal128ColumnPrinter(std::ostream& stream,
                                                   const Type& type
                                                   ): ColumnPrinter(stream,
                                                                    type) {
     // PASS
   }

   void Decimal128ColumnPrinter::reset(const  ColumnVectorBatch& batch) {
     ColumnPrinter::reset(batch);
     data = dynamic_cast<const Decimal128VectorBatch&>(batch).values.data();
     scale =dynamic_cast<const Decimal128VectorBatch&>(batch).scale;
   }

   void Decimal128ColumnPrinter::printRow(unsigned long rowId) {
     if (hasNulls && !notNull[rowId]) {
       stream << "null";
     } else {
       stream << data[rowId].toDecimalString(scale);
     }
   }

  StringColumnPrinter::StringColumnPrinter(std::ostream& stream,
                                           const Type& type
                                           ): ColumnPrinter(stream, type) {
    // PASS
  }

  void StringColumnPrinter::reset(const ColumnVectorBatch& batch) {
    ColumnPrinter::reset(batch);
    start = dynamic_cast<const StringVectorBatch&>(batch).data.data();
    length = dynamic_cast<const StringVectorBatch&>(batch).length.data();
  }

  void StringColumnPrinter::printRow(unsigned long rowId) {
    if (hasNulls && !notNull[rowId]) {
      stream << "null";
    } else {
      stream << '"';
      for(int i=0; i < length[rowId]; ++i) {
        char ch = static_cast<char>(start[rowId][i]);
        switch (ch) {
        case '\\':
          stream << "\\\\";
          break;
        case '\b':
          stream << "\\b";
          break;
        case '\f':
          stream << "\\f";
          break;
        case '\n':
          stream << "\\n";
          break;
        case '\r':
          stream << "\\r";
          break;
        case '\t':
          stream << "\\t";
          break;
        case '"':
          stream << "\\\"";
          break;
        default:
          stream << ch;
          break;
        }
      }
      stream << '"';
    }
  }

  ListColumnPrinter::ListColumnPrinter(std::ostream& stream,
                                       const Type& type
                                       ): ColumnPrinter(stream, type) {
    elementPrinter = createColumnPrinter(stream, type.getSubtype(0));
  }

  void ListColumnPrinter::reset(const  ColumnVectorBatch& batch) {
    ColumnPrinter::reset(batch);
    offsets = dynamic_cast<const ListVectorBatch&>(batch).offsets.data();
    elementPrinter->reset(*dynamic_cast<const ListVectorBatch&>(batch).
                          elements);
  }

  void ListColumnPrinter::printRow(unsigned long rowId) {
    if (hasNulls && !notNull[rowId]) {
      stream << "null";
    } else {
      stream << "[";
      for(int64_t i=offsets[rowId]; i < offsets[rowId+1]; ++i) {
        if (i != offsets[rowId]) {
          stream << ", ";
        }
        elementPrinter->printRow(static_cast<unsigned long>(i));
      }
      stream << "]";
    }
  }

  MapColumnPrinter::MapColumnPrinter(std::ostream& stream,
                                     const Type& type
                                     ): ColumnPrinter(stream, type) {
    keyPrinter = createColumnPrinter(stream, type.getSubtype(0));
    elementPrinter = createColumnPrinter(stream, type.getSubtype(1));
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
      stream << "null";
    } else {
      stream << "[";
      for(int64_t i=offsets[rowId]; i < offsets[rowId+1]; ++i) {
        if (i != offsets[rowId]) {
          stream << ", ";
        }
        stream << "{\"key\": ";
        keyPrinter->printRow(static_cast<unsigned long>(i));
        stream << ", \"value\": ";
        elementPrinter->printRow(static_cast<unsigned long>(i));
        stream << "}";
      }
      stream << "]";
    }
  }

  StructColumnPrinter::StructColumnPrinter(std::ostream& stream,
                                           const Type& type
                                           ): ColumnPrinter(stream, type) {
    for(unsigned i=0; i < type.getSubtypeCount(); ++i) {
      fieldPrinter.push_back(createColumnPrinter(stream, type.getSubtype(i))
                             .release());
    }
  }

  StructColumnPrinter::~StructColumnPrinter() {
    for (size_t i = 0; i < fieldPrinter.size(); i++) {
      delete fieldPrinter[i];
    }
  }

  void StructColumnPrinter::reset(const ColumnVectorBatch& batch) {
    ColumnPrinter::reset(batch);
    const StructVectorBatch& structBatch =
      dynamic_cast<const StructVectorBatch&>(batch);
    for(size_t i=0; i < fieldPrinter.size(); ++i) {
      fieldPrinter[i]->reset(*(structBatch.fields[i]));
    }
  }

  void StructColumnPrinter::printRow(unsigned long rowId) {
    if (hasNulls && !notNull[rowId]) {
      stream << "null";
    } else {
      stream << "{";
      for(unsigned i=0; i < fieldPrinter.size(); ++i) {
        if (i != 0) {
          stream << ", ";
        }
        stream << '"' << type.getFieldName(i) << '"' << ": ";
        fieldPrinter[i]->printRow(rowId);
      }
      stream << "}";
    }
  }

  DateColumnPrinter::DateColumnPrinter(std::ostream& stream,
                                       const Type& type
                                       ): ColumnPrinter(stream, type) {
    // PASS
  }

  void DateColumnPrinter::printRow(unsigned long rowId) {
    if (hasNulls && !notNull[rowId]) {
      stream << "null";
    } else {
      const time_t timeValue = data[rowId] * 24 * 60 * 60;
      struct tm tmValue;
      gmtime_r(&timeValue, &tmValue);
      char buffer[11];
      strftime(buffer, sizeof(buffer), "%Y-%m-%d", &tmValue);
      stream << '"' << buffer << '"';
    }
  }

  void DateColumnPrinter::reset(const ColumnVectorBatch& batch) {
    ColumnPrinter::reset(batch);
    data = dynamic_cast<const LongVectorBatch&>(batch).data.data();
  }

  BooleanColumnPrinter::BooleanColumnPrinter(std::ostream& stream,
                                             const Type& type
                                             ): ColumnPrinter(stream, type) {
    // PASS
  }

  void BooleanColumnPrinter::printRow(unsigned long rowId) {
    if (hasNulls && !notNull[rowId]) {
      stream << "null";
    } else {
      stream << (data[rowId] ? "true" : "false");
    }
  }

  void BooleanColumnPrinter::reset(const ColumnVectorBatch& batch) {
    ColumnPrinter::reset(batch);
    data = dynamic_cast<const LongVectorBatch&>(batch).data.data();
  }

  BinaryColumnPrinter::BinaryColumnPrinter(std::ostream& stream,
                                           const Type& type
                                           ): ColumnPrinter(stream, type) {
    // PASS
  }

  void BinaryColumnPrinter::printRow(unsigned long rowId) {
    if (hasNulls && !notNull[rowId]) {
      stream << "null";
    } else {
      stream << "[";
      for(int64_t i=0; i < length[rowId]; ++i) {
        if (i != 0) {
          stream << ", ";
        }
        stream << (static_cast<const int>(start[rowId][i]) & 0xff);
      }
      stream << "]";
    }
  }

  void BinaryColumnPrinter::reset(const ColumnVectorBatch& batch) {
    ColumnPrinter::reset(batch);
    start = dynamic_cast<const StringVectorBatch&>(batch).data.data();
    length = dynamic_cast<const StringVectorBatch&>(batch).length.data();
  }

  TimestampColumnPrinter::TimestampColumnPrinter(std::ostream& stream,
                                                 const Type& type
                                                 ): ColumnPrinter(stream,
                                                                  type) {
    struct tm epochTm;
    epochTm.tm_sec = 0;
    epochTm.tm_min = 0;
    epochTm.tm_hour = 0;
    epochTm.tm_mday = 1;
    epochTm.tm_mon = 0;
    epochTm.tm_year = 70;
    epochTm.tm_isdst = 0;
    epoch = mktime(&epochTm);
  }

  void TimestampColumnPrinter::printRow(unsigned long rowId) {
    const int64_t NANOS_PER_SECOND = 1000000000;
    const int64_t NANO_DIGITS = 9;
    if (hasNulls && !notNull[rowId]) {
      stream << "null";
    } else {
      int64_t nanos = data[rowId] % NANOS_PER_SECOND;
      time_t seconds =
        static_cast<time_t>(data[rowId] / NANOS_PER_SECOND) + epoch;
      // make sure the nanos are positive
      if (nanos < 0) {
        nanos += NANOS_PER_SECOND;
        seconds -= 1;
      }
      struct tm tmValue;
      localtime_r(&seconds, &tmValue);
      char buffer[20];
      strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", &tmValue);
      stream << '"' << buffer << ".";
      // remove trailing zeros off the back of the nanos value.
      int zeroDigits = 0;
      if (nanos == 0) {
        zeroDigits = 8;
      } else {
        while (nanos % 10 == 0) {
          nanos /= 10;
          zeroDigits += 1;
        }
      }
      char oldFill = stream.fill('0');
      std::streamsize oldWidth = stream.width(NANO_DIGITS - zeroDigits);
      stream << nanos << '"';
      stream.fill(oldFill);
      stream.width(oldWidth);
    }
  }

  void TimestampColumnPrinter::reset(const ColumnVectorBatch& batch) {
    ColumnPrinter::reset(batch);
    data = dynamic_cast<const LongVectorBatch&>(batch).data.data();
  }
}
