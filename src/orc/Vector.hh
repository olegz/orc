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

#ifndef ORC_VECTOR_HH
#define ORC_VECTOR_HH

#include <array>
#include <initializer_list>
#include <list>
#include <memory>
#include <string>

namespace orc {

  enum TypeKind {
    BOOLEAN = 0,
    BYTE = 1,
    SHORT = 2,
    INT = 3,
    LONG = 4,
    FLOAT = 5,
    DOUBLE = 6,
    STRING = 7,
    BINARY = 8,
    TIMESTAMP = 9,
    LIST = 10,
    MAP = 11,
    STRUCT = 12,
    UNION = 13,
    DECIMAL = 14,
    DATE = 15,
    VARCHAR = 16,
    CHAR = 17
  };

  class Type {
  public:
    virtual ~Type();
    virtual int assignIds(int root) = 0;
    virtual int getColumnId() const = 0;
    virtual TypeKind getKind() const = 0;
    virtual unsigned int getSubtypeCount() const = 0;
    virtual const Type& getSubtype(unsigned int typeId) const = 0;
    virtual const std::string& getFieldName(unsigned int fieldId) const = 0;
    virtual unsigned int getMaximumLength() const = 0;
    virtual unsigned int getPrecision() const = 0;
    virtual unsigned int getScale() const = 0;
  };

  const int DEFAULT_DECIMAL_SCALE = 18;
  const int DEFAULT_DECIMAL_PRECISION = 38;

  std::unique_ptr<Type> createPrimitiveType(TypeKind kind);
  std::unique_ptr<Type> createCharType(bool isVarchar,
                                       int maxLength);
  std::unique_ptr<Type> 
                createDecimalType(int precision=DEFAULT_DECIMAL_PRECISION,
                                  int scale=DEFAULT_DECIMAL_SCALE);
  std::unique_ptr<Type> 
    createStructType(std::initializer_list<std::unique_ptr<Type> > types,
                      std::initializer_list<std::string> fieldNames);
  std::unique_ptr<Type> createListType(std::unique_ptr<Type> elements);
  std::unique_ptr<Type> createMapType(std::unique_ptr<Type> key,
                                      std::unique_ptr<Type> value);
  std::unique_ptr<Type> 
    createUnionType(std::initializer_list<std::unique_ptr<Type> > types);

  struct ColumnVectorBatch {
    ColumnVectorBatch(unsigned long capacity);
    virtual ~ColumnVectorBatch();

    // the number of slots available
    unsigned long capacity;
    // the number of current occupied slots
    unsigned long numElements;
    // an array of capacity length marking non-null values
    std::unique_ptr<char[]> notNull;
    // whether there are any null values
    bool hasNulls;

    virtual std::string toString() const = 0;
  };

  struct LongVectorBatch: public ColumnVectorBatch {
    LongVectorBatch(unsigned long capacity);
    virtual ~LongVectorBatch();
    std::unique_ptr<long[]> data;
    std::string toString() const;
  };

  struct DoubleVectorBatch: public ColumnVectorBatch {
    DoubleVectorBatch(unsigned long capacity);
    virtual ~DoubleVectorBatch();
    std::string toString() const;

    std::unique_ptr<double[]> data;
  };

  struct StringVectorBatch: public ColumnVectorBatch {
    StringVectorBatch(unsigned long capacity);
    virtual ~StringVectorBatch();
    std::string toString() const;

    std::unique_ptr<char*([])> data;
    std::unique_ptr<long[]> length;
  };

  struct StructVectorBatch: public ColumnVectorBatch {
    StructVectorBatch(unsigned long capacity);
    virtual ~StructVectorBatch();
    std::string toString() const;

    unsigned long numFields;
    std::unique_ptr<std::unique_ptr<ColumnVectorBatch>[]> fields;
  };

  struct Decimal {
    long upper;
    long lower;
  };
}

#endif
