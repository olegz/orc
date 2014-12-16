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
#include "TypeImpl.hh"

namespace orc {

  Type::~Type() {
    // PASS
  }

  TypeImpl::TypeImpl(TypeKind _kind) {
    columnId = 0;
    kind = _kind;
    maxLength = 0;
    precision = 0;
    scale = 0;
    subtypeCount = 0;
  }

  TypeImpl::TypeImpl(TypeKind _kind, unsigned int _maxLength) {
    columnId = 0;
    kind = _kind;
    maxLength = _maxLength;
    precision = 0;
    scale = 0;
    subtypeCount = 0;
  }

  TypeImpl::TypeImpl(TypeKind _kind, unsigned int _precision,
                     unsigned int _scale) {
    columnId = 0;
    kind = _kind;
    maxLength = 0;
    precision = _precision;
    scale = _scale;
    subtypeCount = 0;
  }

  TypeImpl::TypeImpl(TypeKind _kind, std::vector<Type*> types,
                     std::vector<std::string> _fieldNames) {
    columnId = 0;
    kind = _kind;
    maxLength = 0;
    precision = 0;
    scale = 0;
    subtypeCount = static_cast<unsigned int>(types.size());
    subTypes.reset(new std::unique_ptr<Type>[subtypeCount]);
    fieldNames.reset(new std::string[_fieldNames.size()]);
    for(unsigned int i=0; i < subtypeCount; ++i) {
      subTypes.get()[i].reset(types[i]);
      fieldNames.get()[i] = _fieldNames[i];
    }
  }

  TypeImpl::TypeImpl(TypeKind _kind, std::vector<Type*> types) {
    columnId = 0;
    kind = _kind;
    maxLength = 0;
    precision = 0;
    scale = 0;
    subtypeCount = static_cast<unsigned int>(types.size());
    subTypes.reset(new std::unique_ptr<Type>[subtypeCount]);
    for(unsigned int i=0; i < subtypeCount; ++i) {
      subTypes.get()[i].reset(types[static_cast<unsigned long>(i)]);
    }
  }

  int TypeImpl::assignIds(int root) {
    columnId = root;
    int current = root + 1;
    std::unique_ptr<Type> *children = subTypes.get();
    for(unsigned int i=0; i < subtypeCount; ++i) {
      current = children[i].get()->assignIds(current);
    }
    return current;
  }

  TypeImpl::~TypeImpl() {
    // PASS
  }

  int TypeImpl::getColumnId() const {
    return columnId;
  }

  TypeKind TypeImpl::getKind() const {
    return kind;
  }

  unsigned int TypeImpl::getSubtypeCount() const {
    return subtypeCount;
  }

  const Type& TypeImpl::getSubtype(unsigned int i) const {
    return *(subTypes.get()[i].get());
  }

  const std::string& TypeImpl::getFieldName(unsigned int i) const {
    return fieldNames.get()[i];
  }

  unsigned int TypeImpl::getMaximumLength() const {
    return maxLength;
  }

  unsigned int TypeImpl::getPrecision() const {
    return precision;
  }

  unsigned int TypeImpl::getScale() const {
    return scale;
  }

  std::unique_ptr<Type> convertType(const proto::Type& type,
                                    const proto::Footer& footer) {
    switch (type.kind()) {

    case proto::Type_Kind_BOOLEAN:
    case proto::Type_Kind_BYTE:
    case proto::Type_Kind_SHORT:
    case proto::Type_Kind_INT:
    case proto::Type_Kind_LONG:
    case proto::Type_Kind_FLOAT:
    case proto::Type_Kind_DOUBLE:
    case proto::Type_Kind_STRING:
    case proto::Type_Kind_BINARY:
    case proto::Type_Kind_TIMESTAMP:
    case proto::Type_Kind_DATE:
      return std::unique_ptr<Type>
        (new TypeImpl(static_cast<TypeKind>(type.kind())));

    case proto::Type_Kind_CHAR:
    case proto::Type_Kind_VARCHAR:
      return std::unique_ptr<Type>
        (new TypeImpl(static_cast<TypeKind>(type.kind()),
                      type.maximumlength()));

    case proto::Type_Kind_DECIMAL:
      return std::unique_ptr<Type>
        (new TypeImpl(DECIMAL, type.precision(), type.scale()));

    case proto::Type_Kind_LIST:
    case proto::Type_Kind_MAP:
    case proto::Type_Kind_UNION: {
      unsigned long size = static_cast<unsigned long>(type.subtypes_size());
      std::vector<Type*> typeList(size);
      for(int i=0; i < type.subtypes_size(); ++i) {
        typeList[static_cast<unsigned int>(i)] = 
          convertType(footer.types(static_cast<int>(type.subtypes(i))),
                      footer).release();
      }
      return std::unique_ptr<Type>
        (new TypeImpl(static_cast<TypeKind>(type.kind()), typeList));
    }

    case proto::Type_Kind_STRUCT: {
      unsigned long size = static_cast<unsigned long>(type.subtypes_size());
      std::vector<Type*> typeList(size);
      std::vector<std::string> fieldList(size);
      for(int i=0; i < type.subtypes_size(); ++i) {
        typeList[static_cast<unsigned int>(i)] = 
          convertType(footer.types(static_cast<int>(type.subtypes(i))),
                      footer).release();
        fieldList[static_cast<unsigned int>(i)] = type.fieldnames(i);
      }
      return std::unique_ptr<Type>
        (new TypeImpl(STRUCT, typeList, fieldList));
    }
    }
    throw NotImplementedYet("Unknown type kind");
  }

}
