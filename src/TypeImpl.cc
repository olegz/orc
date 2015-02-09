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

  TypeImpl::TypeImpl(TypeKind _kind,
                     const std::vector<Type*>& types,
                     const std::vector<std::string>& _fieldNames) {
    columnId = 0;
    kind = _kind;
    maxLength = 0;
    precision = 0;
    scale = 0;
    subtypeCount = static_cast<unsigned int>(types.size());
    subTypes.assign(types.begin(), types.end());
    fieldNames.assign(_fieldNames.begin(), _fieldNames.end());
  }

  TypeImpl::TypeImpl(TypeKind _kind, const std::vector<Type*>& types) {
    columnId = 0;
    kind = _kind;
    maxLength = 0;
    precision = 0;
    scale = 0;
    subtypeCount = static_cast<unsigned int>(types.size());
    subTypes.assign(types.begin(), types.end());
  }

  int TypeImpl::assignIds(int root) {
    columnId = root;
    int current = root + 1;
    for(unsigned int i=0; i < subtypeCount; ++i) {
      current = subTypes[i]->assignIds(current);
    }
    return current;
  }

  TypeImpl::~TypeImpl() {
    for (std::vector<Type*>::iterator it = subTypes.begin();
        it != subTypes.end(); it++) {
      delete (*it) ;
    }
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
    return *(subTypes[i]);
  }

  const std::string& TypeImpl::getFieldName(unsigned int i) const {
    return fieldNames[i];
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

  std::unique_ptr<Type> createPrimitiveType(TypeKind kind) {
    return std::unique_ptr<Type>(new TypeImpl(kind));
  }

  std::unique_ptr<Type> createCharType(TypeKind kind,
                                       unsigned int maxLength) {
    return std::unique_ptr<Type>(new TypeImpl(kind, maxLength));
  }

  std::unique_ptr<Type> createDecimalType(unsigned int precision,
                                          unsigned int scale) {
    return std::unique_ptr<Type>(new TypeImpl(DECIMAL, precision, scale));
  }

  std::unique_ptr<Type>
      createStructType(std::vector<Type*> types,
                       std::vector<std::string> fieldNames) {
    std::vector<Type*> typeVector(types.begin(), types.end());
    std::vector<std::string> fieldVector(fieldNames.begin(), fieldNames.end());

    return std::unique_ptr<Type>(new TypeImpl(STRUCT, typeVector,
                                              fieldVector));
  }

#if __cplusplus >= 201103L
  std::unique_ptr<Type> createStructType(
      std::initializer_list<std::unique_ptr<Type> > types,
      std::initializer_list<std::string> fieldNames) {
    std::vector<Type*> typeVector(types.size());
    std::vector<std::string> fieldVector(types.size());
    auto currentType = types.begin();
    auto endType = types.end();
    size_t current = 0;
    while (currentType != endType) {
      typeVector[current++] =
          const_cast<std::unique_ptr<Type>*>(currentType)->release();
      ++currentType;
    }
    fieldVector.insert(fieldVector.end(), fieldNames.begin(),
        fieldNames.end());
    return std::unique_ptr<Type>(new TypeImpl(STRUCT, typeVector,
        fieldVector));
  }
#endif // __cplusplus

  std::unique_ptr<Type> createListType(std::unique_ptr<Type> elements) {
    std::vector<Type*> subtypes(1);
    subtypes[0] = elements.release();
    return std::unique_ptr<Type>(new TypeImpl(LIST, subtypes));
  }

  std::unique_ptr<Type> createMapType(std::unique_ptr<Type> key,
                                      std::unique_ptr<Type> value) {
    std::vector<Type*> subtypes(2);
    subtypes[0] = key.release();
    subtypes[1] = value.release();
    return std::unique_ptr<Type>(new TypeImpl(MAP, subtypes));
  }

  std::unique_ptr<Type>
      createUnionType(std::vector<Type*> types) {
    std::vector<Type*> typeVector(types.begin(), types.end());
    return std::unique_ptr<Type>(new TypeImpl(UNION, typeVector));
  }

  std::unique_ptr<Type> convertType(const proto::Type& type,
                                    const proto::Footer& footer) {
    switch (static_cast<int>(type.kind())) {

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
    default:
      throw NotImplementedYet("Unknown type kind");
    }
  }

  std::string kind2String(TypeKind t) {
      std::string name ;
      switch(t) {
        case BOOLEAN: { name = "BOOLEAN"; break; }
        case BYTE: { name = "BYTE"; break; }
        case SHORT: { name = "SHORT"; break; }
        case INT: { name = "INT"; break; }
        case LONG: { name = "LONG"; break; }
        case FLOAT: { name = "FLOAT"; break; }
        case DOUBLE: { name = "DOUBLE"; break; }
        case STRING: { name = "STRING"; break; }
        case BINARY: { name = "BINARY"; break; }
        case TIMESTAMP: { name = "TIMESTAMP"; break; }
        case LIST: { name = "LIST"; break; }
        case MAP: { name = "MAP"; break; }
        case STRUCT: { name = "STRUCT"; break; }
        case UNION: { name = "UNION"; break; }
        case DECIMAL: { name = "DECIMAL"; break; }
        case DATE: { name = "DATE"; break; }
        case VARCHAR: { name = "VARCHAR"; break; }
        case CHAR: { name = "CHAR"; break; }
        default: { name = "UNKNOWN"; break; }
      }
      return name ;
    }

}
