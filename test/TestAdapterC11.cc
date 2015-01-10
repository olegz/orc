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

#include "TestAdapterC11.hh"

namespace orc {

  std::auto_ptr<Type> createStructType(std::initializer_list<std::unique_ptr<Type> > types,
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

    return std::auto_ptr<Type>(new TypeImpl(STRUCT, typeVector,
        fieldVector));
  }
}  // namespace orc

