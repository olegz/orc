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

#include "orc/MemoryPool.hh"
#include <cstdlib>
#include <iostream>

namespace orc {

  MemoryPool::~MemoryPool() {}

  class MemoryPoolImpl: public MemoryPool {
  public:
    void* malloc(uint64_t size) override ;
    void free(void* p) override ;

    MemoryPoolImpl();
    ~MemoryPoolImpl();
  };

  void* MemoryPoolImpl::malloc(uint64_t size) {
    return std::malloc(size);
  }

  void MemoryPoolImpl::free(void* p) {
    std::free(p);
  }

  MemoryPoolImpl::MemoryPoolImpl() {}
  MemoryPoolImpl::~MemoryPoolImpl() {}

  std::unique_ptr<MemoryPool> createDefaultMemoryPool() {
    return std::unique_ptr<MemoryPool>(new MemoryPoolImpl());
  }
} // namespace orc
