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

#include "orc/Int128.hh"
#include "orc/MemoryPool.hh"

#include <cstdlib>
#include <iostream>

namespace orc {

  MemoryPool::~MemoryPool() {
    // PASS
  }

  class MemoryPoolImpl: public MemoryPool {
  public:
    virtual ~MemoryPoolImpl();

    char* malloc(uint64_t size) override;
    void free(char* p) override;
  };

  char* MemoryPoolImpl::malloc(uint64_t size) {
    return static_cast<char*>(std::malloc(size));
  }

  void MemoryPoolImpl::free(char* p) {
    std::free(p);
  }

  MemoryPoolImpl::~MemoryPoolImpl() {
    // PASS
  }

  template <class T>
  DataBuffer<T>::DataBuffer(MemoryPool& pool,
                            uint64_t newSize
                            ): memoryPool(pool),
                               buf(nullptr),
                               currentSize(0),
                               currentCapacity(0) {
    resize(newSize);
  }

  template <class T>
  DataBuffer<T>::~DataBuffer(){
    for(uint64_t i=currentSize; i > 0; --i) {
      (buf + i - 1)->~T();
    }
    if (buf) {
      memoryPool.free(reinterpret_cast<char*>(buf));
    }
  }

  template <class T>
  void DataBuffer<T>::resize(uint64_t newSize) {
    reserve(newSize);
    if (currentSize > newSize) {
      for(uint64_t i=currentSize; i > newSize; --i) {
        (buf + i - 1)->~T();
      }
    } else if (newSize > currentSize) {
      for(uint64_t i=currentSize; i < newSize; ++i) {
        new (buf + i) T();
      }
    }
    currentSize = newSize;
  }

  template <class T>
  void DataBuffer<T>::reserve(uint64_t newCapacity){
    if (newCapacity > currentCapacity) {
      if (buf) {
        T* buf_old = buf;
        buf = reinterpret_cast<T*>(memoryPool.malloc(sizeof(T) * newCapacity));
        std::memcpy(buf, buf_old, sizeof(T) * currentSize);
        memoryPool.free(reinterpret_cast<char*>(buf_old));
      } else {
        buf = reinterpret_cast<T*>(memoryPool.malloc(sizeof(T) * newCapacity));
      }
      currentCapacity = newCapacity;
    }
  }

  #pragma clang diagnostic ignored "-Wexit-time-destructors"

  MemoryPool* getDefaultPool() {
    static MemoryPoolImpl internal;
    return &internal;
  }

  #pragma clang diagnostic ignored "-Wweak-template-vtables"

  template class DataBuffer<char>;
  template class DataBuffer<char*>;
  template class DataBuffer<double>;
  template class DataBuffer<Int128>;
  template class DataBuffer<int64_t>;
  template class DataBuffer<uint64_t>;

} // namespace orc
