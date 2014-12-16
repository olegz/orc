# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cmake_minimum_required (VERSION 2.6)

project(ORC)
set (ORC_VERSION "1.0.0")

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/")

set (LIBUV_VERSION "0.11.29")
include_directories(${CMAKE_SOURCE_DIR}/libs/libuv-${LIBUV_VERSION}/include)
set (LIBUV_LIB "${CMAKE_BINARY_DIR}/libs/libuv-${LIBUV_VERSION}/libuv.a")
if (NOT APPLE)
  list (APPEND LIBUV_LIB pthread rt)
endif (NOT APPLE)

set (GMOCK_VERSION "1.7.0")
include_directories(
  ${CMAKE_SOURCE_DIR}/libs/gmock-${GMOCK_VERSION}/include
  ${CMAKE_SOURCE_DIR}/libs/gmock-${GMOCK_VERSION}/gtest/include
  )
set (GMOCK_LIB "${CMAKE_BINARY_DIR}/libs/gmock-${GMOCK_VERSION}/libgmock_main.a")
if(NOT APPLE)
  list (APPEND GMOCK_LIB pthread)
endif(NOT APPLE)
enable_testing()

find_package (Protobuf REQUIRED)

# TODO: FindLzo.cmake license?
find_package(LZO REQUIRED)
#if (NOT LZO_FOUND)
# hack: if can't find lzo, or want to share common third-party lib, set path explicitly
    set(LZO_INCLUDE_DIR $ENV{LZO_INCLUDE_DIR})
    set(LZO_LIBRARIES $ENV{LZO_LIBRARIES})
    #endif(NOT LZO_FOUND)
MESSAGE( STATUS "LZO_INCLUDE_DIR:         " ${LZO_INCLUDE_DIR} )
MESSAGE( STATUS "LZO_LIBRARIES:         " ${LZO_LIBRARIES} )
include_directories(${LZO_INCLUDE_DIRS})

find_package(ZLIB REQUIRED)
if (NOT ZLIB_FOUND)
    # hack: if can't find lzo, or want to share common third-party lib, set path explicitly
    set(ZLIB_INCLUDE_DIR $ENV{ZLIB_INCLUDE_DIR})
    set(ZLIB_LIBRARIES $ENV{ZLIB_LIBRARIES})
endif(NOT ZLIB_FOUND)
MESSAGE( STATUS "ZLIB_INCLUDE_DIR:         " ${ZLIB_INCLUDE_DIR} )
MESSAGE( STATUS "ZLIB_LIBRARIES:         " ${ZLIB_LIBRARIES} )
include_directories(${ZLIB_INCLUDE_DIR})

set (CXX11_FLAGS "-std=gnu++11")
if (APPLE)
  set (COVERAGE_FLAGS "--coverage -g -O0")
  set (CXX11_FLAGS "${CXX11_FLAGS} -stdlib=libc++")
  set (WARN_FLAGS "-Weverything -Wno-c++98-compat -Wno-missing-prototypes")
  set (WARN_FLAGS "${WARN_FLAGS} -Wno-c++98-compat-pedantic -Wno-padded")
  set (WARN_FLAGS "${WARN_FLAGS} -Wno-covered-switch-default")
else (APPLE)
  set (WARN_FLAGS "-Wall")
endif (APPLE)
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g ${COVERAGE_FLAGS}")

set (EXAMPLE_DIRECTORY ${CMAKE_SOURCE_DIR}/examples)

add_subdirectory(libs)
add_subdirectory(src)
add_subdirectory(test)
