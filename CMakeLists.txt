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

set (GMOCK_VERSION "1.7.0")
include_directories(
  ${CMAKE_SOURCE_DIR}/libs/gmock-${GMOCK_VERSION}/include
  ${CMAKE_SOURCE_DIR}/libs/gmock-${GMOCK_VERSION}/gtest/include
  )

if (MSVC)
  set (GMOCK_LIB "${CMAKE_BINARY_DIR}/libs/gmock-${GMOCK_VERSION}/gmock_main")
else(MSVC)
  set (GMOCK_LIB "${CMAKE_BINARY_DIR}/libs/gmock-${GMOCK_VERSION}/libgmock_main.a")
endif(MSVC)

if(NOT APPLE AND NOT MSVC)
  list (APPEND GMOCK_LIB pthread)
endif(NOT APPLE AND NOT MSVC)
enable_testing()

find_package (Protobuf REQUIRED)

find_package(ZLIB REQUIRED)
if (NOT ZLIB_FOUND)
    # hack: if can't find zlib, or want to share common third-party lib, set path explicitly
    set(ZLIB_INCLUDE_DIR $ENV{ZLIB_INCLUDE_DIR})
    set(ZLIB_LIBRARIES $ENV{ZLIB_LIBRARIES})
endif(NOT ZLIB_FOUND)
MESSAGE( STATUS "ZLIB_INCLUDE_DIR:         " ${ZLIB_INCLUDE_DIR} )
MESSAGE( STATUS "ZLIB_LIBRARIES:         " ${ZLIB_LIBRARIES} )
include_directories(${ZLIB_INCLUDE_DIR})

if(NOT C09)
if(NOT MSVC)
  # Visual studio enables C++11 support by default.
  set (CXX11_FLAGS "-std=c++11")
endif(NOT MSVC)
endif(NOT C09)

if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
  set (COVERAGE_FLAGS "--coverage -g -O0")
  set (CXX11_FLAGS "${CXX11_FLAGS}")
  set (WARN_FLAGS "-Weverything -Wno-c++98-compat -Wno-missing-prototypes")
  set (WARN_FLAGS "${WARN_FLAGS} -Wno-c++98-compat-pedantic -Wno-padded")
  set (WARN_FLAGS "${WARN_FLAGS} -Wno-covered-switch-default")
  set (WARN_FLAGS "${WARN_FLAGS} -Wno-missing-noreturn -Werror")
elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
  set (WARN_FLAGS "-Wall -Werror")
endif ()

if(MSVC)
  set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} /MTd")
endif(MSVC)

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g")
#set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${COVERAGE_FLAGS}")

set (EXAMPLE_DIRECTORY ${CMAKE_SOURCE_DIR}/examples)

add_subdirectory(libs)
add_subdirectory(src)
if(NOT C09)
  add_subdirectory(test)
endif(NOT C09)