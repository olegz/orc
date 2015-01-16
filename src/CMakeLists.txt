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

configure_file (
  "orc/orc-config.hh.in"
  "orc/orc-config.hh"
  )

include_directories (
  ${CMAKE_CURRENT_BINARY_DIR}
  ${PROTOBUF_INCLUDE_DIRS}
  )

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS orc_proto.proto)

set (CMAKE_CXX_FLAGS "-O0 ${CMAKE_CXX_FLAGS} -g ${CXX11_FLAGS} ${WARN_FLAGS}")

add_library (orc STATIC
  ${PROTO_HDRS}
  wrap/orc-proto-wrapper.cc
  ByteRLE.cc
  ColumnReader.cc
  Compression.cc
  Exceptions.cc
  Int128.cc
  OrcFile.cc
  Reader.cc
  RLEv1.cc
  RLEv2.cc
  RLE.cc
  TypeImpl.cc
  Vector.cc
  ColumnPrinter.cc
  )

target_link_libraries (orc
  ${PROTOBUF_LITE_LIBRARIES}
  ${ZLIB_LIBRARIES}
  )

add_executable (file-dump
  LocalFileReader.cc
  )

target_link_libraries (file-dump
  orc
  ${PROTOBUF_LIBRARIES}
  )

add_executable (file-scan
  FileScan.cc
  )

target_link_libraries (file-scan
  orc
  ${PROTOBUF_LIBRARIES}
  )

