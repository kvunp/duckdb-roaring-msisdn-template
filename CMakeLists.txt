cmake_minimum_required(VERSION 2.8.12...3.29)

project(RoaringMsisdnExtension)

# Find system CRoaring library (from Homebrew)
find_path(ROARING_INCLUDE_DIR 
    NAMES roaring64.h
    PATHS /opt/homebrew/include /usr/local/include /usr/include
    PATH_SUFFIXES roaring
    REQUIRED
)

find_library(ROARING_LIBRARIES 
    NAMES roaring
    PATHS /opt/homebrew/lib /usr/local/lib /usr/lib
    REQUIRED
)

include_directories(src/include)
include_directories(${ROARING_INCLUDE_DIR})
include_directories(${ROARING_INCLUDE_DIR}/..)

set(ROARING_MSISDN_EXTENSION_FILES
    src/roaring_msisdn_extension.cpp
    src/msisdn.cpp
    src/bitmap_funcs.cpp
    src/common.cpp
)

build_static_extension(roaring_msisdn ${ROARING_MSISDN_EXTENSION_FILES})
build_loadable_extension(roaring_msisdn " " ${ROARING_MSISDN_EXTENSION_FILES})

# Link CRoaring library
target_link_libraries(roaring_msisdn_extension ${ROARING_LIBRARIES})
target_link_libraries(roaring_msisdn_loadable_extension ${ROARING_LIBRARIES})

install(
  TARGETS roaring_msisdn_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
