#  ____                  _                          _
# | __ )  ___ _ __   ___| |__  _ __ ___   __ _ _ __| | __
# |  _ \ / _ \ '_ \ / __| '_ \| '_ ` _ \ / _` | '__| |/ /
# | |_) |  __/ | | | (__| | | | | | | | | (_| | |  |   <
# |____/ \___|_| |_|\___|_| |_|_| |_| |_|\__,_|_|  |_|\_\
#
# MADS transport benchmark plugins: bench_source (payload generator) and
# bench_filter (latency/throughput meter). Run them with run_bench.py.
#
# The plugins must be built against the same plugin protocol, pugg and
# nlohmann/json as the MADS that loads them. Two ways to get those:
#
#   - installed MADS package (default): the versions come from
#     <prefix>/share/plugin_deps.json, the manifest `mads plugin` scaffolds
#     plugins from; <prefix> is `mads -p`, or MADS_PREFIX.
#       cmake -Bbuild -G Ninja && cmake --build build
#
#   - MADS build tree (benchmarking a development build): headers, pugg and
#     json are taken from the build tree itself, no download.
#       cmake -Bbuild -G Ninja -DMADS_BUILD_DIR=~/MADS/build
#
# MADS_PLUGIN_SOURCE_DIR overrides the plugin headers in either mode.
cmake_minimum_required(VERSION 3.28)
project(mads_benchmark LANGUAGES CXX)
if(CMAKE_BUILD_TYPE STREQUAL "")
  set(CMAKE_BUILD_TYPE "Release" CACHE STRING "Choose the type of build." FORCE)
endif()

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(SRC_DIR ${CMAKE_CURRENT_LIST_DIR}/src)
if(UNIX AND NOT APPLE)
  add_compile_options(-fPIC)
endif()

set(MADS_PREFIX "" CACHE PATH
  "Installed MADS prefix (default: output of `mads -p`)")
set(MADS_BUILD_DIR "" CACHE PATH
  "MADS build tree to build against instead of an installed package")
set(MADS_PLUGIN_SOURCE_DIR "" CACHE PATH
  "mads_plugin checkout to use for the plugin headers")
include(FetchContent)

# DEPENDENCY VERSIONS ##########################################################
if(MADS_BUILD_DIR)
  # Development build: everything from the build tree
  cmake_path(ABSOLUTE_PATH MADS_BUILD_DIR NORMALIZE)
  if(NOT EXISTS ${MADS_BUILD_DIR}/CMakeCache.txt)
    message(FATAL_ERROR "MADS_BUILD_DIR=${MADS_BUILD_DIR} is not a CMake build tree")
  endif()
  set(BENCH_MADS_SOURCE "build tree ${MADS_BUILD_DIR}")
  if(NOT MADS_PLUGIN_SOURCE_DIR)
    load_cache(${MADS_BUILD_DIR} READ_WITH_PREFIX HOST_ MADS_PLUGIN_SOURCE_DIR)
    if(HOST_MADS_PLUGIN_SOURCE_DIR)
      set(MADS_PLUGIN_SOURCE_DIR ${HOST_MADS_PLUGIN_SOURCE_DIR})
    elseif(EXISTS ${MADS_BUILD_DIR}/plugin-src/src/common.hpp)
      set(MADS_PLUGIN_SOURCE_DIR ${MADS_BUILD_DIR}/plugin-src)
    else()
      message(FATAL_ERROR "No plugin headers in ${MADS_BUILD_DIR}: was it "
                          "configured with MADS_BUILD_APPS?")
    endif()
  endif()
  set(FETCHCONTENT_SOURCE_DIR_PUGG ${MADS_BUILD_DIR}/_deps/pugg-src)
  set(FETCHCONTENT_SOURCE_DIR_JSON ${MADS_BUILD_DIR}/_deps/nlohmann_json-src)
  set(BENCH_PUGG_TAG 1.2.0)
  set(BENCH_JSON_VERSION v3.12.0)
else()
  # Installed package: versions from its plugin dependency manifest
  if(NOT MADS_PREFIX)
    execute_process(COMMAND mads -p
      OUTPUT_VARIABLE MADS_PREFIX OUTPUT_STRIP_TRAILING_WHITESPACE
      RESULT_VARIABLE _rc ERROR_QUIET)
    if(NOT _rc EQUAL 0 OR NOT MADS_PREFIX)
      message(FATAL_ERROR "Cannot find MADS: put `mads` on the PATH, or set "
                          "MADS_PREFIX (installed package) or MADS_BUILD_DIR "
                          "(build tree)")
    endif()
    set(MADS_PREFIX ${MADS_PREFIX} CACHE PATH "" FORCE)
  endif()
  set(_manifest ${MADS_PREFIX}/share/plugin_deps.json)
  if(NOT EXISTS ${_manifest})
    message(FATAL_ERROR "${_manifest} not found: this MADS is too old to "
                        "describe its plugin dependencies")
  endif()
  file(READ ${_manifest} _deps)
  string(JSON BENCH_PLUGIN_TAG GET ${_deps} plugin_git_tag)
  string(JSON BENCH_PUGG_TAG GET ${_deps} pugg_git_tag)
  string(JSON BENCH_JSON_VERSION GET ${_deps} json_version)
  set(BENCH_MADS_SOURCE "installed package ${MADS_PREFIX}")
endif()

# DEPENDENCIES #################################################################
if(MADS_PLUGIN_SOURCE_DIR)
  set(BENCH_PLUGIN_DIR ${MADS_PLUGIN_SOURCE_DIR})
else()
  FetchContent_Declare(plugin
    GIT_REPOSITORY https://github.com/pbosetti/mads_plugin.git
    GIT_TAG        ${BENCH_PLUGIN_TAG}
    GIT_SHALLOW    TRUE
  )
  FetchContent_Populate(plugin)
  set(BENCH_PLUGIN_DIR ${plugin_SOURCE_DIR})
endif()
file(STRINGS ${BENCH_PLUGIN_DIR}/src/common.hpp _proto
  REGEX "#define PLUGIN_PROTOCOL_VERSION")
string(REGEX MATCH "[0-9]+" BENCH_PLUGIN_PROTOCOL "${_proto}")

FetchContent_Declare(pugg
  GIT_REPOSITORY https://github.com/pbosetti/pugg.git
  GIT_TAG        ${BENCH_PUGG_TAG}
  GIT_SHALLOW    TRUE
  EXCLUDE_FROM_ALL
)
set(BUILD_TESTING OFF CACHE INTERNAL "")
set(JSON_BuildTests OFF CACHE INTERNAL "")
FetchContent_Declare(json
  URL https://github.com/nlohmann/json/releases/download/${BENCH_JSON_VERSION}/json.tar.xz
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE
  EXCLUDE_FROM_ALL
)
FetchContent_MakeAvailable(pugg json)

message(STATUS "MADS: ${BENCH_MADS_SOURCE}")
message(STATUS "Plugin headers: ${BENCH_PLUGIN_DIR} (protocol P${BENCH_PLUGIN_PROTOCOL})")
if(BENCH_PLUGIN_PROTOCOL LESS 9)
  message(STATUS "Protocol < 9: zero-copy plugin hooks are compiled out")
endif()

# PLUGINS ######################################################################
# Same layout as `mads plugin` scaffolds: on macOS a plugin is an executable
# with exported symbols, elsewhere a shared library; both named <name>.plugin.
macro(add_bench_plugin name)
  if(APPLE)
    add_executable(${name} ${SRC_DIR}/${name}.cpp)
    set_target_properties(${name} PROPERTIES ENABLE_EXPORTS TRUE)
  else()
    add_library(${name} SHARED ${SRC_DIR}/${name}.cpp)
  endif()
  target_include_directories(${name} BEFORE PRIVATE
    ${SRC_DIR} ${BENCH_PLUGIN_DIR}/src ${json_SOURCE_DIR}/include)
  target_link_libraries(${name} PRIVATE pugg)
  target_compile_definitions(${name} PRIVATE PLUGIN_NAME="${name}")
  set_target_properties(${name} PROPERTIES
    PREFIX ""
    SUFFIX ".plugin"
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}
    LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
endmacro()

add_bench_plugin(bench_source)
add_bench_plugin(bench_filter)

# What the plugins were built against, for run_bench.py: it defaults to these
# MADS binaries and records the details with every result.
if(MADS_BUILD_DIR)
  set(BENCH_MADS_BIN ${MADS_BUILD_DIR}/src/main)
  set(BENCH_MADS_LIB ${MADS_BUILD_DIR})
else()
  set(BENCH_MADS_BIN ${MADS_PREFIX}/bin)
  set(BENCH_MADS_LIB ${MADS_PREFIX}/bin)
endif()
file(WRITE ${CMAKE_BINARY_DIR}/bench_build_info.json "{
  \"mads\": \"${BENCH_MADS_SOURCE}\",
  \"mads_bin\": \"${BENCH_MADS_BIN}\",
  \"mads_lib\": \"${BENCH_MADS_LIB}\",
  \"plugin_protocol\": ${BENCH_PLUGIN_PROTOCOL},
  \"plugin_dir\": \"${BENCH_PLUGIN_DIR}\",
  \"compiler\": \"${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}\",
  \"build_type\": \"${CMAKE_BUILD_TYPE}\"
}
")
