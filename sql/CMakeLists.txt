cmake_minimum_required(VERSION 3.16)

set(NO_STATISTICS OFF)

include(${CMAKE_CURRENT_LIST_DIR}/../cmake/version.cmake)

set(CMAKE_CXX_STANDARD 20)

project(
    tracy-sql
    LANGUAGES C CXX
    VERSION ${TRACY_VERSION_STRING}
)

include(${CMAKE_CURRENT_LIST_DIR}/../cmake/config.cmake)
include(${CMAKE_CURRENT_LIST_DIR}/../cmake/vendor.cmake)
include(${CMAKE_CURRENT_LIST_DIR}/../cmake/server.cmake)
include(${CMAKE_CURRENT_LIST_DIR}/../cmake/GitRef.cmake)

# SQLite amalgamation

CPMAddPackage(
    NAME sqlite3
    URL https://www.sqlite.org/2025/sqlite-amalgamation-3510000.zip
    URL_HASH SHA256=1caf7116f2910600d04473ad69d37ec538fa62fa36adccd37b5e0e43647c98be
    DOWNLOAD_ONLY TRUE
)

add_library(TracySqlite3 STATIC EXCLUDE_FROM_ALL ${sqlite3_SOURCE_DIR}/sqlite3.c)
target_include_directories(TracySqlite3 PUBLIC ${sqlite3_SOURCE_DIR})
target_compile_definitions(TracySqlite3 PRIVATE
    SQLITE_THREADSAFE=0
    SQLITE_OMIT_LOAD_EXTENSION
    SQLITE_OMIT_DEPRECATED
    SQLITE_OMIT_SHARED_CACHE
    SQLITE_DEFAULT_MEMSTATUS=0
    SQLITE_ENABLE_MATH_FUNCTIONS
    SQLITE_ENABLE_PERCENTILE
    SQLITE_TEMP_STORE=3
)

set(PROGRAM_FILES
    src/tracy-sql.cpp
    src/TracySqlDb.cpp
    src/TracySqlRun.cpp
)

add_executable(${PROJECT_NAME} ${PROGRAM_FILES} ${COMMON_FILES} ${SERVER_FILES})
add_git_ref(${PROJECT_NAME})
target_link_libraries(${PROJECT_NAME} PRIVATE TracyServer TracySqlite3)
set_property(DIRECTORY ${CMAKE_CURRENT_LIST_DIR} PROPERTY VS_STARTUP_PROJECT ${PROJECT_NAME})

install(TARGETS ${PROJECT_NAME} DESTINATION ${CMAKE_INSTALL_BINDIR})
