# cmake/run_sql_test.cmake
# Invoked by CTest to run the SQL test script via the sqlite3 shell.
# Variables: SQLITE3_EXE, EXT, TEST_SQL

if(NOT SQLITE3_EXE OR NOT EXT OR NOT TEST_SQL)
    message(FATAL_ERROR "run_sql_test.cmake: SQLITE3_EXE, EXT, and TEST_SQL must all be set")
endif()

# Write a tiny init script that loads the extension then reads the test file.
# Strip any existing .load lines from the test file to avoid path conflicts.
file(READ "${TEST_SQL}" TEST_CONTENT)
string(REGEX REPLACE "\\.load[^\n]*\n?" "" TEST_CONTENT_CLEAN "${TEST_CONTENT}")
file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/init_msgpack_test.sql"
    ".load ${EXT}\n${TEST_CONTENT_CLEAN}"
)

execute_process(
    COMMAND ${SQLITE3_EXE} :memory:
    INPUT_FILE "${CMAKE_CURRENT_BINARY_DIR}/init_msgpack_test.sql"
    OUTPUT_VARIABLE OUT
    ERROR_VARIABLE  ERR
    RESULT_VARIABLE RES
)

if(ERR)
    message(STATUS "stderr: ${ERR}")
endif()

if(NOT RES EQUAL 0)
    message(FATAL_ERROR "sqlite3 exited with code ${RES}")
endif()

# Count failures: rows where result column != expected column
string(REGEX MATCHALL "\n\\| [^\n]+ \\|" ROWS "${OUT}")
set(FAIL_COUNT 0)
foreach(ROW IN LISTS ROWS)
    # Extract result and expected columns (columns 2 and 3 of pipe-delimited output)
    string(REGEX MATCH "\\| ([^|]+) \\| ([^|]+) \\| ([^|]+) \\|" M "${ROW}")
    if(M)
        string(STRIP "${CMAKE_MATCH_2}" RESULT)
        string(STRIP "${CMAKE_MATCH_3}" EXPECTED)
        if(NOT RESULT STREQUAL EXPECTED AND NOT CMAKE_MATCH_1 MATCHES "test")
            math(EXPR FAIL_COUNT "${FAIL_COUNT} + 1")
            message(STATUS "FAIL [${CMAKE_MATCH_1}]: got='${RESULT}' want='${EXPECTED}'")
        endif()
    endif()
endforeach()

message(STATUS "All tests complete. ${FAIL_COUNT} failures")
if(FAIL_COUNT GREATER 0)
    message(FATAL_ERROR "${FAIL_COUNT} SQL test(s) failed")
endif()
