# cmake/run_bench_readme.cmake
#
# Runs the benchmark executable, captures its stdout, then replaces
# the section between <!-- BENCH_START --> and <!-- BENCH_END --> in
# README.md with the new results.
#
# Required variables (passed via -D on the cmake -P command line):
#   BENCH_EXE  — full path to bench_msgpack_vs_json executable
#   README     — full path to README.md

if(NOT BENCH_EXE OR NOT README)
    message(FATAL_ERROR
        "run_bench_readme.cmake: BENCH_EXE and README must be set.\n"
        "  cmake -DBENCH_EXE=<path> -DREADME=<path> -P run_bench_readme.cmake")
endif()

message(STATUS "Running benchmark: ${BENCH_EXE}")

execute_process(
    COMMAND "${BENCH_EXE}"
    OUTPUT_VARIABLE BENCH_OUT
    ERROR_VARIABLE  BENCH_ERR
    RESULT_VARIABLE BENCH_RC
)

if(BENCH_RC)
    message(WARNING "Benchmark stderr: ${BENCH_ERR}")
    message(FATAL_ERROR "Benchmark exited with code ${BENCH_RC}")
endif()

# Read the current README
file(READ "${README}" README_CONTENT)

# Find the start and end markers
string(FIND "${README_CONTENT}" "<!-- BENCH_START -->" START_POS)
string(FIND "${README_CONTENT}" "<!-- BENCH_END -->"   END_POS)

if(START_POS EQUAL -1 OR END_POS EQUAL -1)
    message(FATAL_ERROR
        "README does not contain <!-- BENCH_START --> / <!-- BENCH_END --> markers.")
endif()

# Split the README into three parts: before, old-bench, after
string(SUBSTRING "${README_CONTENT}" 0 ${START_POS} BEFORE)

# After = everything from the character after "<!-- BENCH_END -->" onwards
math(EXPR AFTER_POS "${END_POS} + 18") # length("<!-- BENCH_END -->") = 18
string(LENGTH "${README_CONTENT}" README_LEN)
math(EXPR AFTER_LEN "${README_LEN} - ${AFTER_POS}")
string(SUBSTRING "${README_CONTENT}" ${AFTER_POS} ${AFTER_LEN} AFTER)

# Reassemble with fresh benchmark output, then append the image reference.
# The benchmark binary outputs <!-- BENCH_START --> ... <!-- BENCH_END --> itself,
# but we want the image line between the table and the closing marker.
# Strategy: strip the closing marker from BENCH_OUT, append image + marker.
string(REPLACE "<!-- BENCH_END -->" "" BENCH_OUT_TRIMMED "${BENCH_OUT}")
set(IMG_LINE "\n![sqlite-msgpack vs JSON vs JSONB performance](docs/bench.png)\n\n<!-- BENCH_END -->")
file(WRITE "${README}" "${BEFORE}${BENCH_OUT_TRIMMED}${IMG_LINE}${AFTER}")

message(STATUS "README.md updated with new benchmark results.")
