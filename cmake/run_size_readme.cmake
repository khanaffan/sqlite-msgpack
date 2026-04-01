# cmake/run_size_readme.cmake
#
# Runs the size-comparison executable, captures its stdout, then replaces
# the section between <!-- SIZE_START --> and <!-- SIZE_END --> in README.md.
#
# Required variables:
#   SIZE_EXE  — full path to bench_size_comparison executable
#   README    — full path to README.md

if(NOT SIZE_EXE OR NOT README)
    message(FATAL_ERROR
        "run_size_readme.cmake: SIZE_EXE and README must be set.")
endif()

message(STATUS "Running size comparison: ${SIZE_EXE}")

execute_process(
    COMMAND "${SIZE_EXE}"
    OUTPUT_VARIABLE SIZE_OUT
    ERROR_VARIABLE  SIZE_ERR
    RESULT_VARIABLE SIZE_RC
)

if(SIZE_RC)
    message(WARNING "Size comparison stderr: ${SIZE_ERR}")
    message(FATAL_ERROR "bench_size_comparison exited with code ${SIZE_RC}")
endif()

file(READ "${README}" README_CONTENT)

string(FIND "${README_CONTENT}" "<!-- SIZE_START -->" START_POS)
string(FIND "${README_CONTENT}" "<!-- SIZE_END -->"   END_POS)

if(START_POS EQUAL -1 OR END_POS EQUAL -1)
    message(FATAL_ERROR
        "README does not contain <!-- SIZE_START --> / <!-- SIZE_END --> markers.")
endif()

string(SUBSTRING "${README_CONTENT}" 0 ${START_POS} BEFORE)

math(EXPR AFTER_POS "${END_POS} + 17")  # length("<!-- SIZE_END -->") = 17
string(LENGTH "${README_CONTENT}" README_LEN)
math(EXPR AFTER_LEN "${README_LEN} - ${AFTER_POS}")
string(SUBSTRING "${README_CONTENT}" ${AFTER_POS} ${AFTER_LEN} AFTER)

# Strip closing marker from output, then re-append with image line
string(REPLACE "<!-- SIZE_END -->" "" SIZE_OUT_TRIMMED "${SIZE_OUT}")
set(IMG_LINE "\n![Serialised-size comparison](docs/size_comparison.png)\n\n<!-- SIZE_END -->")
file(WRITE "${README}" "${BEFORE}${SIZE_OUT_TRIMMED}${IMG_LINE}${AFTER}")

message(STATUS "README.md updated with new size-comparison results.")
