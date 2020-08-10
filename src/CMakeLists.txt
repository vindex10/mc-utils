cmake_minimum_required(VERSION 3.16)

project(fastlhe)

find_package(Python3 COMPONENTS Interpreter Development)

add_library(fastlhe SHARED fastlhe.cpp)

execute_process(COMMAND "${Python3_EXECUTABLE}-config" "--extension-suffix"
                OUTPUT_VARIABLE "Python3_SOABI")
string(STRIP "${Python3_SOABI}" "Python3_SOABI")

execute_process(COMMAND "${Python3_EXECUTABLE}" "-mpybind11" "--includes"
                OUTPUT_VARIABLE pybind11_includes)
string(STRIP "${pybind11_includes}" pybind11_includes)

set_target_properties(fastlhe PROPERTIES PREFIX "")
set_target_properties(fastlhe PROPERTIES OUTPUT_NAME "fastlhe")
set_target_properties(fastlhe PROPERTIES SUFFIX "${Python3_SOABI}")
separate_arguments(pybind11_includes)
target_compile_options(fastlhe PRIVATE ${pybind11_includes})

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
