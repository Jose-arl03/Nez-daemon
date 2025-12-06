cmake_minimum_required(VERSION 3.10)
project(unix_socket_client CXX)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED True)

add_executable(unix_socket_client main.cpp)