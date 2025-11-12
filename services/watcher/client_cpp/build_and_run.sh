#!/bin/bash

# Build the C++ client
echo "Building C++ client..."
mkdir -p build
cd build
cmake ..
make
cd ..

if [ $? -eq 0 ]; then
    echo "C++ client built successfully."
    echo "Running C++ client..."
    ./build/unix_socket_client
else
    echo "C++ client build failed."
fi