FROM ddomizzi/deployer:normas

# Be aggressive. Remove the entire puzzlemesh directory.
RUN rm -rf /home/app/puzzlemesh

# Add the local app directory, which contains our modified puzzlemesh source
ADD ./app /home/app/

# List the contents of the workflow.cpp to prove it has been updated
RUN echo "--- Contents of workflow.cpp ---" && cat /home/app/puzzlemesh/buildingcomponents/workflow.cpp && echo "--- End of workflow.cpp ---"

# Now, compile puzzlemesh from our source
RUN cd /home/app/puzzlemesh && make

# Verify that the new executable exists and check its timestamp
RUN echo "--- Listing of puzzlemesh directory after make ---" && ls -l /home/app/puzzlemesh && echo "--- End of listing ---"

# Add and build the unix_socket_client
ADD ./client_cpp /home/app/client_cpp
RUN cd /home/app/client_cpp && cmake . && make && cp unix_socket_client /usr/local/bin/

RUN apt-get update
