FROM gcr.io/bazel-public/bazel:6.3.2

COPY --chown=ubuntu build build

RUN cd build \
    && bazel build //testers:run_sdk_destination_tester_deploy.jar

MZFROM ubuntu-base

RUN apt-get update \
    && apt-get -qy install openjdk-17-jre

COPY --from=0 /home/ubuntu/build/bazel-bin/testers/run_sdk_destination_tester_deploy.jar /usr/local/share/java/

ENTRYPOINT ["java", "-jar", "/usr/local/share/java/run_sdk_destination_tester_deploy.jar", "--working-dir=/data"]
