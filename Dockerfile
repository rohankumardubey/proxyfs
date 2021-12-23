# To build this image:
#
#   docker build                                      \
#          --target {base|dev|build|imgr|iclient}     \
#          [--build-arg GolangVersion=<X.YY.Z>]       \
#          [--build-arg MakeTarget={|all|ci|minimal}] \
#          [-t <repository>[:<tag>]]                  .
#
#   Notes:
#     --target base:
#       1) provides a clean/small base image for the rest to leverage
#       2) only addition is libc6-compat to enable Golang runtime compatibility
#     --target dev:
#       1) builds an image capable of building all elements
#       2) /src is shared with context dir of host
#     --target build:
#       1) clones the context dir at /clone
#       2) performs a make clean to clear out non-source-controlled artifacts
#       3) performs a make $MakeTarget
#     --target imgr:
#       1) builds an image containing imgr (assuming --target build built imgr)
#       2) creates TLS cert/key files (both CA and node)
#     --target iclient:
#       1) builds an image containing iclient (assuming --target build built iclient)
#       2) copies CA cert from imgr           (assuming --target build built icert)
#     --build-arg GolangVersion:
#       1) identifies Golang version
#       2) default specified in ARG GolangVersion line in --target base
#     --build-arg MakeTarget:
#       1) identifies Makefile target(s) to build (following make clean)
#       2) defaults to blank (equivalent to "all")
#       3) only used in --target build
#     -t:
#       1) provides a name REPOSITORY:TAG for the built image
#       2) if no tag is specified, TAG will be "latest"
#       3) if no repository is specified, only the IMAGE ID will identify the built image
#
# To run the resultant image:
#
#   docker run                                                        \
#          [-d|--detach]                                              \
#          [-it]                                                      \
#          [--rm]                                                     \
#          [--privileged]                                             \
#          [--mount src="$(pwd)",target="/src",type=bind]             \
#          [--env DISPLAY=<hostOrIP>:<displayNumber>[.<screenNumber]] \
#          <image id>|<repository>[:<tag>]
#
#   Notes:
#     -d|--detach:   tells Docker to detach from running container 
#     -it:           tells Docker to run container interactively
#     --rm:          tells Docker to destroy container upon exit
#     --privileged:
#       1) tells Docker to, among other things, grant access to /dev/fuse
#       2) only useful for --target dev and --target iclient
#     --mount:
#       1) bind mounts the context into /src in the container
#       2) /src will be a read-write'able equivalent to the context dir
#       3) only useful for --target dev
#     --env DISPLAY: tells Docker to set ENV DISPLAY for X apps (e.g. wireshark)

FROM alpine:3.15.0 as base
RUN apk add --no-cache libc6-compat

FROM base as dev
ARG GolangVersion=1.17.5
RUN apk add --no-cache bind-tools    \
                       curl          \
                       fuse          \
                       gcc           \
                       git           \
                       jq            \
                       libc-dev      \
                       make          \
                       tar           \
                       terminus-font \
                       wireshark
RUN curl -sSL https://github.com/coreos/etcd/releases/download/v3.5.0/etcd-v3.5.0-linux-amd64.tar.gz    \
    | tar -vxz -C /usr/local/bin --strip=1 etcd-v3.5.0-linux-amd64/etcd etcd-v3.5.0-linux-amd64/etcdctl \
    && chown root:root /usr/local/bin/etcd /usr/local/bin/etcdctl
ENV LIBGL_ALWAYS_INDIRECT=1
ENV XDG_RUNTIME_DIR="/tmp/runtime-root"
ENV GolangBasename="go${GolangVersion}.linux-amd64.tar.gz"
ENV GolangURL="https://golang.org/dl/${GolangBasename}"
WORKDIR /tmp
RUN wget -nv ${GolangURL}
RUN tar -C /usr/local -xzf $GolangBasename
ENV PATH $PATH:/usr/local/go/bin
RUN git clone https://github.com/go-delve/delve
WORKDIR /tmp/delve
RUN go build github.com/go-delve/delve/cmd/dlv
RUN cp dlv /usr/local/go/bin/.
VOLUME /src
WORKDIR /src

FROM dev as build
ARG MakeTarget
COPY . /clone
WORKDIR /clone
RUN make clean
RUN make $MakeTarget

FROM base as imgr
COPY --from=build /clone/icert/icert ./
RUN ./icert -ca -ed25519 -caCert caCert.pem -caKey caKey.pem -commonName RootCA -ttl 3560
RUN ./icert -ed25519 -caCert caCert.pem -caKey caKey.pem -commonName imgr -ttl 3560 -cert cert.pem -key key.pem -dns imgr
COPY --from=build /clone/imgr/imgr      ./
COPY --from=build /clone/imgr/imgr.conf ./

FROM imgr as iclient
RUN rm icert caKey.pem cert.pem key.pem imgr imgr.conf
RUN apk add --no-cache fuse
COPY --from=build /clone/iclient/iclient                  ./
COPY --from=build /clone/iclient/iclient.conf             ./
COPY --from=build /clone/iclient/iclient.sh               ./
COPY --from=build /clone/iauth/iauth-swift/iauth-swift.so ./
RUN apk add --no-cache curl
