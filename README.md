# RDMA client-server program

A simple client-server request-response program implemented using RDMA.

1. The program uses RDMA CM library for RDMA connection setup
2. The client can generate load using open loop or closed loop approach (`DO_CLOSED_LOOP` macro)
3. The client can opt for a CPU-efficient sender or a polling-based sender (`BLOCKING_SENDER` macro)

The purpose of this program to get used to the commonly used RDMA verbs and RDMA CM
APIs. This repository gives a simple skeleton or husk of a scalable RPC application that
needs RDMA as its transport.

## Prereqs

The following steps assume that you have an RDMA-capable NIC and that the `libibverbs`
and `librdmacm` libraries are installed on your system. If not, then please make sure
to install them.

## Build

```
g++ -O3 client.cc -libverbs -lrdmacm -o client
g++ -O3 server.cc -libverbs -lrdmacm -o server
```

## Run

Run the server
```
./server
```

Run the client (from another session)
```
./client
```