# Solana gRPC vs WebSocket Benchmark

A benchmarking tool that compares latency between gRPC and WebSocket protocols for receiving Solana slot updates. It
measures which protocol receives updates first and calculates the average latency difference between them.

## Prerequisites

- Rust toolchain 1.85.0 or later
- Access to a Solana RPC node with both:
    - WebSocket endpoint (typically port 8900)
    - gRPC endpoint (typically port 10000)

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/grpc-ws-benchmark.git
   cd grpc-ws-benchmark
   ```

2. Create a `.env` file in the project root with the following variables:
   ```
   WS_URL=ws://your-rpc-node:8900
   GRPC_URL=http://your-rpc-node:10000
   RUN_DURATION_SECS=30
   ```
   Replace `your-rpc-node` with your Solana RPC node address.

3. Build and run the benchmark:
   ```bash
   cargo build --release
   cargo run --release
   ```

The benchmark will run for the duration specified in your `.env` file (default 30 seconds) and output comparison
statistics between gRPC and WebSocket performance.
