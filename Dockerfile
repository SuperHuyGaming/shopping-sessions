# Shopping Sessions: native Rust engine. Scorers are Intel Xeon (Cascade Lake+, AVX-512) -> x86-64-v4.
FROM rust:1.89-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock* ./
COPY src ./src
ENV RUSTFLAGS="-C target-cpu=x86-64-v4"
RUN cargo build --release --locked && strip target/release/shop

FROM debian:bookworm-slim
COPY --from=build /src/target/release/shop /app/shop
WORKDIR /app
CMD ["/app/shop"]
