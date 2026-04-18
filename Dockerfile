# ==============================================================================
# Amatiz — Multi-stage Docker build
# ==============================================================================
# Build (locally):
#   docker build -t amatiz .
#
# Build multi-arch via Buildx (what CI does):
#   docker buildx build --platform linux/amd64,linux/arm64 -t amatiz .
#
# Run:
#   docker run -p 3100:3100 -v ./data:/data -v ./index:/index amatiz
#
# Final image is `scratch` + the static musl binary (~5–10 MB).
# ==============================================================================

# ---------------------------------------------------------------------------
# Stage 1: Build  — always runs on the build host's native arch ($BUILDPLATFORM),
# then uses Zig's built-in cross-compiler to target $TARGETARCH. This avoids
# slow QEMU emulation when producing arm64 images from an amd64 runner.
# ---------------------------------------------------------------------------
FROM --platform=$BUILDPLATFORM alpine:3.20 AS builder

ARG ZIG_VERSION=0.14.1
ARG TARGETARCH

RUN apk add --no-cache curl xz file ca-certificates

# Always pull the amd64 Zig toolchain; we cross-compile from there.
# Note: Zig's tarball naming is `zig-<target>-<version>.tar.xz` (target before version).
RUN curl -fSL "https://ziglang.org/download/${ZIG_VERSION}/zig-x86_64-linux-${ZIG_VERSION}.tar.xz" \
      | tar -xJ -C /opt && \
    mv "/opt/zig-x86_64-linux-${ZIG_VERSION}" /opt/zig
ENV PATH="/opt/zig:${PATH}"

WORKDIR /src
COPY build.zig build.zig.zon ./
COPY src/ src/

# Map Docker's TARGETARCH onto Zig's target triple, then build statically
# linked against musl.
RUN case "$TARGETARCH" in \
      amd64) ZIG_TARGET=x86_64-linux-musl ;; \
      arm64) ZIG_TARGET=aarch64-linux-musl ;; \
      *)     echo "unsupported TARGETARCH: $TARGETARCH" >&2 && exit 1 ;; \
    esac && \
    zig build -Doptimize=ReleaseSmall -Dtarget="$ZIG_TARGET" && \
    file zig-out/bin/amatiz

# ---------------------------------------------------------------------------
# Stage 2: Runtime — scratch + static binary.
# ---------------------------------------------------------------------------
FROM scratch

COPY --from=builder /src/zig-out/bin/amatiz /usr/local/bin/amatiz

VOLUME ["/data", "/index"]
EXPOSE 3100

ENTRYPOINT ["/usr/local/bin/amatiz"]
CMD ["serve", "--data-dir", "/data", "--index-dir", "/index"]

