# Dockerfile

# Start from NVIDIA CUDA image
FROM nvidia/cuda:12.4.1-cudnn-devel-ubuntu22.04

# Set environment variables for build optimization
ENV DEBIAN_FRONTEND=noninteractive \
    MAX_JOBS=16 \
    TORCH_CUDA_ARCH_LIST="7.5"

# Install system dependencies and Python tools in a single step
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3-pip python3-dev git cmake \
    build-essential libopenblas-dev libssl-dev \
    wget curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/* && \
    pip3 install --no-cache-dir \
    setuptools wheel ninja typing-extensions requests numpy pyyaml

# Clone PyTorch source and check out the latest stable version
WORKDIR /workspace
RUN git clone --recursive https://github.com/pytorch/pytorch.git && \
    cd pytorch && \
    git checkout v2.4.0 && \
    git submodule sync && git submodule update --init --recursive

# Build PyTorch from source for this machine
WORKDIR /workspace/pytorch
RUN CFLAGS="-march=native" CXXFLAGS="-march=native" USE_NNPACK=0 python3 setup.py install

WORKDIR /app

CMD ["python3", "-c", "import torch; print('PyTorch version:', torch.__version__); print('CUDA available:', torch.cuda.is_available()); print('GPU:', torch.cuda.get_device_name(0))"]

