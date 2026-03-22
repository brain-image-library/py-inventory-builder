Bootstrap: docker
From: ubuntu:22.04

%environment
    # Activate Conda by default
    export PATH="/opt/conda/bin:$PATH"
    source /opt/conda/etc/profile.d/conda.sh
    conda activate base

%post
    echo "Installing dependencies..."
    apt-get update && apt-get install -y wget bzip2 git && rm -rf /var/lib/apt/lists/*

    echo "Installing Miniconda..."
    wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O /tmp/miniconda.sh
    bash /tmp/miniconda.sh -b -p /opt/conda
    rm /tmp/miniconda.sh

    echo "Setting up conda..."
    export PATH="/opt/conda/bin:$PATH"
    source /opt/conda/etc/profile.d/conda.sh
    conda init
    conda config --set always_yes yes --set changeps1 no

    echo "Copying environment.yml and creating environment..."
    mkdir /opt/env
    cp /environment.yml /opt/env/environment.yml
    conda env create -f /opt/env/environment.yml

%files
    environment.yml /environment.yml

%runscript
    exec "$@"

