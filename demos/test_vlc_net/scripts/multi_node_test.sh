#!/bin/bash

# ./multi_node_test.sh aws_machines.txt https://github.com/hetu-project/chronos.git

if [ "$#" -ne 2 ]; then
    echo "Usage: \$0 <aws_machine_list> <git_repository_url|clean>"
    exit 1
fi

AWS_MACHINE_LIST="$1"
CLEAN_MODE="no"
if [ "$2" = "clean" ]; then
    CLEAN_MODE="yes"
else
    GIT_REPOSITORY_URL="$2"
fi

if [ ! -f "$AWS_MACHINE_LIST" ]; then
    echo "File $AWS_MACHINE_LIST does not exist."
    exit 1
fi

if [ "$CLEAN_MODE" = "yes" ]; then
    while IFS= read -r machine_ip; do
        echo "Clean to $machine_ip"

        ssh -i ~/.ssh/vlc_net_test.pem -o "StrictHostKeyChecking=no" ubuntu@"$machine_ip" -y <<'EOF'
        rm test_vlc_net server*
        pkill -f 'test_vlc_net  --server'
EOF
    done <"$AWS_MACHINE_LIST"
    exit 1
fi

echo "Update the repo dependency..."
sudo apt update -y
sudo apt install -y "Development Tools"
sudo apt install -y curl

echo "Install rust buildtools"
if ! command -v rustc &>/dev/null; then
    echo "Rust is not installed. Installing Rust..."
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs -o rustup.sh
    sh rustup.sh -y
    # shellcheck disable=SC1091
    source "$HOME/.cargo/env"
    rustc --version
else
    echo "Rust is already installed."
fi

echo "Starting local build..."
if command -v ./test_vlc_net &>/dev/null; then
    echo "test_vlc_net is already installed."
    EXECUTABLE_PATH="./test_vlc_net"
else
    git clone "$GIT_REPOSITORY_URL"
    REPO_DIR=$(basename "$GIT_REPOSITORY_URL" .git)

    cd "$REPO_DIR" || exit
    git checkout poc_1

    cargo build
    cargo build -p test_vlc_net

    EXECUTABLE_PATH="./target/release/test_vlc_net"
fi

if [ ! -f "$EXECUTABLE_PATH" ]; then
    echo "Build failed: $EXECUTABLE_PATH not found."
    exit 1
fi

while IFS= read -r machine_ip; do
    echo "Deploying to $machine_ip"
    scp -i ~/.ssh/vlc_net_test.pem -o "StrictHostKeyChecking=no" $EXECUTABLE_PATH ubuntu@"$machine_ip":/home/ubuntu/test_vlc_net

    ssh -i ~/.ssh/vlc_net_test.pem -o "StrictHostKeyChecking=no" ubuntu@"$machine_ip" <<"EOF"
        # sudo apt update -y
        chmod +x /home/ubuntu/test_vlc_net
        echo "generate"
        for I in {1..10}
        do
            PORT=$((9600 + I))
            TOKIO_CONSOLE_PORT=$((6669 + I))
            ./test_vlc_net  --server server"$I".json generate --host /ip4/0.0.0.0/tcp/ --port "$PORT" --topic vlc --trigger-us 1000000 \
                --bootnodes "/ip4/213.136.78.134/tcp/9600/p2p/12D3KooWFuHHoBaXFy3ahQfTtd2Unyhwyvwnww1YXzbXaUyhRbxA"  --concurrent-verify 100 \
                --tokio-console-port "$TOKIO_CONSOLE_PORT" --max-discover-node 2 --time-window-s 5
        done

        echo "run node"
        for I in {1..10}
        do
            nohup ./test_vlc_net  --server server"$I".json run >>server.log 2>&1 &
        done
EOF
done <"$AWS_MACHINE_LIST"

echo "Deployment completed."
