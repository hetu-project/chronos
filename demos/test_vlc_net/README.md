# Test VLC Net

This module is mainly convenient for testing vlc network. It combines vlc and gossip for constructing the multi-node network.   

And some features as follow:
- Configuration: It uses the variety commands to define detail of the vlc network.
- Demonstrate: The test_vlc_net will collect useful data metric in work period.
- Streamlined workflow: This program keep concise and core workflows for better testing.

## Compile server node

```bash
# For compling tikv-jemalloc-sys
sudo apt-get install make
sudo apt-get install build-essential

# if enable the tokio console function, need export env variate. Then build
export RUSTFLAGS="--cfg tokio_unstable"

cargo build -p test_vlc_net
cd target/debug
```

## Generate config files

### DHT feature

If using DHT as the discovery protocol, first generate the `bootnode` configuration.

```bash
./test_vlc_net  --server server0.json generate --host /ip4/0.0.0.0/tcp/ --max-discover-node 4096
cat server0.json
```

### Business node configuration

Generate configurations for other business nodes:

```bash
for I in {1..4}
do
    PORT=$((9600 + I))
    TOKIO_CONSOLE_PORT=$((6669 + I))
    ./test_vlc_net --server server"$I".json generate \
        --host /ip4/127.0.0.1/tcp/ \
        --port "$PORT" \
        --topic vlc \
        --trigger-us 1000 \
        --bootnodes "/ip4/127.0.0.1/tcp/9601" \
        --max-sys-memory-percent 80 \
        --max-proc-memory-mb 8192 \
        --enable-tx-send \
        --tokio-console-port "$TOKIO_CONSOLE_PORT"
done
```

**Note**: Replace the `--bootnodes` value with the `multi_addr` field from the bootnode config file  `server0.json`.
**Example**: `/ip4/127.0.0.1/tcp/9600/p2p/12D3KooWQt4eiRVEZGFcThutsLvUbJS2rgLJ9QnE1ptp9ZmjRMay` 

### Configuration parameters explained

* --server: Specifies the output config file name
* --host: Sets the IP address and protocol for the node
* --port: Sets the port number for the node
* --topic: Defines the topic for the VLC network
* --trigger-us: Sets the trigger interval in microseconds
* --bootnodes: Specifies the address of the bootstrap node(s)
* --enable-tx-send: Enables transaction sending capability
* --tokio-console-port: Sets the port for the tokio console (if enabled)
* --max-sys-memory-percent: Sets the maximum system memory usage for sending event.
* --disconnect-rate: Sets the disconnect rate with peers when memory almost exhausted.
* --max-proc-memory-mb: Set the maximum process memory usage for keeping network connection.


## Run multi-node network

### Start the bootnode (if using DHT)

If DHT is enabled, first start the bootnode:

```bash
nohup ./test_vlc_net  --server server0.json run >>server0.log 2>&1 &
``` 

### Start business nodes

Run the following script to start multiple business nodes:

```bash
for I in {1..4}
do
    nohup ./test_vlc_net  --server server"$I".json run >>server.log 2>&1 &
done

# if enable the tokio console function for watching debug metric
cargo install tokio-console
tokio-console
```

### Monitor with tokio console (optional)

If you've enabled the tokio console function and want to monitor runtime metrics:

1.Install the tokio-console tool:

```bash
cargo install tokio-console
```

2.Run the console:

```bash
tokio-console
```

This will allow you to view detailed tokio runtime information about your VLC network nodes.

## Monitor network metrics

To view the log output of all nodes:

```bash
tail -f server.log
```

To filter and watch only the connected node count:

```bash
tail -F server.log | grep --line-buffered "Connected node num"
```

## Gracefully stop the network

### To stop all running nodes:

```bash
pkill -f 'test_vlc_net.*run'
```

**Note**: This command will terminate all processes that match the pattern 'test_vlc_net.*run'. Ensure you don't have any other important processes matching this pattern before running it.

For a more selective approach, you can stop nodes individually using their process IDs:

1.Find the process IDs:

```bash
ps aux | grep '[t]est_vlc_net.*run'
```

2.Stop each process using its ID:
```bash
kill <process_id>
```

Replace `<process_id>` with the actual process ID from the previous command's output.