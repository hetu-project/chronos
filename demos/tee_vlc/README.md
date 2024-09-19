# VLC In TEE

This module verifiable logic clock is an implementation of Chronos's TEE backend.

## Prepare environment

Now, this repository uses the aws nitro enclave as its trust execution environment.  

So, please create a cloud virtual instance and notice choose the `Amazon-2023 linux` as base image.  
Because this base operator system is more friendly for using of the aws nitro enclave.

### Prepare Env & Configuration

1. Prepare Env & install dependency tools
```sh
sudo sudo dnf upgrade 
sudo dnf install -y tmux htop openssl-devel perl docker-24.0.5-1.amzn2023.0.3 aws-nitro-enclaves-cli aws-nitro-enclaves-cli-devel
``` 

2. Configuration

Please `cat /etc/nitro_enclaves/allocator.yaml` and set cpu_count & memory_mib. For tee_vlc: just `2 core + 1024 M` is enough, for tee_llm: `4 core + 16384 M` at least. Update the file and save it.

3. run `init.sh`

```sh
cd scripts
sudo chmod +x init_env.sh
./init_env.sh
```  
Remember please re-run the script when you update the `/etc/nitro_enclaves/allocator.yaml`.


## Run VLC TEE Images

```bash
cd image
cargo run --bin run-solo-vlc-enclave -- . --features nitro-enclaves
```

## Testing

```bash
cargo run --bin call_vlc_client --features nitro-enclaves
```

