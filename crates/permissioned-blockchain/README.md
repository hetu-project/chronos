## NeoBFT: Accelerating Byzantine Fault Tolerance Using Authenticated In-Network Ordering

[![DOI](https://zenodo.org/badge/672471121.svg)](https://zenodo.org/doi/10.5281/zenodo.8317486)

The evaluation of NeoBFT consists of three parts: 
* AOM micro-benchmarks (section 6.1, figure 4-6)
* four replica performance benchmarks (section 6.2, 6.4 and 6.5, figure 7, 9 and 10)
* AWS deployment (section 6.3, figure 8).

The first two parts should be conducted on a hardware-accessible cluster.
For part one, you will need a Tofino1 switch, a Xillinx AU50 FPGA and a server machine.
For part two, you will need the switch and the FPGA same as part 1, and five server machines, and the first four of which must have identical specification.

For part three, you will need an AWS account (and pay for the bill after evaluation).

For all three parts, you will also need a development environment that builds artifacts, controls the evaluation runs and collect results.
Refer to the [preparation guide](./prepare.md) for detail setup.

During the evaluation, you will only need to issue commands from switch, the first server machine and development environment.

# Collect Data for Part 1

Open three terminals, the first two on switch and the third on the first server.

In the first terminal, start switchd for HMAC program 

```
switch:~$ $SDE/run_switchd.sh -p neo_hmac_bench
```

You may be prompted to enter password for sudo access. 
Wait until the switchd prompt `bfshell>` is shown.

In the second terminal, set up switch program

```
switch:~$ cd neo-switch
switch:~/neo-switch$ ./setup_hmac.sh
```

### Generate Packets for 99% Load

Start packet generation script, input "99" for load

```
switch:~/neo-switch$ ./pktgen_hmac.sh
... (output omitted)
load: 99
```

In the third terminal, capture packets (assuming network interface named `ens1f0`)

```
server1:~$ sudo tcpdump -i ens1f0 -e | grep -e "00:00:00:" > extracted.log
```

In the second terminal, start packet generation session

```
PD(neo_hmac_bench)[0]>>> pktgen.app_enable(1)
```

In the first terminal, check switch's instantaneous throughput (there may be some output after the prompt which is safe to be ignored)

```
bfshell> ucli
Starting UCLI from bf-shell 

Cannot read termcap database;
using dumb terminal settings.
bf-sde> pm rate-period 1
bf-sde> pm rate-show
-----+----+---+----+-------+---+-------+-------+---------+---------+----+----
PORT |MAC |D_P|P/PT|SPEED  |RDY|RX Mpps|TX Mpps|RX Mbps  |TX Mbps  |RX %|TX %
-----+----+---+----+-------+---+-------+-------+---------+---------+----+----
9/0  |15/0|  0|0/ 0|100G   |UP |   0.00|  76.25|     0.00| 54904.47|  0%| 54%
... (output omitted)
bf-sde> exit
```

As shown in `Tx Mpps` column, the throughput is 76.25Mpps.

After ~10 seconds, stop packet generation

```
PD(neo_hmac_bench)[0]>>> pktgen.app_disable(1)
```

Enter `ctrl-d` to exit session.

In the third terminal, `ctrl-c` to stop packet capturing.
Save MAC addresses into result file

```
server1:~$ awk -F ' ' '{print $6}' extracted.log > results_hmac99.log
```

### Generate Packets for 50% Load

Repeat the process above, input "50" for load.
Save the capturing results to `results_hmac50.log`.

You don't need to check for throughput in the first terminal.

### Generate Packets for 25% Load

Repeat the process above, input "25" for load.
Save the capturing results to `results_hmac25.log`.

You don't need to check for throughput in the first terminal.

In the first terminal, `ctrl-\` to stop switchd.

To collect data for public key cryptography variant, repeat the same process while replacing `hmac` with `fpga`, i.e. `neo_hmac_bench` to `neo_fpga_bench`, `./setup_hmac.sh` to `./setup_fpga.sh`, `./pktgen_hmac.sh` to `./pktgen_fpga.sh`.

The throughput shown in the first terminal is 1.11Mpps.

Save the results to `results_fpgaXX.log` files.

# Collect Data for Part 2

Open three terminals, the first two on switch and the third on dev.

In the first terminal, start switchd for HMAC program

```
switch:~$ $SDE/run_switchd.sh -p neo_hmac
```

You may be prompted to enter password for sudo access. 
Wait until the switchd prompt `bfshell>` is shown.

In the second terminal, set up switch program

```
switch:~$ cd neo-switch
switch:~/neo-switch$ ./setup_hmac.sh
```

In the third terminal, reload servers and run benchmarks

```
dev:~$ cd neobft-artifact
dev:~/neobft-artifact$ cargo -q run -p reload
    Finished release [optimized] target(s) in 0.16s
* server started on server1
dev:~/neobft-artifact$ cargo -q run -p control -- hmac
```

The results will be saved to `saved-hmac.csv`.
The complete run takes minutes.
If the evaluating is crashed or killed manually, it will resume in the next run.
Just make sure to reload because running it again.

After running, in the first terminal enter `ctrl-\` to stop switchd.

Then repeat the process with `hmac` replaced to `fpga`, i.e. `neo_hmac` to `neo_fpga`, `./setup_hmac.sh` to `./setup_fpga.sh`, and `hmac` to `fpga` in the control script command.
The results are saved to `saved-fpga.csv`.

# Collect Data for Part 3

Open one terminal on dev.

Create AWS cluster

```
dev:~$ cd neobft-artifact
dev:~/neobft-artifact$ terraform -chdir=scripts/neo-aws apply
```

Enter "yes" when prompted.

Initialize the cluster

```
dev:~/neobft-artifact$ cargo -q run -p neo-aws
```

Reload servers and run control script

```
dev:~/neobft-artifact$ cargo -q run -p reload --features aws
    Finished release [optimized] target(s) in 0.07s
* server started on ip-x-x-x-x.ap-east-1.compute.amazonaws.com
dev:~/neobft-artifact$ cargo -q run -p control --features aws -- aws
```

The results will be saved to `saved-aws.csv`.

After running, destroy AWS cluster

```
dev:~/neobft-artifact$ terraform -chdir=scripts/neo-aws destroy
```

Enter "yes" when prompted.

# Visualization

Collect all result files into `data` directory and refer to the notebooks.

# Disclaimer

This is a complete overhaul of the original version.
The exact code the produces camera-ready results can be found in `master-legacy` branch.

Noticeable implementation differences:
* Replace public key cryptography library `secp256k1` to `k256`
* Remove cryptography offloading and worker threads
* Apply the same pace-based adaptive batching strategy to all protocols universally
  * The Zyzzyva implementation is currently buggy under the new strategy, but its faulty variant seems normal

These changes help produce more realistic results, better explore protocol's best and typical latency, and reduce resource utilization.
