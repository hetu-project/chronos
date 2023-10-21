# Preparation Guide

## Hardware

Each of the five servers should attach a 100G NIC.

The five server machines should connect NIC to switch's pipeline 0.
The FPGA should also be connected to switch.
Update the following files based on the physical/dev ports that servers/FPGA connect to and server NICs MAC addresses:
* `assets/neo-switch/bootstrap/port_setup`: line 3-9, physical ports
* `assets/neo-switch/bootstrap/bcast_setup.py`: line 1-3, dev ports
* `assets/neo-switch/bootstrap/mcast_setup.py`: line 2, dev ports
* `assets/neo-switch/p4src/fpga/neo_fpga.p4`: line 171-177, MAC addresses and dev ports
* `assets/neo-switch/p4src/switch/forwarding.p4`: line 20-26, MAC addresses and dev ports

## Software

The development environment should install Rust toolchain that targets server machines.
It should also install Terraform and AWS CLI.

In order to plot, development environment should install `seaborn` pypi package and Jupyter notebook environment.

> Note: It is assumed that there is access to the Intel P4 Studio SDE version 9.7.0 and the ICA tools helper scripts.

The P4 Studio SDE is placed in switch's home directory, i.e., `$HOME/bf-sde-9.7.0`, and also the tools at `$HOME/tools`.
Environment variables should be loaded with `source ~/tools/set_sde.bash`.

The FPGA host machine should have Xilinx Runtime (XRT) and deployment platform for Alveo U50 installed, which can be retrieved [here](https://www.xilinx.com/products/boards-and-kits/alveo/u50.html#vitis).
The design is tested with host machine running Ubuntu 20.04, XRT 2.13.479 for Vitis toolchain 2022.1.
FPGA shell version shall be `xilinx_u50_gen3x16_xdma_201920_3`.

## Configure AWS account

Make sure vCore limit is not less than 600 in the evaluating region.

Create a key pair called "Ephemeral" in the evaluating region.

Update the following files based on the evaluating region:
* `script/neo-aws/main.tf`: line 13

## Configure Development Environment

Development environment should be able to SSH into server machines without password.

It should be able to access HTTP service on server machines port 9999.

Update the following files based on server machine host names:
* `scripts/reload/src/main.rs`: line 9-13, and line 16 if development environment is one of the server machines
* `scripts/control/src/main.rs`: line 234-240

Set up AWS account with `aws configure`.
Append `~/.ssh/config` with following content

```
Host *.compute.amazonaws.com
    StrictHostKeyChecking no
    UserKnownHostsFile=/dev/null
    User ubuntu
    IdentityFile [path to "Ephemeral" key pair's PEM file]
    LogLevel Quiet
```

## Configure Switch

Copy `assets/neo-switch` to switch home directory.
Enter the directory and run `make`.

## Configure FPGA

Copy the `assets/neo-accel.xclbin.tgz` tarball to the FPGA host machine and untar. 
Then the FPGA device can be programmed with `xbutil program -d <FPGA_PCIE_BDF> -u <XCLBIN_FILE>`.

## Configure Servers

Isolate core 0-1 and their hyperthreading siblings.

Set the number of NIC's RX queue to 1.

Set CPU frequency governors to performance.

Update the following files based on server IPs that bind to evaluation NICs:
* `scripts/control/src/main.rs`: line 225-230

Alternatively, configure the IPs according to the file.
