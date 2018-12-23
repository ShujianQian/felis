Build
-----

1. Enter a nix environment. Under current source directory

```
nix-shell
```

2. If you have not runned before, you can run the configure script

```
./configure
```

3. Now you can build

```
./buck.pex build db
```

This will generate a binary to `buck-out/gen/db`. By default, the build is debug build and if you need optimized build you can run.

```
./buck.pex build db_release
```

This will generate the release binary to `buck-out/gen/db#release`

Test
----

Use

```
./buck.pex build test
```

to build the test binary. Then run the `buck-out/gen/dbtest` to run all unit tests. We use google-test. Run run partial test, please look at https://github.com/google/googletest/blob/master/googletest/docs/advanced.md#running-a-subset-of-the-tests .

Run
---

Setting Things Up
=================

First, you need to run the `felis-controller`. It's a scala project, so `sbt "run config.json"` will start the service.

Second, Felis need to use HugePages for memory allocation (to reduce the TLB misses). The following is what I did on c153 to pre-allocate 400GB memory of HugePages memory. You can adjust the amount depending on your memory size. (Each HuagePage is 2MB by default in Linux.)

```
echo 204800 > /proc/sys/vm/nr_hugepages
```

Run The Workload
================

Nodes need to be specified first. They are speicified in `config.json` on the felis-controller side. Once they are defined, you just need to run.

```
buck-out/gen/db#release -c 127.0.0.1:<rpc_port> -n host1 -w tpcc
```

`-c` is the felis-controller IP address, `-n` is the host name for this node, and `-w` means run `tpcc` workload. With the default configuration, you are able to run two nodes: `host1` and `host2`.

The node will initialize workload dataset and once they are idle, they are waiting for further commands from the controller. When all of them finishes initialization, you can tell the controller that everybody can proceed:

```
curl localhost:8666/broadcast/ -d '{"type": "status_change", "status": "connecting"}'
```

This will make every node start running the actual benchmark. When it all finishes, you can also use the following commands to safely shutdown.

```
curl localhost:8666/broadcast/ -d '{"type": "status_change", "status": "exiting"}'
```