Build -----

If you on the CSL cluster, you don't need to install any
dependencies. Otherwise, you need Clang 8 and Buck.

1. If you have not runned before, you can run the configure script

```
./configure
```

This script will download `buck` build tool, look for clang compiler,
and generate configurations for `buck`.

3. Now you can build

```
./buck.pex build db
```

This will generate a binary to `buck-out/gen/db#debug`. By default,
the build is debug build and if you need optimized build you can run.

```
./buck.pex build db_release
```

This will generate the release binary to `buck-out/gen/db#release`

Test ----

Use

```
./buck.pex build test
```

to build the test binary. Then run the `buck-out/gen/dbtest` to run
all unit tests. We use google-test. Run run partial test, please look
at
https://github.com/google/googletest/blob/master/googletest/docs/advanced.md#running-a-subset-of-the-tests
.


Logs ----

If you are running the debug version, the logging level is "debug" by
default, otherwise, the logging level is "info". You can always tune
the debugging level by setting the `LOGGER` environmental
variable. Possible values for `LOGGER` are: `trace`, `debug`, `info`,
`warning`, `error`, `critical`, `off`.

The debug level will output to a log file named `dbg-hostname.log`
where hostname is your node name. This is to prevent debugging log
flooding your screen.

Run ---

Setting Things Up =================

First, you need to run the `felis-controller`. Please refer to the
README file there.

Second, Felis need to use HugePages for memory allocation (to reduce
the TLB misses). Common CSL cluster machines should have these already
setup, so you can skip this step. The following is what I did on c153
to pre-allocate 400GB memory of HugePages memory. You can adjust the
amount depending on your memory size. (Each HuagePage is 2MB by
default in Linux.)

```
echo 204800 > /proc/sys/vm/nr_hugepages
```

Run The Workload ================

Nodes need to be specified first. They are speicified in `config.json`
on the felis-controller side. Once they are defined, you just need to
run. For example:

```
buck-out/gen/db#release -c 127.0.0.1:<rpc_port> -n host1 -w tpcc -Xcpu32 -Xmem32G -XYcsbContentionKey4 -XYcsbSkewFactor90 -XVHandleBatchAppend -XVHandleParallel
```

`-c` is the felis-controller IP address, `-n` is the host name for
this node, and `-w` means run `tpcc` workload. With the default
configuration, you are able to run two nodes: `host1` and
`host2`. `-X` are for the extended arguments. For a list of `-X`,
please refer to `opts.h`. Mostly you will need `-Xcpu` and `-Xmem` to
specifies how many cores and how much memory to use. (Currently,
number of CPU must be multiple of 8. That's bug, but we don't have
time to fix it though.)

The node will initialize workload dataset and once they are idle, they
are waiting for further commands from the controller. When all of them
finishes initialization, you can tell the controller that everybody
can proceed:

```
curl localhost:8666/broadcast/ -d '{"type": "status_change", "status": "connecting"}'
```

This will make every node start running the actual benchmark. When it
all finishes, you can also use the following commands to safely
shutdown.

```
curl localhost:8666/broadcast/ -d '{"type": "status_change", "status": "exiting"}'
```
