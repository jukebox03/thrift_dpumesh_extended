#!/bin/bash
# Stop old processes
sudo killall test_comch gateway dpumesh_dpu 2>/dev/null

# Start DPUmesh (Host representor part? Wait, DPU runs on localhost, which is the same machine in this environment?)
cd lib/cpp/src/thrift/transport/doca
sudo ./build/dpumesh_dpu -p 03:00.0 -r 94:00.0 &
DPU_PID=$!
cd ../../../../../..

sleep 2

sudo DPUMESH_PCI_ADDR=94:00.0 DPUMESH_POD_ID=1 ./gateway &
GW_PID=$!

sleep 2

sudo DPUMESH_PCI_ADDR=94:00.0 DPUMESH_POD_ID=2 ./test_comch
TEST_RET=$?

sudo kill $GW_PID $DPU_PID 2>/dev/null
wait $DPU_PID $GW_PID 2>/dev/null

exit $TEST_RET
