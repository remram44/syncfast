#!/bin/sh

export RUST_LOG=debug

echo
echo '$ index' /passwd index
target/debug/diff index /etc/passwd /tmp/index

echo
echo '$ delta' index /passwd deltaPP
target/debug/diff delta /tmp/index /etc/passwd /tmp/deltaPP

echo
echo '$ delta' index /group deltaPG
target/debug/diff delta /tmp/index /etc/group /tmp/deltaPG

echo
echo '$ patch' /passwd deltaPP passwd
target/debug/diff patch /etc/passwd /tmp/deltaPP /tmp/passwd
diff -q /etc/passwd /tmp/passwd && echo success || echo DIFFERENT

echo
echo '$ patch' /passwd deltaPG group
target/debug/diff patch /etc/passwd /tmp/deltaPG /tmp/group
diff -q /etc/group /tmp/group && echo success || echo DIFFERENT

echo
echo '$ patch' /group deltaPP broke
target/debug/diff patch /etc/group /tmp/deltaPP /tmp/broke
