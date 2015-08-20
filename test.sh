#!/bin/sh

set -e

export RUST_LOG=debug

echo
echo '$ index' /passwd index
target/debug/diff index --blocksize=512 /etc/passwd /tmp/index

echo
echo '$ delta' index /passwd deltaPP
target/debug/diff delta /tmp/index /etc/passwd /tmp/deltaPP

echo
echo '$ delta' index /group deltaPG
target/debug/diff delta /tmp/index /etc/group /tmp/deltaPG

echo
echo '$ patch' /passwd deltaPP passwd
target/debug/diff patch /etc/passwd /tmp/deltaPP /tmp/passwd
if diff -q /etc/passwd /tmp/passwd; then
    echo success
else
    echo DIFFERENT
    exit 1
fi

echo
echo '$ patch' /passwd deltaPG group
target/debug/diff patch /etc/passwd /tmp/deltaPG /tmp/group
if diff -q /etc/group /tmp/group; then
    echo success
else
    echo DIFFERENT
    exit 1
fi

echo
echo '$ patch' /group deltaPP broke
(
    set +e
    target/debug/diff patch /etc/group /tmp/deltaPP /tmp/broke
    test 0 != $?
)

echo
echo "Tests succeeded"
