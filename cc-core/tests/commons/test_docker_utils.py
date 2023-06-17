import random
import string

import pytest

from cc_core.commons.docker_utils import ContainerFileBitsWrapper

NUM_CHUNKS = 8
SOURCE_BYTES = "".join(random.choices(string.printable, k=1024)).encode("utf-8")


def bytes_generator(b):
    offset = 0
    while True:
        size = random.randrange(1, 10)
        if offset + size >= len(b):
            size = len(b) - offset
        if size == 0:
            return

        yield b[offset : offset + size]
        offset += size


def test_container_file_bits_wrapper_read():
    bg = bytes_generator(SOURCE_BYTES)
    bytes_read = b""

    cfbw = ContainerFileBitsWrapper(bg)

    for i in range(NUM_CHUNKS):
        s = len(SOURCE_BYTES) // NUM_CHUNKS
        br = cfbw.read(s)
        bytes_read += br

    assert bytes_read == SOURCE_BYTES, "source bytes and bytes read do not match"


def test_container_file_bits_wrapper_seek():
    bg = bytes_generator(SOURCE_BYTES)
    bytes_read = b""

    cfbw = ContainerFileBitsWrapper(bg)

    s = len(SOURCE_BYTES) // NUM_CHUNKS

    cfbw.seek(NUM_CHUNKS // 2 * s)

    for i in range(NUM_CHUNKS // 2 * s):
        br = cfbw.read(s)
        bytes_read += br

    assert (
        bytes_read == SOURCE_BYTES[NUM_CHUNKS // 2 * s :]
    ), "source bytes and bytes read do not match"


def test_container_file_bits_wrapper_tell():
    bg = bytes_generator(SOURCE_BYTES)

    cfbw = ContainerFileBitsWrapper(bg)

    for i in range(4):
        _ = cfbw.read(128)

    assert (
        cfbw.tell() == 4 * 128
    ), "tell does not return 4*128 after reading 128 bytes 4 times"
