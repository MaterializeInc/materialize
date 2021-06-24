#!env python3
import fileinput
import re

extract_key_value_re = re.compile(r"(.*)=([^i]*)i?")

header = []
buffer = []
for line in fileinput.input():
    (key, value, time) = line.strip().split(' ')
    for key_item in key.split(','):
        key_value_group = extract_key_value_re.match(key_item)
        if key_value_group:
            buffer.append(key_value_group.group(2))
            if header:
                header.append(key_value_group.group(1))
        else:
            buffer.append(key_item)
            if header is not None:
                header.append(key_item)
    for key_item in value.split(','):
        key_value_group = extract_key_value_re.match(key_item)
        assert key_value_group
        buffer.append(key_value_group.group(2))
        if header is not None:
            header.append(key_value_group.group(1))
    buffer.append(time)
    if header is not None:
        header.append("timestamp")

    if header is not None:
        print(",".join(header))
        header = None
    print(",".join(buffer))

    buffer.clear()
