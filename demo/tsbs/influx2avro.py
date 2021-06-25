#!env python3
import avro.schema
import fileinput
import os
import re
import sys
import json
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
from string import Template


def get_schema(fields):
    return \
        Template("""[
    {
        "type": "array",
        "items": {
            "type": "record",
            "name": "update",
            "namespace": "com.materialize.cdc",
            "fields": [
                {
                    "name": "data",
                    "type": {
                        "type": "record",
                        "name": "data",
                        "fields": [
                            $fields
                        ]
                    }
                },
                {
                    "name": "time",
                    "type": "long"
                },
                {
                    "name": "diff",
                    "type": "long"
                }
            ]
        }
    },
    {
        "type": "record",
        "name": "progress",
        "namespace": "com.materialize.cdc",
        "fields": [
            {
                "name": "lower",
                "type": {
                    "type": "array",
                    "items": "long"
                }
            },
            {
                "name": "upper",
                "type": {
                    "type": "array",
                    "items": "long"
                }
            },
            {
                "name": "counts",
                "type": {
                    "type": "array",
                    "items": {
                        "type": "record",
                        "name": "counts",
                        "fields": [
                            {
                                "name": "time",
                                "type": "long"
                            },
                            {
                                "name": "count",
                                "type": "long"
                            }
                        ]
                    }
                }
            }
        ]
    }
]
""").substitute(fields=fields)


row_data_template = Template("{\"$key\": \"$value\"}")

row_template = Template("{\"array\":[{\"data\":[$data],\"time\":time,\"diff\":diff}]}")

extract_key_value_re = re.compile(r"(.*)=([^i]*)i?")


def format_row(headers, fields, time, diff):
    # data = []
    data = {}
    for (key, value) in zip(headers, fields):
        data[key] = value
    return {"array": [{"data": data, "time": time, "diff": diff}]}
    # return [{"data": data, "time": time, "diff": diff}]
    #     data.append(row_data_template.substitute(key=key, value=value))
    # return row_template.substitute(data=",".join(data))


header = []
header_ready = False
old_values = {}
previous_time = 0
time_count = 0


for line in fileinput.input():
    buffer = []
    (key, value, time) = line.strip().split(' ')
    time = int(int(time) / 1000000)
    for key_item in key.split(','):
        key_value_group = extract_key_value_re.match(key_item)
        if key_value_group:
            buffer.append(key_value_group.group(2))
            if not header_ready:
                header.append(key_value_group.group(1))
        else:
            buffer.append(key_item)
            if header is not None:
                header.append(key_item)

    for key_item in value.split(','):
        key_value_group = extract_key_value_re.match(key_item)
        assert key_value_group
        buffer.append(key_value_group.group(2))
        if not header_ready:
            header.append(key_value_group.group(1))

    if not header_ready:
        fields = []
        for head in header:
            fields.append(
                Template("{\"name\": \"$head\", \"type\": \"string\"}").substitute(head=head))
        header_ready = True
        schema = avro.schema.parse(get_schema(",".join(fields)))
        writer = DataFileWriter(os.fdopen(sys.stdout.fileno(), "wb", closefd=False), DatumWriter(),
                                schema)
        with open('/tmp/tsbs.schema', 'w') as f:
            print(str(schema), file=f)

    if previous_time != time:
        counts = []
        if time_count > 0:
            counts.append({"time": previous_time, "count": time_count})
        time_count = 0
        # writer.append({"lower": [previous_time], "upper": [time], "counts": counts})
        print(json.dumps({"com.materialize.cdc.progress":{"lower": [previous_time], "upper": [time], "counts": counts}}))
        # writer.append({"com.materialize.cdc.progress": {"lower": [previous_time], "upper": [time],
        #                                                 "counts": counts}})

        previous_time = time
    time_count += 1

    old_value = old_values.pop(key, None)
    if old_value:
        print(json.dumps(format_row(header, old_value, time, -1)))
        # writer.append(format_row(header, old_value, time, -1))
    print(json.dumps(format_row(header, buffer, time, 1)))
    # writer.append(format_row(header, buffer, time, 1))
    old_values[key] = buffer
