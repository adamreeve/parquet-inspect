import argparse
import json
from pathlib import Path
import struct
from typing import Tuple

from thrift.transport import TTransport
from thrift.protocol import TCompactProtocol

from parquet_inspect.generated.parquet.ttypes import FileMetaData, PageHeader


def main():
    parser = argparse.ArgumentParser(
                    prog='parquet-inspect',
                    description='Parses Parquet file metadata')
    parser.add_argument('file_path', type=Path)

    args = parser.parse_args()

    inspect(args.file_path)


def inspect(file_path: Path):
    with open(file_path, 'rb') as f:
        data = f.read()

    with memoryview(data) as data:
        footer_len = data[-8:-4]
        footer_len, = struct.unpack('<l', footer_len)
        footer = data[-footer_len - 8:-8]

        metadata = read_file_metadata(footer)
        print(f"### File metadata")
        dump(metadata)

        if metadata.row_groups is None:
            return

        for row_group_idx, row_group in enumerate(metadata.row_groups):
            print(f"### Row group {row_group_idx}")
            for col in row_group.columns:
                print(f"### Column '{'.'.join(col.meta_data.path_in_schema)}'")

                for page_offset in [
                        col.meta_data.index_page_offset,
                        col.meta_data.dictionary_page_offset]:
                    if page_offset is not None:
                        print(f"### Page at {page_offset}")
                        header, _ = read_data_page(data[page_offset:])
                        dump(header)

                total_values = col.meta_data.num_values
                remaining_values = total_values
                data_page_offset = col.meta_data.data_page_offset

                while remaining_values > 0 and data_page_offset is not None:
                    print(f"### Data page at {data_page_offset}")
                    header, header_size = read_data_page(data[data_page_offset:])
                    dump(header)

                    if header.data_page_header is not None:
                        num_values = header.data_page_header.num_values
                    elif header.data_page_header_v2 is not None:
                        num_values = header.data_page_header_v2.num_values
                    else:
                        raise Exception(f"Page at {data_page_offset} is not a data page or v2 data page")

                    remaining_values -= num_values
                    data_page_offset = data_page_offset + header.compressed_page_size + header_size


def read_file_metadata(footer: memoryview) -> FileMetaData:
    transport = TTransport.TMemoryBuffer(footer)
    protocol = TCompactProtocol.TCompactProtocol(transport)

    metadata = FileMetaData()
    metadata.read(protocol)

    return metadata


def read_data_page(data_page: memoryview) -> Tuple[PageHeader, int]:
    transport = TTransport.TMemoryBuffer(data_page)
    protocol = TCompactProtocol.TCompactProtocol(transport)

    header = PageHeader()
    header.read(protocol)

    # Is there a way to get the header size without using a private member?
    header_size = transport._buffer.tell()

    return header, header_size


def dump(obj):
    print(json.dumps(obj, indent=2, default=get_dumpable))


def get_dumpable(obj):
    if hasattr(obj, '__dict__'):
        return obj.__dict__

    return repr(obj)
