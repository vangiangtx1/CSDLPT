import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import SlidingWindows

# Hàm parse dữ liệu từ stream A
def parse_stream_a(elem):
    user_id, name, ts = elem.split(",")
    return beam.window.TimestampedValue((int(user_id), name), int(ts))

# Hàm parse dữ liệu từ stream B
def parse_stream_b(elem):
    user_id, order, ts = elem.split(",")
    return beam.window.TimestampedValue((int(user_id), order), int(ts))

# Format kết quả sau join
def format_result(joined):
    user_id, (a_val, b_val) = joined
    return f"{user_id}: {a_val} - {b_val}"

# Dữ liệu giả lập stream A (user info)
stream_a_data = [
    "1,Alice,1717730000",
    "2,Bob,1717730005",
    "3,Carol,1717730010",
]

# Dữ liệu giả lập stream B (order info)
stream_b_data = [
    "1,Order#1,1717730002",
    "2,Order#2,1717730010",
    "3,Order#3,1717730015",
]

options = PipelineOptions()

with beam.Pipeline(options=options) as p:
    # Stream A xử lý
    windowed_a = (
        p
        | "Stream A: Create" >> beam.Create(stream_a_data)
        | "Stream A: Parse" >> beam.Map(parse_stream_a)
        | "Window A" >> beam.WindowInto(SlidingWindows(size=10, period=5))
        | "Key A" >> beam.Map(lambda x: (x[0], x[1]))  # user_id làm key
    )

    # Stream B xử lý
    windowed_b = (
        p
        | "Stream B: Create" >> beam.Create(stream_b_data)
        | "Stream B: Parse" >> beam.Map(parse_stream_b)
        | "Window B" >> beam.WindowInto(SlidingWindows(size=10, period=5))
        | "Key B" >> beam.Map(lambda x: (x[0], x[1]))
    )

    # Join bằng user_id và loại bỏ trùng
    joined = (
        {"A": windowed_a, "B": windowed_b}
        | "CoGroupByKey" >> beam.CoGroupByKey()
        | "Filter and Join" >> beam.FlatMap(
            lambda x: [
                (x[0], (a, b))
                for a in x[1]["A"]
                for b in x[1]["B"]
            ]
        )
        | "Remove Duplicates" >> beam.Distinct()
        | "Format Output" >> beam.Map(format_result)
        | "Print Output" >> beam.Map(print)
    )
