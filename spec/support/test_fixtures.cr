# Shared protobuf messages and helpers used across specs

class GreetRequest
  include Protobuf::Message

  contract_of "proto2" do
    optional :name, :string, 1
  end
end

class GreetResponse
  include Protobuf::Message

  contract_of "proto2" do
    optional :message, :string, 1
  end
end

# Minimal concrete service for testing handle() dispatch.
# We write the handle method by hand (mirroring what the `rpc` macro generates)
# because the macro requires the RPC name to be a resolved constant.
class EchoService
  include GRPC::Service

  @@service_name = "echo.EchoService"

  def handle(method_name : String, request_body : IO)
    if method_name == "SayHello"
      say_hello(GreetRequest.from_protobuf(request_body))
    else
      previous_def(method_name, request_body)
    end
  end

  def say_hello(request : GreetRequest) : GreetResponse
    GreetResponse.new(message: "Hello, #{request.name}!")
  end
end

# Subclass that skips the real TCP connection so we can unit-test
# pure header-sorting logic without a server.
class TestableClient < GRPC::Client
  def initialize(
    authority : String = "localhost:50051",
    scheme : String = "http",
    default_headers : HTTP::Headers = HTTP::Headers.new,
  )
    @authority = authority
    @scheme = scheme
    @default_headers = default_headers
    @requests = {} of Stream => Channel(Nil)
    @bidirectional_streams = {} of Stream => Channel(Bytes)
    @connection = uninitialized Connection
  end

  def sorted_headers_for_test(headers : HTTP::Headers)
    sorted_headers_with_defaults(headers)
  end
end

# Client over an in-memory HTTP/2 connection. The spec plays the server by
# writing raw frames to the other end of the pipe; the loop is started by hand.
class PipedClient < GRPC::Client
  def initialize(io : IO)
    @authority = "localhost:50051"
    @scheme = "http"
    @default_headers = HTTP::Headers.new
    @requests = {} of Stream => Channel(Nil)
    @bidirectional_streams = {} of Stream => Channel(Bytes)
    @connection = Connection.new(io, Connection::Type::CLIENT)
  end

  def run_loop
    handle_connection
  end
end

# Writes a DATA frame for *stream_id* as a server would put it on the wire
def write_data_frame(io : IO, stream_id : Int32, payload : Bytes, end_stream = false)
  io.write_bytes((payload.size.to_u32 << 8) | HTTP2::Frame::Type::DATA.value.to_u32, IO::ByteFormat::BigEndian)
  io.write_byte(end_stream ? HTTP2::Frame::Flags::END_STREAM.value : 0_u8)
  io.write_bytes(stream_id.to_u32, IO::ByteFormat::BigEndian)
  io.write(payload)
  io.flush
end

# Writes a HEADERS frame for *stream_id* as a server would put it on the wire (END_HEADERS set)
def write_headers_frame(io : IO, stream_id : Int32, headers : HTTP::Headers, end_stream = false)
  payload = HTTP2::HPACK::Encoder.new.encode(headers)
  flags = HTTP2::Frame::Flags::END_HEADERS
  flags |= HTTP2::Frame::Flags::END_STREAM if end_stream
  io.write_bytes((payload.size.to_u32 << 8) | HTTP2::Frame::Type::HEADERS.value.to_u32, IO::ByteFormat::BigEndian)
  io.write_byte(flags.value)
  io.write_bytes(stream_id.to_u32, IO::ByteFormat::BigEndian)
  io.write(payload)
  io.flush
end

# A frame as the spec's fake server reads it off the wire
record WireFrame, type : HTTP2::Frame::Type, flags : HTTP2::Frame::Flags, stream_id : Int32, payload : Bytes do
  def end_stream?
    flags.end_stream?
  end

  # Increment carried by a WINDOW_UPDATE frame
  def window_increment
    IO::ByteFormat::BigEndian.decode(UInt32, payload).to_i32 & 0x7fffffff
  end
end

# Reads one frame the client wrote, or nil once the client side is closed
def read_wire_frame(io : IO) : WireFrame?
  header = Bytes.new(9)
  return if io.read_fully?(header).nil?
  length = (header[0].to_i32 << 16) | (header[1].to_i32 << 8) | header[2].to_i32
  type = HTTP2::Frame::Type.new(header[3].to_i32)
  flags = HTTP2::Frame::Flags.new(header[4])
  stream_id = IO::ByteFormat::BigEndian.decode(UInt32, header[5, 4]).to_i32 & 0x7fffffff
  payload = Bytes.new(length)
  io.read_fully(payload)
  WireFrame.new(type, flags, stream_id, payload)
end

# Writes a RST_STREAM frame for *stream_id* as a server would put it on the wire
def write_rst_stream_frame(io : IO, stream_id : Int32, code = HTTP2::Error::Code::INTERNAL_ERROR)
  io.write_bytes((4_u32 << 8) | HTTP2::Frame::Type::RST_STREAM.value.to_u32, IO::ByteFormat::BigEndian)
  io.write_byte(0_u8)
  io.write_bytes(stream_id.to_u32, IO::ByteFormat::BigEndian)
  io.write_bytes(code.value.to_u32, IO::ByteFormat::BigEndian)
  io.flush
end
