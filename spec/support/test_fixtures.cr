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
