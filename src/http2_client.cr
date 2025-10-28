module GRPC
  class Client
    alias Connection = HTTP2::Connection
    alias Stream = HTTP2::Stream
    alias Frame = HTTP2::Frame
    alias Error = HTTP2::Error

    DEFAULT_DNS_TIMEOUT = 5.0
    DEFAULT_CONNECT_TIMEOUT = 5.0

    getter connection : Connection
    property default_headers : HTTP::Headers
    @requests = {} of Stream => Channel(Nil)

    def initialize(
      host : String,
      port : Int32,
      ssl_context : HTTP::Client::TLSContext = nil,
      dns_timeout = DEFAULT_DNS_TIMEOUT,
      connect_timeout = DEFAULT_CONNECT_TIMEOUT,
      @default_headers = HTTP::Headers.new
    )
      @authority = "#{host}:#{port}"

      io = TCPSocket.new(host, port, dns_timeout, connect_timeout)

      case ssl_context
      when true
        ssl_context = OpenSSL::SSL::Context::Client.new
        ssl_context.alpn_protocol = "h2"
        io = OpenSSL::SSL::Socket::Client.new(io, ssl_context, hostname: host.to_s)
        @scheme = "https"
      when OpenSSL::SSL::Context::Client
        ssl_context.alpn_protocol = "h2"
        io = OpenSSL::SSL::Socket::Client.new(io, ssl_context, hostname: host.to_s)
        @scheme = "https"
      else
        @scheme = "http"
      end

      connection = Connection.new(io, Connection::Type::CLIENT)
      connection.write_client_preface
      connection.write_settings

      frame = connection.receive
      unless frame.try(&.type) == Frame::Type::SETTINGS
        raise Error.protocol_error("Expected SETTINGS frame")
      end

      @connection = connection
      spawn handle_connection
    end

    private def handle_connection
      loop do
        unless frame = @connection.receive
          next
        end

        case frame.type
        when Frame::Type::HEADERS
          @requests[frame.stream].send(nil)
        when Frame::Type::PUSH_PROMISE
          # TODO: got SERVER PUSHed headers
        when Frame::Type::GOAWAY
          break
        end
      end
    end

    # Unary request that closes the stream via flags after sending the data
    def send(headers : HTTP::Headers, data : Bytes? = nil, &)
      stream = @connection.streams.create
      @requests[stream] = Channel(Nil).new

      headers = sorted_headers_with_defaults(headers)

      stream.send_headers(headers, flags: Frame::Flags::END_HEADERS)

      unless data.nil?
        stream.send_data(data, flags: Frame::Flags::END_STREAM)
      end

      while stream.active?
        @requests[stream].receive
      end

      yield stream.headers, stream.trailers? || HTTP::Headers.new, stream.data

      if stream.active?
        stream.send_rst_stream(Error::Code::NO_ERROR)
      end
    end

    def close
      @connection.close unless closed?
    end

    def closed?
      @connection.closed?
    end

    # HTTP2 requires pseudo headers (starting with colon) to be first
    protected def sorted_headers_with_defaults(headers : HTTP::Headers)
      # if you must, you can override these using default_headers
      sorted_headers = HTTP::Headers{
        ":authority" => @authority,
        ":scheme" => @scheme,
      }

      default_headers.each do |name, value|
        if name.starts_with?(":")
          sorted_headers[name] = value
        end
      end

      headers.each do |name, value|
        if name.starts_with?(":")
          sorted_headers[name] = value
        end
      end

      default_headers.each do |name, value|
        unless name.starts_with?(":")
          sorted_headers[name] = value
        end
      end

      headers.each do |name, value|
        unless name.starts_with?(":")
          sorted_headers[name] = value
        end
      end

      sorted_headers
    end
  end
end
