require "./spec_helper"

# Client that reports when its read loop ends and exposes the socket it opened
private class WatchedClient < GRPC::Client
  getter loop_done = Channel(Nil).new(1)

  def tcp
    @tcp
  end

  private def handle_connection
    super
  ensure
    @loop_done.send(nil)
  end
end

private enum PeerMode
  # completes the preface and settings, then neither writes nor closes, as a powered-off host
  Silent
  # completes the preface and settings, then closes its side
  HangUp
  # opens every flow-control window wide, then never reads, so a writer blocks in the socket write
  Stalled
end

# Accepts connections, completes the HTTP/2 preface, then behaves as *mode* says
private class FakeServer
  getter port : Int32
  getter peers = [] of IO

  def initialize(@mode : PeerMode, @tls : OpenSSL::SSL::Context::Server? = nil)
    @server = TCPServer.new("127.0.0.1", 0)
    @port = @server.local_address.port
    spawn accept_loop
  end

  private def accept_loop
    while socket = @server.accept?
      spawn serve(socket)
    end
  end

  private def serve(socket : TCPSocket)
    io = socket.as(IO)
    if tls = @tls
      io = OpenSSL::SSL::Socket::Server.new(socket, tls, sync_close: true)
    end
    io.read_fully(Bytes.new(HTTP2::CLIENT_PREFACE.bytesize))
    read_wire_frame(io) # the client's SETTINGS
    if @mode.stalled?
      write_wide_windows(io)
    else
      io.write_bytes(HTTP2::Frame::Type::SETTINGS.value.to_u32, IO::ByteFormat::BigEndian)
      io.write_byte(0_u8)
      io.write_bytes(0_u32, IO::ByteFormat::BigEndian)
    end
    io.flush
    case @mode
    in .silent?, .stalled? then @peers << socket
    in .hang_up?
      # after the client's SETTINGS ack, so the hang-up meets the running loop and not the handshake
      while frame = read_wire_frame(io)
        break if frame.type.settings? && frame.flags.ack?
      end
      socket.close
    end
  rescue IO::Error | OpenSSL::Error
    socket.close
  end

  # SETTINGS INITIAL_WINDOW_SIZE = 2^31-1 plus a connection WINDOW_UPDATE to the same size
  private def write_wide_windows(io : IO) : Nil
    max_window = Int32::MAX.to_u32
    io.write_bytes((6_u32 << 8) | HTTP2::Frame::Type::SETTINGS.value, IO::ByteFormat::BigEndian)
    io.write_byte(0_u8)
    io.write_bytes(0_u32, IO::ByteFormat::BigEndian)
    io.write_bytes(HTTP2::Settings::Identifier::INITIAL_WINDOW_SIZE.value.to_u16, IO::ByteFormat::BigEndian)
    io.write_bytes(max_window, IO::ByteFormat::BigEndian)

    io.write_bytes((4_u32 << 8) | HTTP2::Frame::Type::WINDOW_UPDATE.value, IO::ByteFormat::BigEndian)
    io.write_byte(0_u8)
    io.write_bytes(0_u32, IO::ByteFormat::BigEndian)
    io.write_bytes(max_window - HTTP2::DEFAULT_INITIAL_WINDOW_SIZE.to_u32, IO::ByteFormat::BigEndian)
  end

  def close
    @server.close
    @peers.each { |peer| peer.close rescue nil }
    @peers.clear
  end
end

private TLS_DIR = File.tempname("grpc-spec-tls")

private def server_tls : OpenSSL::SSL::Context::Server
  key, cert = File.join(TLS_DIR, "key.pem"), File.join(TLS_DIR, "cert.pem")
  unless File.exists?(cert)
    Dir.mkdir_p(TLS_DIR)
    Process.run("openssl", ["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
                            "-subj", "/CN=localhost", "-keyout", key, "-out", cert])
  end
  context = OpenSSL::SSL::Context::Server.new
  context.private_key = key
  context.certificate_chain = cert
  context.alpn_protocol = "h2"
  context
end

private def client_tls : OpenSSL::SSL::Context::Client
  context = OpenSSL::SSL::Context::Client.new
  context.verify_mode = OpenSSL::SSL::VerifyMode::NONE
  context
end

private def open_fds : Int32
  Dir.children("/proc/self/fd").size
end

private def ended_within(channel, span = 1.second) : Bool
  select
  when channel.receive then true
  when timeout(span) then false
  end
end

describe GRPC::Client do
  describe "#close" do
    {false, true}.each do |tls|
      label = tls ? "TLS" : "plain"

      it "closes the socket it opened (#{label})" do
        server = FakeServer.new(:silent, tls ? server_tls : nil)
        client = WatchedClient.new("127.0.0.1", server.port, tls ? client_tls : nil)
        client.tcp.not_nil!.closed?.should be_false

        client.close

        client.tcp.not_nil!.closed?.should be_true
        client.closed?.should be_true
      ensure
        server.try(&.close)
      end

      it "ends the read loop when the peer has gone silent (#{label})" do
        server = FakeServer.new(:silent, tls ? server_tls : nil)
        client = WatchedClient.new("127.0.0.1", server.port, tls ? client_tls : nil)
        Fiber.yield # let the loop block in read

        client.close

        ended_within(client.loop_done).should be_true
      ensure
        server.try(&.close)
      end

      it "closes its socket when the peer hangs up (#{label})" do
        server = FakeServer.new(:hang_up, tls ? server_tls : nil)
        client = WatchedClient.new("127.0.0.1", server.port, tls ? client_tls : nil)

        ended_within(client.loop_done).should be_true

        client.tcp.not_nil!.closed?.should be_true
        client.closed?.should be_true
        client.close # closing again is harmless
      ensure
        server.try(&.close)
      end

      it "returns while a request body is blocked on a peer that stopped reading (#{label})" do
        server = FakeServer.new(:stalled, tls ? server_tls : nil)
        client = WatchedClient.new("127.0.0.1", server.port, tls ? client_tls : nil)
        sending = Channel(Exception?).new(1)
        spawn do
          # far more than both kernel buffers hold, with every window open
          client.send(HTTP::Headers{":method" => "POST", ":path" => "/stalled"}, Bytes.new(64 * 1024 * 1024)) { }
          sending.send(nil)
        rescue ex
          sending.send(ex)
        end
        sleep 200.milliseconds # until the writer is parked in the socket write, holding the write mutex

        closed = Channel(Nil).new(1)
        spawn do
          client.close
          closed.send(nil)
        end

        ended_within(closed, GRPC::Client::CLOSE_TIMEOUT + 1.second).should be_true
        select
        when result = sending.receive
          result.should be_a(IO::Error)
        when timeout(1.second)
          fail "the blocked request did not end"
        end
      ensure
        server.try(&.close)
      end
    end

    it "leaks no file descriptors across many clients" do
      server = FakeServer.new(:silent)
      before = open_fds

      200.times do
        client = WatchedClient.new("127.0.0.1", server.port)
        client.close
        ended_within(client.loop_done).should be_true
      end

      # what remains open is the silent server's own ends, one per client
      open_fds.should eq(before + 200)
      server.close
      open_fds.should eq(before - 1)
    ensure
      server.try(&.close)
    end
  end
end
