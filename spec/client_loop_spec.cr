require "./spec_helper"

private def piped_client
  client_io, server_io = IO::Stapled.pipe
  client = PipedClient.new(client_io)
  done = Channel(Nil).new
  spawn do
    client.run_loop
  ensure
    done.send(nil)
  end
  {client, server_io, done}
end

private def stream_headers
  HTTP::Headers{":method" => "POST", ":path" => "/svc/Watch", "content-type" => "application/grpc"}
end

# Plays a flow-control-abiding server for stream 1: waits for the request, then writes *reply* in
# DATA frames without ever exceeding the stream window, growing it only on the client's
# WINDOW_UPDATEs. Without those the reply stalls at the initial window, as etcd does.
private def serve_unary(server_io, reply : Bytes, trailers : HTTP::Headers? = nil)
  window_updates = Channel(Int32).new(16)
  spawn do
    request_done = false
    while frame = read_wire_frame(server_io)
      if frame.stream_id == 1 && frame.type.data? && frame.end_stream?
        request_done = true
        write_reply(server_io, reply, trailers, window_updates)
      elsif frame.stream_id == 1 && frame.type.window_update?
        window_updates.send(frame.window_increment)
      end
    end
  end
end

private def write_reply(server_io, reply : Bytes, trailers, window_updates)
  spawn do
    window = HTTP2::DEFAULT_INITIAL_WINDOW_SIZE
    chunk = HTTP2::DEFAULT_MAX_FRAME_SIZE
    offset = 0
    while offset < reply.size
      # fill the window exactly, as a real server does, then wait for it to grow
      while window == 0
        window += window_updates.receive
      end
      size = {chunk, reply.size - offset, window}.min
      last = offset + size == reply.size
      write_data_frame(server_io, 1, reply[offset, size], end_stream: last && trailers.nil?)
      window -= size
      offset += size
    end
    write_headers_frame(server_io, 1, trailers, end_stream: true) if trailers
  end
end

describe GRPC::Client do
  describe "#handle_connection" do
    it "hands over exactly the bytes of each DATA frame" do
      client, server_io, _ = piped_client
      channel = client.open_stream(stream_headers)
      stream_id = client.connection.streams.find(1).id

      write_data_frame(server_io, stream_id, "abc".to_slice)
      String.new(channel.receive).should eq("abc")

      write_data_frame(server_io, stream_id, "defgh".to_slice)
      got = channel.receive
      got.size.should eq(5)
      String.new(got).should eq("defgh")

      write_data_frame(server_io, stream_id, "i".to_slice)
      String.new(channel.receive).should eq("i")

      client.close
    end

    it "keeps serving other streams after a consumer closes its channel" do
      client, server_io, done = piped_client
      abandoned = client.open_stream(stream_headers)
      abandoned.close
      write_data_frame(server_io, 1, "dropped".to_slice)

      live = client.open_stream(stream_headers)
      write_data_frame(server_io, 3, "still here".to_slice)
      String.new(live.receive).should eq("still here")

      select
      when done.receive
        fail "connection loop died"
      when timeout(50.milliseconds)
      end

      client.close
    end

    it "ends the loop on a protocol error instead of raising out of the fiber" do
      client, server_io, done = piped_client
      # DATA on a stream that was never opened is a protocol error
      write_data_frame(server_io, 1, "x".to_slice)

      select
      when done.receive
      when timeout(1.second)
        fail "connection loop did not end"
      end

      client.close
    end
  end

  describe "#send" do
    it "receives a unary reply larger than the stream window" do
      client, server_io, _ = piped_client
      client.request_timeout = 2.seconds
      reply = Bytes.new(200_000) { |i| (i % 251).to_u8 }
      serve_unary(server_io, reply)

      client.send(stream_headers, "req".to_slice) do |_headers, _trailers, body|
        body.gets_to_end.to_slice.should eq(reply)
      end

      client.close
    end

    it "keeps the whole reply when trailers close the stream" do
      client, server_io, _ = piped_client
      client.request_timeout = 2.seconds
      reply = Bytes.new(100_000) { |i| (i % 13).to_u8 }
      serve_unary(server_io, reply, HTTP::Headers{"grpc-status" => "0"})

      client.send(stream_headers, "req".to_slice) do |_headers, trailers, body|
        trailers["grpc-status"].should eq("0")
        body.gets_to_end.to_slice.should eq(reply)
      end

      client.close
    end

    it "yields what arrived when the server resets the stream" do
      client, server_io, _ = piped_client
      client.request_timeout = 2.seconds
      spawn do
        while frame = read_wire_frame(server_io)
          if frame.stream_id == 1 && frame.type.data? && frame.end_stream?
            write_data_frame(server_io, 1, "partial".to_slice)
            write_rst_stream_frame(server_io, 1)
          end
        end
      end

      client.send(stream_headers, "req".to_slice) do |_headers, _trailers, body|
        body.gets_to_end.should eq("partial")
      end

      client.close
    end

    it "raises IO::TimeoutError when the server never answers" do
      client, _, _ = piped_client
      client.request_timeout = 50.milliseconds

      expect_raises(IO::TimeoutError, /no answer within/) do
        client.send(stream_headers, "req".to_slice) { }
      end

      client.close
    end
  end
end
