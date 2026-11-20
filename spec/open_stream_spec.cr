require "./spec_helper"

private def piped_client
  client_io, server_io = IO::Stapled.pipe
  client = PipedClient.new(client_io)
  spawn client.run_loop
  {client, server_io}
end

private def watch_headers
  HTTP::Headers{":method" => "POST", ":path" => "/svc/Watch", "content-type" => "application/grpc"}
end

private def within(span = 2.seconds, &block)
  done = Channel(Exception?).new(1)
  spawn do
    block.call
    done.send(nil)
  rescue ex
    done.send(ex)
  end
  select
  when error = done.receive
    raise error if error
  when timeout(span)
    fail "timed out"
  end
end

# Echo server for stream 1: answers each gRPC message with "re:" + message, grants back every
# DATA byte it reads, and reports the frames it saw on *seen*
private def serve_echo(server_io, seen : Channel(WireFrame)? = nil)
  spawn do
    reader = GrpcMessageReader.new
    answered = false
    while frame = read_wire_frame(server_io)
      seen.try(&.send(frame))
      next unless frame.stream_id == 1 && frame.type.data?
      unless answered
        write_headers_frame(server_io, 1, HTTP::Headers{":status" => "200", "content-type" => "application/grpc"})
        answered = true
      end
      if frame.payload.size > 0
        write_window_update_frame(server_io, 0, frame.payload.size)
        write_window_update_frame(server_io, 1, frame.payload.size)
      end
      reader.feed(frame.payload) do |message|
        write_data_frame(server_io, 1, grpc_frame("re:".to_slice + message))
      end
      if frame.end_stream?
        write_data_frame(server_io, 1, grpc_frame("bye".to_slice))
        write_headers_frame(server_io, 1, HTTP::Headers{"grpc-status" => "0"}, end_stream: true)
      end
    end
  end
end

private def message_of(bytes : Bytes) : String
  messages = [] of String
  GrpcMessageReader.new.feed(bytes) { |m| messages << String.new(m) }
  messages.join("|")
end

describe GRPC::Client::OpenStream do
  it "sends later messages on the stream and receives their answers" do
    client, server_io = piped_client
    serve_echo(server_io)

    within do
      open = client.open_stream(watch_headers, grpc_frame("first".to_slice))
      message_of(open.receive.receive).should eq("re:first")

      open.send(grpc_frame("second".to_slice))
      message_of(open.receive.receive).should eq("re:second")
    end

    client.close
  end

  it "half-closes with close_send and still receives the server's answer" do
    client, server_io = piped_client
    seen = Channel(WireFrame).new(64)
    serve_echo(server_io, seen)

    within do
      open = client.open_stream(watch_headers, grpc_frame("hello".to_slice))
      message_of(open.receive.receive).should eq("re:hello")

      open.close_send
      message_of(open.receive.receive).should eq("bye")
      open.receive.receive?.should be_nil # trailers closed the stream
      open.closed?.should be_true
    end

    frames = [] of WireFrame
    while frame = (select
          when f = seen.receive then f
          else
            nil
          end)
      frames << frame
    end
    last = frames.select { |f| f.stream_id == 1 && f.type.data? }.last
    last.end_stream?.should be_true
    last.payload.size.should eq(0)

    expect_raises(IO::Error) { client.open_stream(watch_headers).tap(&.close_send).send(grpc_frame("x".to_slice)) }

    client.close
  end

  it "ends receive when the server finishes the call while our side is open" do
    client, server_io = piped_client
    seen = Channel(WireFrame).new(64)
    spawn do
      while frame = read_wire_frame(server_io)
        seen.send(frame)
        if frame.stream_id == 1 && frame.type.data?
          write_headers_frame(server_io, 1, HTTP::Headers{":status" => "200", "content-type" => "application/grpc"})
          write_data_frame(server_io, 1, grpc_frame("last".to_slice))
          write_headers_frame(server_io, 1, HTTP::Headers{"grpc-status" => "14"}, end_stream: true)
        end
      end
    end

    open = client.open_stream(watch_headers, grpc_frame("watch".to_slice))
    within do
      message_of(open.receive.receive).should eq("last")
      open.receive.receive?.should be_nil
      loop do
        frame = seen.receive
        next unless frame.type.rst_stream?
        frame.stream_id.should eq(1)
        IO::ByteFormat::BigEndian.decode(UInt32, frame.payload).should eq(HTTP2::Error::Code::NO_ERROR.value)
        break
      end
    end

    open.closed?.should be_true
    client.@bidirectional_streams.should be_empty
    expect_raises(IO::Error, /closed/) { open.send(grpc_frame("late".to_slice)) }

    client.close
  end

  it "resets the stream on close, forgets it and refuses later sends" do
    client, server_io = piped_client
    seen = Channel(WireFrame).new(64)
    serve_echo(server_io, seen)

    open = client.open_stream(watch_headers)
    open.close
    open.closed?.should be_true
    open.receive.closed?.should be_true

    within do
      loop do
        frame = seen.receive
        next unless frame.type.rst_stream?
        frame.stream_id.should eq(1)
        IO::ByteFormat::BigEndian.decode(UInt32, frame.payload).should eq(HTTP2::Error::Code::CANCEL.value)
        break
      end
    end

    client.@bidirectional_streams.should be_empty
    expect_raises(IO::Error, /closed/) { open.send(grpc_frame("late".to_slice)) }

    client.close
  end

  it "waits for WINDOW_UPDATE instead of overrunning the initial window" do
    client, server_io = piped_client
    window = HTTP2::DEFAULT_INITIAL_WINDOW_SIZE
    big = grpc_frame(Bytes.new(window * 2) { |i| (i % 251).to_u8 })
    received = Channel(Int32).new(64)
    spawn do
      while frame = read_wire_frame(server_io)
        received.send(frame.payload.size) if frame.stream_id == 1 && frame.type.data?
      end
    end

    open = client.open_stream(watch_headers)
    sent = Channel(Nil).new(1)
    spawn do
      open.send(big)
      sent.send(nil)
    end

    total = 0
    while total < window
      total += received.receive
    end
    total.should eq(window)
    select
    when received.receive
      fail "sent past the window"
    when sent.receive
      fail "send returned before the whole message went out"
    when timeout(100.milliseconds)
    end

    write_window_update_frame(server_io, 0, big.size)
    write_window_update_frame(server_io, 1, big.size)
    within do
      while total < big.size
        total += received.receive
      end
      sent.receive
    end
    total.should eq(big.size)

    client.close
  end

  it "raises IO::Error to a send waiting for a window when the stream is closed" do
    client, server_io = piped_client
    spawn { while read_wire_frame(server_io); end }
    open = client.open_stream(watch_headers)

    failed = Channel(Exception?).new(1)
    spawn do
      open.send(grpc_frame(Bytes.new(HTTP2::DEFAULT_INITIAL_WINDOW_SIZE * 2)))
      failed.send(nil)
    rescue ex
      failed.send(ex)
    end
    sleep 50.milliseconds

    open.close
    within { failed.receive.should be_a(IO::Error) }

    client.close
  end

  it "keeps the connection after DATA the server sent before seeing our reset" do
    client, server_io = piped_client
    spawn { while read_wire_frame(server_io); end }

    open = client.open_stream(watch_headers)
    open.close
    write_data_frame(server_io, 1, "in flight".to_slice)

    live = client.open_stream(watch_headers)
    write_data_frame(server_io, 3, "still here".to_slice)
    within { String.new(live.receive.receive).should eq("still here") }
    client.closed?.should be_false

    client.close
  end

  it "closes the client and fails a waiting send when the connection drops" do
    client, server_io = piped_client
    spawn { while read_wire_frame(server_io); end }
    open = client.open_stream(watch_headers)

    failed = Channel(Exception?).new(1)
    spawn do
      open.send(grpc_frame(Bytes.new(HTTP2::DEFAULT_INITIAL_WINDOW_SIZE * 2)))
      failed.send(nil)
    rescue ex
      failed.send(ex)
    end
    sleep 50.milliseconds

    server_io.close
    within { failed.receive.should be_a(IO::Error) }
    client.closed?.should be_true
    open.closed?.should be_true
  end

  it "delivers 1000 sends from one fiber in order while another reads" do
    client, server_io = piped_client
    serve_echo(server_io)
    count = 1000
    payload = "x" * 200

    open = client.open_stream(watch_headers)
    answers = [] of String
    reader_done = Channel(Nil).new(1)
    spawn do
      reader = GrpcMessageReader.new
      while answers.size < count && (bytes = open.receive.receive?)
        reader.feed(bytes) { |m| answers << String.new(m) }
      end
      reader_done.send(nil)
    end

    within(10.seconds) do
      count.times { |i| open.send(grpc_frame("#{i}:#{payload}".to_slice)) }
      reader_done.receive
    end

    answers.should eq(Array.new(count) { |i| "re:#{i}:#{payload}" })

    client.close
  end
end
