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
