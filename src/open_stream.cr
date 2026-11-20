module GRPC
  class Client
    # A bidirectional stream from `Client#open_stream`. Each DATA frame's bytes arrive on `#receive`,
    # which the client closes when the stream or the connection ends. The connection loop waits for
    # `#receive` to be read, so keep reading it while sending: a send waiting for WINDOW_UPDATE
    # needs the loop to run.
    class OpenStream
      getter receive = Channel(Bytes).new

      # `send_data` sends a message frame by frame; two at once could interleave them
      @send_lock = Mutex.new

      protected def initialize(@client : Client, @stream : Stream)
      end

      # Sends one gRPC message, already framed (as from `GRPC.encode_protobuf`). Waits for the
      # flow-control windows; raises `IO::Error` once the stream, our sending side or the
      # connection is closed.
      def send(data : Bytes) : Nil
        write(data, Frame::Flags::None)
      end

      # Ends our side (END_STREAM); the server may keep sending on `#receive`
      def close_send : Nil
        write(Bytes.empty, Frame::Flags::END_STREAM)
      end

      # Cancels the stream (RST_STREAM CANCEL) and stops delivering to `#receive`
      def close : Nil
        return if closed?
        @client.forget(@stream)
        @stream.send_rst_stream(Error::Code::CANCEL) rescue nil
        @receive.close
      end

      def closed? : Bool
        !@stream.active? || @client.closed?
      end

      private def write(data : Bytes, flags : Frame::Flags) : Nil
        @send_lock.synchronize { @stream.send_data(data, flags) }
      rescue ex : HTTP2::Error
        raise IO::Error.new("stream #{@stream.id}: #{ex.message}", cause: ex)
      end
    end
  end
end
