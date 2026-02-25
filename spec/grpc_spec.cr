require "./spec_helper"

describe GRPC do
  describe ".encode_protobuf" do
    it "produces output with a 5-byte gRPC prefix" do
      encoded = GRPC.encode_protobuf(GreetRequest.new(name: "test"))
      encoded.size.should be >= 5
    end

    it "sets compression flag byte to 0 (uncompressed)" do
      encoded = GRPC.encode_protobuf(GreetRequest.new(name: "anything"))
      encoded[0].should eq(0_u8)
    end

    it "encodes payload length as big-endian Int32 in bytes 1–4" do
      encoded = GRPC.encode_protobuf(GreetRequest.new(name: "test"))
      declared_length = IO::ByteFormat::BigEndian.decode(Int32, encoded[1, 4])
      declared_length.should eq(encoded.size - 5)
    end

    it "produces a 5-byte output (header only) for a default-value message" do
      encoded = GRPC.encode_protobuf(GreetRequest.new)
      # proto2 optional field with nil value serializes to zero bytes
      declared_length = IO::ByteFormat::BigEndian.decode(Int32, encoded[1, 4])
      declared_length.should eq(0)
      encoded.size.should eq(5)
    end
  end

  describe ".decode_protobuf" do
    it "roundtrips a message with a string field" do
      original = GreetRequest.new(name: "world")
      encoded = GRPC.encode_protobuf(original)
      decoded = GRPC.decode_protobuf(IO::Memory.new(encoded), GreetRequest)
      decoded.name.should eq("world")
    end

    it "roundtrips a default-value (nil field) message" do
      encoded = GRPC.encode_protobuf(GreetRequest.new)
      decoded = GRPC.decode_protobuf(IO::Memory.new(encoded), GreetRequest)
      decoded.name.should be_nil
    end

    it "roundtrips the response type independently" do
      original = GreetResponse.new(message: "Hello, world!")
      encoded = GRPC.encode_protobuf(original)
      decoded = GRPC.decode_protobuf(IO::Memory.new(encoded), GreetResponse)
      decoded.message.should eq("Hello, world!")
    end
  end
end
