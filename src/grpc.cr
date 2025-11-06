require "protobuf"
require "http2"
require "socket"
require "openssl"

require "./config"
require "./client"
require "./status_codes"
require "./errors"
require "./service"

module GRPC
  # grpc-encod a protobuf object by prefixing compression and length
  def self.encode_protobuf(object : Protobuf::Message)
    io = IO::Memory.new

    request_payload = object.to_protobuf.to_slice

    io.write_bytes(0_u8) # Not compressed
    io.write_bytes(request_payload.size, IO::ByteFormat::BigEndian)
    io.write request_payload

    io.to_slice
  end

  # decode a grpc-encoded protobuf by stripping off compression and length prefix
  def self.decode_protobuf(io, klass : Protobuf::Message.class)
      compressed = io.read_byte != 0
      size = io.read_bytes(Int32, IO::ByteFormat::BigEndian)
      bytes = Bytes.new(size)
      io.read(bytes)
      payload = IO::Memory.new(bytes)
      klass.from_protobuf(payload)
  end
end