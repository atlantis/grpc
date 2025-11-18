require "./status_codes"
require "./errors"

module GRPC
  module Service
    class Error < Exception
    end

    class InvalidMethodName < Error
    end

    macro included
      def self.service_name
        @@service_name
      end

      def handle(method_name : String, request_body : IO)
        raise InvalidMethodName.new("Unknown RPC method {{@type.id}}/#{method_name}")
      end

      macro rpc(name, receives request_type, returns response_type)
        \{% method_name = name.stringify.underscore.id %}
        abstract def \{{method_name}}(request : \{{request_type}}) : \{{response_type}}

        def handle(method_name : String, request_body : IO)
          if method_name == \{{name.stringify}}
            \{{method_name}}(\{{request_type}}.from_protobuf(request_body))
          else
            previous_def(method_name, request_body)
          end
        end

        class Stub < ::GRPC::Service::Stub({{@type.id}})
          def \{{method_name}}(request : \{{request_type}}) : \{{response_type}}
            io = IO::Memory.new

            request_payload = request.to_protobuf.to_slice

            io.write_bytes(0_u8) # Not compressed
            io.write_bytes(request_payload.size, IO::ByteFormat::BigEndian)
            io.write request_payload

            headers = HTTP::Headers {
              ":method" => "POST",
              ":path" => "/#{T.service_name}/\{{name}}",
              "content-type" => "application/grpc",
            }

            http2.send(headers, io.to_slice) do |response_headers, response_trailers, response_data|
              if raw_status = (response_headers["grpc-status"]? || response_trailers["grpc-status"]?).try(&.to_i?)
                status_code = GRPC::StatusCode.from_value?(raw_status) || GRPC::StatusCode::UNKNOWN
                if status_code.ok?
                  compressed = response_data.read_byte != 0 # TODO: Handle compression?
                  length = response_data.read_bytes Int32, IO::ByteFormat::BigEndian

                  return \{{response_type}}.from_protobuf(response_data)
                else
                  raise GRPC::BadStatus.new(
                    status_code,
                    response_headers["grpc-message"]? || response_trailers["grpc-message"]? || "Unknown error!"
                  )
                end

              else
                raise "Unable to get status code from response"
              end
            end

            raise "Unexpected response"
          end
        end
      end
    end

    class Stub(T)
      def initialize(@config : GRPC::Config? = nil)
      end


      @[Deprecated("Move to new config pattern")]
      def initialize(host : String, port : Int32)
        @config = Config.new(GRPC::Client.new(host, port))
      end

      def http2
        @config.try(&.http2) || Config.defaults.http2 || raise "No HTTP/2 client configured"
      end
    end
  end
end
