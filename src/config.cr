# shamelesly stolen from Kemal's config pattern
module GRPC
  VERSION = {{ `shards version "#{__DIR__}"`.chomp.stringify }}

  class Config
    DEFAULTS = Config.new

    property http2 : HTTP2::Client? = nil

    def initialize
      if (host = ENV.fetch("GRPC_HOST", nil)) && (port = ENV.fetch("GRPC_PORT", nil).try(&.to_i?))
        tls = case ENV.fetch("GRPC_TLS")
              when "true", "1", "yes"
                true
              else
                false
              end

        @http2 = HTTP2::Client.new(host, port, tls)
      end
    end

    def self.defaults(&)
      yield DEFAULTS
    end

    def self.defaults
      DEFAULTS
    end
  end
end
