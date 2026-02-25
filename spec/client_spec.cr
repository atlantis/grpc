require "./spec_helper"

# TestableClient is defined in spec/support/test_fixtures.cr.
# It subclasses GRPC::Client, skips the TCP connection, and exposes
# sorted_headers_with_defaults as a public method for testing.

describe GRPC::Client do
  describe "#sorted_headers_with_defaults (header ordering)" do
    it "always places :authority and :scheme before regular headers" do
      client = TestableClient.new(authority: "example.com:443", scheme: "https")
      headers = HTTP::Headers{
        "content-type" => "application/grpc",
        ":method"      => "POST",
        ":path"        => "/svc/Method",
      }

      result = client.sorted_headers_for_test(headers)
      keys = result.keys

      # Pseudo-headers must all precede regular headers
      last_pseudo = keys.rindex { |k| k.starts_with?(":") } || -1
      first_regular = keys.index { |k| !k.starts_with?(":") }

      if first_regular
        last_pseudo.should be < first_regular
      end
    end

    it "injects :authority from the authority argument" do
      client = TestableClient.new(authority: "myhost:1234")
      result = client.sorted_headers_for_test(HTTP::Headers.new)
      result[":authority"].should eq("myhost:1234")
    end

    it "injects :scheme from the scheme argument" do
      client = TestableClient.new(scheme: "https")
      result = client.sorted_headers_for_test(HTTP::Headers.new)
      result[":scheme"].should eq("https")
    end

    it "passes caller-supplied pseudo-headers through" do
      client = TestableClient.new
      headers = HTTP::Headers{
        ":method" => "POST",
        ":path"   => "/pkg.Service/Method",
      }
      result = client.sorted_headers_for_test(headers)
      result[":method"].should eq("POST")
      result[":path"].should eq("/pkg.Service/Method")
    end

    it "passes caller-supplied regular headers through" do
      client = TestableClient.new
      headers = HTTP::Headers{
        "content-type" => "application/grpc",
        "te"           => "trailers",
      }
      result = client.sorted_headers_for_test(headers)
      result["content-type"].should eq("application/grpc")
      result["te"].should eq("trailers")
    end

    it "lets default_headers supply regular headers that appear after pseudo ones" do
      defaults = HTTP::Headers{"grpc-accept-encoding" => "identity"}
      client = TestableClient.new(default_headers: defaults)
      result = client.sorted_headers_for_test(HTTP::Headers.new)
      result["grpc-accept-encoding"].should eq("identity")
    end

    it "caller headers override default_headers for the same key" do
      defaults = HTTP::Headers{"content-type" => "application/grpc+proto"}
      client = TestableClient.new(default_headers: defaults)
      call_headers = HTTP::Headers{"content-type" => "application/grpc"}
      result = client.sorted_headers_for_test(call_headers)
      result["content-type"].should eq("application/grpc")
    end

    it "default_headers pseudo-headers are overridden by caller pseudo-headers" do
      defaults = HTTP::Headers{":method" => "GET"}
      client = TestableClient.new(default_headers: defaults)
      call_headers = HTTP::Headers{":method" => "POST"}
      result = client.sorted_headers_for_test(call_headers)
      result[":method"].should eq("POST")
    end
  end
end
