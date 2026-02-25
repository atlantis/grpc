require "./spec_helper"

describe GRPC::Service do
  describe "#handle" do
    it "dispatches by RPC name to the implementing method" do
      service = EchoService.new
      request = GreetRequest.new(name: "world")
      body = IO::Memory.new(request.to_protobuf.to_slice)

      response = service.handle("SayHello", body)

      response.should be_a(GreetResponse)
      response.as(GreetResponse).message.should eq("Hello, world!")
    end

    it "passes nil name field through to the implementation" do
      service = EchoService.new
      body = IO::Memory.new(GreetRequest.new.to_protobuf.to_slice)

      response = service.handle("SayHello", body)
      response.as(GreetResponse).message.should eq("Hello, !")
    end

    it "raises InvalidMethodName for an unknown RPC name" do
      service = EchoService.new
      body = IO::Memory.new(Bytes.empty)

      expect_raises(GRPC::Service::InvalidMethodName) do
        service.handle("NoSuchMethod", body)
      end
    end

    it "InvalidMethodName message includes the service type and method name" do
      service = EchoService.new
      body = IO::Memory.new(Bytes.empty)

      begin
        service.handle("Ghost", body)
        fail "expected InvalidMethodName to be raised"
      rescue ex : GRPC::Service::InvalidMethodName
        msg = ex.message.not_nil!
        msg.should contain("Ghost")
        msg.should contain("EchoService")
      end
    end
  end

  describe GRPC::Service::InvalidMethodName do
    it "is a subclass of GRPC::Service::Error" do
      ex = GRPC::Service::InvalidMethodName.new("test")
      ex.should be_a(GRPC::Service::Error)
    end

    it "is an Exception so it can be raised and rescued generically" do
      expect_raises(Exception) do
        raise GRPC::Service::InvalidMethodName.new("oops")
      end
    end
  end
end
