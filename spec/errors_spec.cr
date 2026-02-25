require "./spec_helper"

describe GRPC::BadStatus do
  describe "#code" do
    it "stores the provided status code" do
      err = GRPC::BadStatus.new(GRPC::StatusCode::NOT_FOUND, "thing missing")
      err.code.should eq(GRPC::StatusCode::NOT_FOUND)
    end
  end

  describe "#message" do
    it "prefixes the message with the status code name" do
      err = GRPC::BadStatus.new(GRPC::StatusCode::PERMISSION_DENIED, "not allowed")
      err.message.should eq("PERMISSION_DENIED: not allowed")
    end

    it "works with an empty message string" do
      err = GRPC::BadStatus.new(GRPC::StatusCode::INTERNAL, "")
      err.message.should eq("INTERNAL: ")
    end

    it "defaults to an empty message when none is given" do
      err = GRPC::BadStatus.new(GRPC::StatusCode::UNKNOWN)
      err.message.should eq("UNKNOWN: ")
    end
  end

  it "is an Exception subclass so it can be raised and rescued" do
    expect_raises(GRPC::BadStatus) do
      raise GRPC::BadStatus.new(GRPC::StatusCode::INTERNAL, "boom")
    end
  end

  it "can be rescued as a generic Exception" do
    rescued_code = nil
    begin
      raise GRPC::BadStatus.new(GRPC::StatusCode::UNAVAILABLE, "down")
    rescue ex : Exception
      rescued_code = ex.message
    end
    rescued_code.should eq("UNAVAILABLE: down")
  end
end
