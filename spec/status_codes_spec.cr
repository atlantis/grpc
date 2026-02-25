require "./spec_helper"

describe GRPC::StatusCode do
  describe "integer values" do
    it "maps standard gRPC status codes to their canonical integers" do
      GRPC::StatusCode::OK.value.should eq(0)
      GRPC::StatusCode::CANCELLED.value.should eq(1)
      GRPC::StatusCode::UNKNOWN.value.should eq(2)
      GRPC::StatusCode::INVALID_ARGUMENT.value.should eq(3)
      GRPC::StatusCode::DEADLINE_EXCEEDED.value.should eq(4)
      GRPC::StatusCode::NOT_FOUND.value.should eq(5)
      GRPC::StatusCode::ALREADY_EXISTS.value.should eq(6)
      GRPC::StatusCode::PERMISSION_DENIED.value.should eq(7)
      GRPC::StatusCode::RESOURCE_EXHAUSTED.value.should eq(8)
      GRPC::StatusCode::INTERNAL.value.should eq(13)
      GRPC::StatusCode::UNAVAILABLE.value.should eq(14)
      GRPC::StatusCode::UNAUTHENTICATED.value.should eq(16)
    end
  end

  describe ".from_value?" do
    it "returns the matching member for known values" do
      GRPC::StatusCode.from_value?(0).should eq(GRPC::StatusCode::OK)
      GRPC::StatusCode.from_value?(5).should eq(GRPC::StatusCode::NOT_FOUND)
      GRPC::StatusCode.from_value?(13).should eq(GRPC::StatusCode::INTERNAL)
      GRPC::StatusCode.from_value?(16).should eq(GRPC::StatusCode::UNAUTHENTICATED)
    end

    it "returns nil for values that have no matching member" do
      GRPC::StatusCode.from_value?(17).should be_nil
      GRPC::StatusCode.from_value?(999).should be_nil
    end
  end

  describe "predicate methods" do
    it "ok? returns true only for OK" do
      GRPC::StatusCode::OK.ok?.should be_true
    end

    it "ok? returns false for error codes" do
      GRPC::StatusCode::CANCELLED.ok?.should be_false
      GRPC::StatusCode::UNKNOWN.ok?.should be_false
      GRPC::StatusCode::INTERNAL.ok?.should be_false
      GRPC::StatusCode::UNAUTHENTICATED.ok?.should be_false
    end

    it "each member's predicate identifies itself" do
      GRPC::StatusCode::NOT_FOUND.not_found?.should be_true
      GRPC::StatusCode::INTERNAL.internal?.should be_true
      GRPC::StatusCode::UNAVAILABLE.unavailable?.should be_true
    end
  end
end
