module GRPC
  class BadStatus < Exception
    getter code : StatusCode

    def initialize(@code : StatusCode, @message : String = "")
    end

    # doing it this way adds the StatusCode to everything (inspect_with_backtrace, etc)
    def message
      "#{@code}: #{super}"
    end
  end
end
