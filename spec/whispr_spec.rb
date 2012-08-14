require "whispr"

describe Whispr do

  it { Whispr.should respond_to :parse_retention_def }

  describe "Whispr.parse_retention_def" do
    it "should require precision and points seperated by a ':'" do
      expect {
        Whispr.parse_retention_def("now")
      }.to raise_error(ArgumentError)
    end
  end

end
