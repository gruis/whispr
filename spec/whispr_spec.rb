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

  it { Whispr.should respond_to :validArchiveList? }
  describe "Whispr.validArchiveList?" do
    pending
  end

  it { Whispr.should respond_to :validateArchiveList }
  describe "Whispr.validateArchiveList" do
    pending
  end

  it { Whispr.should respond_to :validateArchiveList! }
  describe "Whispr.validateArchiveList!" do
    pending
  end

  it { Whispr.should respond_to :create }
  describe "Whispr.create"  do
    pending
  end

end
