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

  describe "#update" do
    let(:archive) { Whispr.new(StringIO.new("")) }
    subject { archive }

    context "passing no arguments" do
      it "should not call update_one, or update_many" do
        archive.should_not_receive(:update_one)
        archive.should_not_receive(:update_many)
        archive.update()
      end
      it do
        expect { archive.update() }.to_not raise_error
      end
    end

    context "passing a single item list" do
      it "should not call update_one, or update_many" do
        archive.should_not_receive(:update_one)
        archive.should_not_receive(:update_many)
        archive.update(Time.new.to_i)
      end
      it do
        expect { archive.update(Time.new.to_i) }.to_not raise_error
      end
    end

    context "passing two item list" do
      it "should call #update_one" do
        args = [Time.new.to_i, 10]
        archive.should_receive(:update_one).with(*args.reverse)
        archive.should_not_receive(:update_many)
        archive.update(*args)
      end
    end

    context "passing a three item list" do
      it "should not call update_one, or update_many" do
        archive.should_not_receive(:update_one)
        archive.should_not_receive(:update_many)
        archive.update(Time.new.to_i, 10, Time.new.to_i)
      end
      it do
        expect { archive.update(Time.new.to_i, 10, Time.new.to_i) }.to_not raise_error
      end
    end
    context "passing a four item list" do
      it "should call #udpate_many" do
        args = [Time.new.to_i, 10, Time.new.to_i, 20]
        archive.should_not_receive(:update_one)
        archive.should_receive(:update_many).with(args)
        archive.update(*args)
      end
    end

    context "passing an array" do
      context "with two elements" do
        it "should call #update_one" do
          args = [Time.new.to_i, 10]
          archive.should_receive(:update_one).with(*args.reverse)
          archive.should_not_receive(:update_many)
          archive.update(args)
        end
      end
      context "with four elements" do
        it "should call #update_many" do
          args = [Time.new.to_i, 10, Time.new.to_i, 20]
          archive.should_not_receive(:update_one)
          archive.should_receive(:update_many).with(args)
          archive.update(args)
        end
      end
      context "with a single element" do
        it "should not call update_one, or update_many" do
          archive.should_not_receive(:update_one)
          archive.should_not_receive(:update_many)
          archive.update(Time.new.to_i)
        end
        it do
          expect { archive.update(Time.new.to_i) }.to_not raise_error
        end
      end

      context "with three elements" do
        it "should call #update_many" do
          archive.should_not_receive(:update_one)
          archive.should_not_receive(:update_many)
          archive.update([Time.new.to_i, 10, Time.new.to_i])
        end
        it do
          expect { archive.update([Time.new.to_i, 10, Time.new.to_i]) }.to_not raise_error
        end
      end

      context "with no elements" do
        it "should not call update_one, or update_many" do
          archive.should_not_receive(:update_one)
          archive.should_not_receive(:update_many)
          archive.update([])
        end
        it do
          expect { archive.update([]) }.to_not raise_error
        end
      end
    end

    context "passing an array and a list of elements" do
      context "when the array is the first element" do
        it "should call #update_many with all of the elements" do
          args = [[Time.new.to_i, 10, Time.new.to_i, 20], Time.new.to_i, 30]
          archive.should_not_receive(:update_one)
          archive.should_receive(:update_many).with(args.flatten)
          archive.update(*args)
        end
      end

      context "when the array is the last element" do
        it "should call #update_many with all of the elements" do
          args = [Time.new.to_i, 10, [Time.new.to_i, 20, Time.new.to_i, 30]]
          archive.should_not_receive(:update_one)
          archive.should_receive(:update_many).with(args.flatten)
          archive.update(*args)
        end
      end

      context "when the array is in the middle" do
        it "should call #update_many with all of the elements" do
          args = [Time.new.to_i, 10, [Time.new.to_i, 20, Time.new.to_i, 30], Time.new.to_i, 40]
          archive.should_not_receive(:update_one)
          archive.should_receive(:update_many).with(args.flatten)
          archive.update(*args)
        end
      end

      context "when there is more than one array" do
        it "should call #update_many with all of the elements" do
          args = [Time.new.to_i, 10, [Time.new.to_i, 20], [Time.new.to_i, 30], Time.new.to_i, 40]
          archive.should_not_receive(:update_one)
          archive.should_receive(:update_many).with(args.flatten)
          archive.update(*args)
        end
      end

      context "the number of elements plus the array is an odd number" do
        it "should call #update_many with all of the elements" do
          args = [[Time.new.to_i, 10, Time.new.to_i, 20], Time.new.to_i, 30, Time.new.to_i, 40]
          archive.should_not_receive(:update_one)
          archive.should_receive(:update_many).with(args.flatten)
          archive.update(*args)
        end
      end

      context "there is an uneven total number of elements" do
        context "one of the nested arrays is uneven" do
          it "should not call #update_many, or #update_one" do
            archive.should_not_receive(:update_one)
            archive.should_not_receive(:update_many)
            archive.update(Time.new.to_i, 10, [Time.new.to_i], [Time.new.to_i, 30], Time.new.to_i, 40)
          end
        end
        context "the element list is uneven" do
          it "should not call #update_many, or #update_one" do
            archive.should_not_receive(:update_one)
            archive.should_not_receive(:update_many)
            archive.update(Time.new.to_i, 10, [Time.new.to_i, 20], [Time.new.to_i, 30], Time.new.to_i)
          end
        end
      end
    end
  end

end
