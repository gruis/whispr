require 'whispr/version'
require 'whispr/archive'
require 'stringio'

class Whispr
  module Error; end
  class WhisprError < StandardError
    include Error
    def self.exception(e)
      return e if e.nil? || e == self
      ne = new(e.to_s)
      ne.set_backtrace e.backtrace if e.respond_to?(:backtrace)
      ne
    end
  end
  class CorruptWhisprFile < WhisprError; end

  METADATA_FMT      = "NNgN"
  METADATA_SIZE     = 16
  ARCHIVE_INFO_FMT  = "NNN"
  ARCHIVE_INFO_SIZE =  12
  POINT_FMT         = "NG"
  POINT_SIZE        = 12

  AGGR_TYPES = [
    :none,
    :average,
    :sum,
    :last,
    :max,
    :min
  ].freeze

  # @return [File, StringIO] file handle of the whisper file
  attr_reader :fh

  def initialize(file)
    @fh = file.is_a?(File) || file.is_a?(StringIO) ? file : File.open(file, 'r')
    @fh.binmode
  end

  # @return [Hash]
  def header
    @header ||= read_header
  end
  alias :info :header

  # @return [Array] Archives
  # @see Whispr::Archive
  def archives
    @archives ||= info[:archives].map { |a| Archive.new(self, a) }
  end

private

  def read_header
    o_pos = @fh.pos

    begin
      @fh.pos = 0
      metadata = @fh.read(METADATA_SIZE)
      aggr_type, max_retention, xff, arch_count = metadata.unpack(METADATA_FMT)
      archives = arch_count.times.map do |i|
        arch_info = @fh.read(ARCHIVE_INFO_SIZE)
        offset, s_per_pnt, points = arch_info.unpack(ARCHIVE_INFO_FMT)
        { :retention => s_per_pnt * points,
          :secondsPerPoint => s_per_pnt,
          :points => points,
          :size => points * POINT_SIZE,
          :offset => offset
        }
      end
    rescue => e
      raise CorruptWhisprFile.exception(e)
    ensure
      @fh.pos = o_pos
    end

    { :maxRetention      => max_retention,
      :xFilesFactor      => xff,
      :aggregationMethod => AGGR_TYPES[aggr_type],
      :archives          => archives
    }
  end
end
