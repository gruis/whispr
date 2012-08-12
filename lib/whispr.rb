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
  class InvalidTimeInterval < WhisprError; end
  class TimestampNotCovered < WhisprError; end
  class InvalidAggregationMethod < WhisprError; end
  class ArchiveBoundaryExceeded < WhisprError; end

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

  class << self
    def create(path, archiveList, opts = {})
      opts = {:xff => 0.5, :aggregationMethod => :average, :sparse => false}.merge(opts)
    end
  end

  # @return [File, StringIO] file handle of the whisper file
  attr_reader :fh

  attr_accessor :auto_flush
  alias :auto_flush? :auto_flush

  def initialize(file, auto_flush = true)
    @fh = file.is_a?(File) || file.is_a?(StringIO) ? file : File.open(file, 'r+')
    @fh.binmode
    @auto_flush = auto_flush
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


  # Retrieve values from a whisper file within the given time window.
  #
  # The most appropriate archive within the whisper file will be chosen. The
  # return value will be a two element Array.  The first element will be a
  # three element array containing the start time, end time and step. The
  # second element will be a N element array containing each value at each
  # step period.
  #
  # @see Archive#fetch
  def fetch(fromTime, untilTime = Time.new)
    fromTime  = fromTime.to_i
    untilTime = untilTime.to_i
    now       = Time.now.to_i
    oldest    = header[:maxRetention]
    fromTime  = oldest if fromTime < oldest
    raise InvalidTimeInterval.new("Invalid time interval") unless fromTime < untilTime
    untilTime = now if untilTime > now || untilTime < fromTime

    diff    = now - fromTime
    archive = archives.find{|a| a.retention >= diff }
    return archive.fetch(fromTime, untilTime)
  end

  def update(*points)
    return if points.empty?
    # TODO lock the file
    if points.length == 1
      update_one(points[0][1], points[0][0])
    else
      update_many(points)
    end
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

  def update_one(value, timestamp = nil)
    now       = Time.new.to_i
    timestamp = now if timestamp.nil?
    diff      = now - timestamp
    if !(diff < header[:maxRetention] && diff >= 0)
      raise TimestampNotCovered, "Timestamp not covered by any archives in this database"
    end

    aidx = (0 ... archives.length).find { |i| archives[i].retention > diff }
    archive       = archives[aidx]
    lowerArchives = archives[aidx + 1 .. - 1]

    myInterval    = timestamp - (timestamp % archive.spp)
    myPackedPoint = [myInterval, value].pack(POINT_FMT)
    @fh.seek(archive.offset)
    baseInterval, baseValue = @fh.read(POINT_SIZE).unpack(POINT_FMT)

    if baseInterval == 0
      # this file's first update
      @fh.seek(archive.offset)
      @fh.write(myPackedPoint)
      baseInterval, baseValue = myInterval, value
    else
      timeDistance  = myInterval - baseInterval
      pointDistance = timeDistance / archive.spp
      byteDistance  = pointDistance * POINT_SIZE
      myOffset      = archive.offset + (byteDistance % archive.size)
      @fh.seek(myOffset)
      @fh.write(myPackedPoint)
    end

    higher = archive
    lowerArchives.each do |lower|
      break unless propagate(myInterval, higher, lower)
      higher = lower
    end

    @fh.flush if auto_flush?
  end

  def update_many(points)
    # order points by timestamp, newest first
    points   = points.map{|ts, v| [ts.to_i, v.to_f ] }.sort {|b,a| a[0] <=> b[0] }
    now            = Time.new.to_i
    archives       = self.archives.to_enum
    currentArchive = archives.next
    currentPoints  = []
    points.each do |point|
      age = now - point[0]
      while currentArchive.retention < age
        unless currentPoints.empty?
          currentPoints.reverse! # put points in chronological order
          currentArchive.update_many(currentPoints)
          currentPoints = []
        end
        begin
          currentArchive = archives.next
        rescue StopIteration
          currentArchive = nil
          break
        end
      end
      # drop remaining points that don't fit in the database
      break unless currentArchive

      currentPoints << point
    end

    if currentArchive && !currentPoints.empty?
      # don't forget to commit after we've checked all the archives
      currentPoints.reverse!
      currentArchive.update_many(currentPoints)
    end

    @fh.flush if auto_flush?
  end

  def propagate(timestamp, higher, lower)
    aggregationMethod = header[:aggregationMethod]
    xff               = header[:xFilesFactor]

    lowerIntervalStart = timestamp - (timestamp % lower.spp)
    lowerIntervalEnd   = lowerIntervalStart + lower.spp
    @fh.seek(higher.offset)
    higherBaseInterval, higherBaseValue = @fh.read(POINT_SIZE).unpack(POINT_FMT)

    if higherBaseInterval == 0
      higherFirstOffset = higher.offset
     else
       timeDistance = lowerIntervalStart - higherBaseInterval
       pointDistance = timeDistance / higher.spp
       byteDistance  = pointDistance * POINT_SIZE
       higherFirstOffset = higher.offset + (byteDistance % higher.size)
    end

    higherPoints        = lower.spp / higher.spp
    higherSize          = higherPoints * POINT_SIZE
    relativeFirstOffset = higherFirstOffset - higher.offset
    relativeLastOffset  = (relativeFirstOffset + higherSize) % higher.size
    higherLastOffset    = relativeLastOffset + higher.offset
    @fh.seek(higherFirstOffset)

    if higherFirstOffset < higherLastOffset
      # don't wrap the archive
      seriesString = @fh.read(higherLastOffset - higherFirstOffset)
    else
      # wrap the archive
      higherEnd    = higher.offset + higher.size
      seriesString = @fh.read(higherEnd - higherFirstOffset)
      @fh.seek(higher.offset)
      seriesString += @fh.read(higherLastOffset - higher.offset)
    end

    points         = seriesString.length / POINT_SIZE
    unpackedSeries = seriesString.unpack(POINT_FMT * points)

    # construct a list of values
    neighborValues  = points.times.map{}
    currentInterval = lowerIntervalStart
    step            = higher.spp
    (0..unpackedSeries.length).step(2) do |i|
      pointTime           = unpackedSeries[i]
      neighborValues[i/2] = unpackedSeries[i+1] if pointTime == currentInterval
      currentInterval    += step
    end

    knownValues = neighborValues.select { |v| !v.nil? }
    return false if knownValues.empty?
    if (knownValues.length / neighborValues.length).to_f < header[:xFilesFactor]
      return false
    end

    # we have enough data to propagate a value
    aggregateValue = aggregate(aggregationMethod, knownValues)
    myPackedPoint  = [lowerIntervalStart, aggregateValue].pack(POINT_FMT)
    @fh.seek(lower.offset)
    lowerBaseInterval, lowerBaseValue = @fh.read(POINT_SIZE).unpack(POINT_FMT)

    if lowerBaseInterval == 0
      # first propagated update to this lower archive
      @fh.seek(lower.offset)
      @fh.write(myPackedPoint)
    else
      timeDistance  = lowerIntervalStart - lowerBaseInterval
      pointDistance = timeDistance / lower.spp
      byteDistance  = pointDistance * POINT_SIZE
      lowerOffset   = lower.offset + (byteDistance % lower.size)
      @fh.seek(lowerOffset)
      @fh.write(myPackedPacket)
    end
    true
  end

  def aggregate(aggregationMethod, knownValues)
    case aggregationMethod
    when :average
      (knownVaues.inject(0){|sum, i| sum + i } / knownValues.length).to_f
    when :sum
      knownVaues.inject(0){|sum, i| sum + i }
    when :last
      knownValues[-1]
    when :max
      v  = knownValues[0]
      knownValues[1..-1].each { |k| v = k if k > v }
      v
    when :min
      v  = knownValues[0]
      knownValues[1..-1].each { |k| v = k if k < v }
      v
    else
      raise InvalidAggregationMethod, "Unrecognized aggregation method #{aggregationMethod}"
    end
  end

end
