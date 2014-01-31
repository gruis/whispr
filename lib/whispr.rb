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
  class ValueError < WhisprError; end
  class InvalidConfiguration < WhisprError; end

  LONG_FMT          = "N"
  METADATA_FMT      = "#{LONG_FMT*2}g#{LONG_FMT}"
  METADATA_SIZE     = 16
  ARCHIVE_INFO_FMT  = LONG_FMT * 3
  ARCHIVE_INFO_SIZE =  12
  POINT_FMT         = "#{LONG_FMT}G"
  POINT_SIZE        = 12
  CHUNK_SIZE        = 16384

  AGGR_TYPES = [
    :_,
    :average,
    :sum,
    :last,
    :max,
    :min
  ].freeze

  class << self

    def unitMultipliers
      @unitMultipliers ||= {
        's' => 1,
        'm' => 60,
        'h' => 3600,
        'd' => 86400,
        'w' => 86400 * 7,
        'y' => 86400 * 365
      }
    end

    def parse_retention_def(rdef)
      raise ArgumentError.new("precision and points must be separated by a ':'") unless rdef && rdef.include?(":")
      (precision, points) = rdef.strip.split(':')
      if precision.to_i.to_s == precision
        precision = precision.to_i * unitMultipliers['s']
      else
        _, precision, unit = precision.split(/([\d]+)/)
        unit = 's' unless unit
        raise ValueError.new("Invalid precision specification unit #{unit}") unless unitMultipliers[unit[0]]
        precision = precision.to_i * unitMultipliers[unit[0]]
      end

      if points.to_i.to_s == points
        points = points.to_i
      else
        _, points, unit = points.split(/([\d]+)/)
        raise ValueError.new("Invalid retention specification unit #{unit}") unless unitMultipliers[unit[0]]
        points = points.to_i * unitMultipliers[unit[0]] / precision
      end

      [precision, points]
    end

    # Create whipser file on the file system and prepopulate it.
    # @param [String] path
    # @param [Array] archiveList each archive is an array with two elements: [secondsPerPoint,numberOfPoints]
    # @param [Hash] opts
    # @option opts [Float] :xff (0.5) the fraction of data points in a propagation interval that must have known values for a propagation to occur
    # @option opts [Symbol] :aggregationMethod (average) the function to use when propogating data; must be one of AGGR_TYPES[1..-1]
    # @option opts [Boolean] :overwrite (false)
    # @option opts [Boolean] :sparse (false)
    # @raise [InvalidConfiguration] if the archiveList is inavlid, or if 'path' exists and :overwrite is not true
    # @see Whsipr.validateArchiveList
    def create(path, archiveList, opts = {})
      validate_opts(opts)
      validateArchiveList!(archiveList)
      raise InvalidConfiguration.new("File #{path} already exists!") if File.exists?(path) && !opts[:overwrite]

      # if file exists it will be truncated
      File.open(path, "wb")  do |fh|
        fh.flock(File::LOCK_EX)
        prepopulate(fh, archiveList, opts)
      end

      new(path)
    end

    # Set defaults for the options to #create and #prepopulate as well as validate the supplied options.
    # @param [Hash] opts
    # @return [Hash] updated options
    def validate_opts(opts = {})
      opts = {:xff => 0.5, :aggregationMethod => :average, :sparse => false, :overwrite => false}.merge(opts)
      unless AGGR_TYPES[1..-1].include?(opts[:aggregationMethod])
        raise InvalidConfiguration.new("aggregationMethod must be one of #{AGGR_TYPES[1..-1]}")
      end
      opts
    end

    # Build the header and reserve space for the archives in the Whispr file.
    #
    # You probably don't want to use this method, you probably want to use
    # #create instead. Calls to prepopulate MUST be preceeded by a call to
    # validateArchiveList! with the archiveList argument.
    #
    # @param [File] the filehandle that will hold the archive
    # @param [Array] archiveList each archive is an array with two elements: [secondsPerPoint,numberOfPoints]
    # @param [Hash] opts
    # @option opts [Float] :xff the fraction of data points in a propagation interval that must have known values for a propagation to occur
    # @option opts [Symbol] :aggregationMethod the function to use when propogating data; must be one of AGGR_TYPES[1..-1]
    # @option opts [Boolean] :overwrite (false)
    # @raise [InvalidConfiguration] if the archiveList is inavlid, or if 'path' exists and :overwrite is not true
    # @see Whsipr.validateArchiveList
    # @see Whsipr.create
    def prepopulate(fh, archiveList, opts = {})
      opts            = validate_opts(opts)
      aggregationType = AGGR_TYPES.index(opts[:aggregationMethod])
      oldest         = archiveList.map{|spp, points| spp * points }.sort.last
      packedMetadata = [aggregationType, oldest, opts[:xff], archiveList.length].pack(METADATA_FMT)
      fh.write(packedMetadata)
      headerSize            = METADATA_SIZE + (ARCHIVE_INFO_SIZE * archiveList.length)
      archiveOffsetPointer = headerSize
      archiveList.each do |spp, points|
        archiveInfo = [archiveOffsetPointer, spp, points].pack(ARCHIVE_INFO_FMT)
        fh.write(archiveInfo)
        archiveOffsetPointer += (points * POINT_SIZE)
      end

      if opts[:sparse]
        fh.seek(archiveOffsetPointer - headerSize - 1)
        fh.write("\0")
      else
        remaining = archiveOffsetPointer - headerSize
        zeroes = "\x00" * CHUNK_SIZE
        while remaining > CHUNK_SIZE
          fh.write(zeroes)
          remaining -= CHUNK_SIZE
        end
        fh.write(zeroes[0..remaining])
      end

      fh.flush
      fh.fsync rescue nil
      fh
    end

    # Is the provided archive list valid?
    # @return [Boolean] true, false
    def validArchiveList?(archiveList)
      !(!!(validateArchiveList!(archiveList) rescue true))
    end

    # Validate an archive list without raising an exception
    # @return [NilClass, InvalidConfiguration]
    def validateArchiveList(archiveList)
      validateArchiveList!(archiveList) rescue $!
    end

    # Validate an archive list
    # An ArchiveList must:
    # 1. Have at least one archive config. Example: [60, 86400]
    # 2. No archive may be a duplicate of another.
    # 3. Higher precision archives' precision must evenly divide all lower precision archives' precision.
    # 4. Lower precision archives must cover larger time intervals than higher precision archives.
    # 5. Each archive must have at least enough points to consolidate to the next archive
    # @raise [InvalidConfiguration]
    # @return [nil]
    def validateArchiveList!(archiveList)
      raise InvalidConfiguration.new("you must specify at least on archive configuration") if Array(archiveList).empty?
      archiveList = archiveList.sort{|a,b| a[0] <=> b[0] }
      archiveList[0..-2].each_with_index do |archive, i|
        nextArchive = archiveList[i+1]
        unless archive[0] < nextArchive[0]
          raise InvalidConfiguration.new("A Whipser database may not be configured " +
            "having two archives with the same precision " +
            "(archive#{i}: #{archive}, archive#{i+1}: #{nextArchive})")
        end
        unless nextArchive[0] % archive[0] == 0
          raise InvalidConfiguration.new("Higher precision archives' precision must " +
            "evenly divide all lower precision archives' precision " +
            "(archive#{i}: #{archive}, archive#{i+1}: #{nextArchive})")
        end

        retention = archive[0] * archive[1]
        nextRetention = nextArchive[0] * nextArchive[1]
        unless nextRetention > retention
          raise InvalidConfiguration.new("Lower precision archives must cover larger " +
            "time intervals than higher precision archives " +
            "(archive#{i}: #{archive[1]}, archive#{i + 1}:, #{nextArchive[1]})")
        end

        archivePoints = archive[1]
        pointsPerConsolidation = nextArchive[0] / archive[0]
        unless archivePoints >= pointsPerConsolidation
          raise InvalidConfiguration.new("Each archive must have at least enough points " +
            "to consolidate to the next archive (archive#{i+1} consolidates #{pointsPerConsolidation} of " +
            "archive#{i}'s points but it has only #{archivePoints} total points)")
        end
      end
      nil
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
    oldest    = now - header[:maxRetention]
    fromTime  = oldest if fromTime < oldest
    raise InvalidTimeInterval.new("Invalid time interval") unless fromTime < untilTime
    untilTime = now if untilTime > now || untilTime < fromTime

    diff    = now - fromTime
    archive = archives.find{|a| a.retention >= diff }
    return archive.fetch(fromTime, untilTime)
  end

  # Update one or many points
  # Each element of the points list should be a two dimensional Array where
  # the first element is a timestamp and the second element is a value.
  def update(*points)
    if points[0].is_a?(Array)
      # Cover the least exhaustive, and most likely, nested array check first
      points = points.length == 1 ? points[0] : points.flatten
    elsif points.any? { |p| p.is_a?(Array) }
      points = points.flatten
    end
    return if points.empty? || points.length % 2 != 0

    # TODO lock the file
    if points.length == 2
      update_one(points[1], points[0])
    else
      update_many(points)
    end
  end

  def closed?
    @fh.closed?
  end

  def close
    @fh.close
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
    rescue IOError => e
      raise e.extend(Error)
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
      raise TimestampNotCovered, "Timestamp (#{timestamp}) not covered by any archives in this database"
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
    points   = points.each_slice(2).map{|ts, v| [ts.to_i, v.to_f ] }.sort {|b,a| a[0] <=> b[0] }
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
       timeDistance      = lowerIntervalStart - higherBaseInterval
       pointDistance     = timeDistance / higher.spp
       byteDistance      = pointDistance * POINT_SIZE
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
    if (knownValues.length.to_f / neighborValues.length.to_f) < header[:xFilesFactor]
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
      @fh.write(myPackedPoint)
    end
    true
  end

  def aggregate(aggregationMethod, knownValues)
    case aggregationMethod
    when :average
      (knownValues.inject(0){|sum, i| sum + i } / knownValues.length).to_f
    when :sum
      knownValues.inject(0){|sum, i| sum + i }
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
