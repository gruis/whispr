class Whispr
  class Archive
    include Enumerable

    # @return [Hash] the archive header
    attr_reader :header
    # @return [Fixnum] the start location in the whisper file of this Archive
    attr_reader :offset
    # @return [Fixnum] the number of points in this archive
    attr_reader :points
    # @return [Fixnum] the total size of this archive (points * POINT_SIZE)
    attr_reader :size
    # @return [Fixnum] number of seconds worth of data retained by this archive
    attr_reader :retention
    # @return [Fixnum] seconds per point
    attr_reader :spp
    # @return [Whispr} the Whisper that contains this Archive
    attr_reader :whisper

    def initialize(whisper, header)
      @whisper   = whisper
      @header    = header
      @offset    = @header[:offset]
      @points    = @header[:points]
      @size      = @header[:size]
      @retention = @header[:retention]
      @spp       = @header[:secondsPerPoint]
      @eoa       = @size * @points + @offset
    end

    # Retrieve each point from the archive.
    #
    # If a block is provided each point is read directly from
    # the whisper file one at a time and yielded. If a block
    # is not provided, all points are read from the file and
    # returned as an enum.
    #
    # Each point is represented as a three element Array. The first
    # element is the index of the point. The second element is the
    # timestamp of the point and the third element is the value of
    # the point.
    def each(&blk)
      return slurp.to_enum unless block_given?
      o_pos = @whisper.fh.pos
      begin
        @whisper.fh.pos = @offset
        points.times {|i| yield(i, *next_point) }
      ensure
        @whisper.fh.pos = o_pos
      end
    end

    # Has the end of the archive been reached?
    def eoa?
      @whisper.fh.pos >= @eoa
    end

    def to_enum
      slurp.to_enum
    end

    # Retrieve the next point from the whisper file.
    # @api private
    def next_point
      return nil if @whisper.fh.pos >= @eoa || @whisper.fh.pos < @offset
      @whisper.fh.read(POINT_SIZE).unpack(POINT_FMT)
    end

    # Retrieve all points for this archive from the whisper file.
    #
    # Each point is represented as a three element Array. The first
    # element is the index of the point. The second element is the
    # timestamp of the point and the third element is the value of
    # the point.
    #
    # @return [Array]
    def slurp
      o_pos = @whisper.fh.pos
      @whisper.fh.pos = @offset
      data = @whisper.fh.read(@size).unpack(POINT_FMT * @points)
      @points.times.map { |i| [i, data.shift, data.shift] }
    ensure
      @whisper.fh.pos = o_pos
    end

    # Retrieve values for a time period from an archive within a whisper file
    #
    # The return value will be a two element Array.  The first element will be
    # a three element array containing the start time, end time and step. The
    # second element will be a N element array containing each value at each
    # step period.
    #
    # @see Whispr#fetch
    def fetch(fromTime, untilTime)
      fromInterval  = (fromTime - (fromTime % spp)) + spp
      untilInterval = (untilTime - (untilTime % spp)) + spp
      o_pos         = @whisper.fh.pos
      begin
        @whisper.fh.seek(offset)
        baseInterval, baseValue = @whisper.fh.read(POINT_SIZE).unpack(POINT_FMT)
        if baseInterval == 0
          step     = spp
          points   = (untilInterval - fromInterval) / step
          timeInfo = [fromInterval, untilInterval, step]
          return [timeInfo, points.times.map{}]
        end

        # Determine fromOffset
        timeDistance  = fromInterval - baseInterval
        pointDistance = timeDistance / spp
        byteDistance  = pointDistance * POINT_SIZE
        fromOffset    = offset + (byteDistance % size)

        # Determine untilOffset
        timeDistance  = untilInterval - baseInterval
        pointDistance = timeDistance / spp
        byteDistance  = pointDistance * POINT_SIZE
        untilOffset   = offset + (byteDistance % size)

        # Reall all the points in the interval
        @whisper.fh.seek(fromOffset)
        if fromOffset < untilOffset
          # we don't wrap around the archive
          series = @whisper.fh.read(untilOffset - fromOffset)
        else
          # we wrap around the archive, so we need two reads
          archiveEnd  = offset + size
          series      = @whisper.fh.read(archiveEnd - fromOffset)
          @whisper.fh.seek(offset)
          series     += @whisper.fh.read(untilOffset - offset)
        end

        points          = series.length / POINT_SIZE
        series          = series.unpack(POINT_FMT * points)
        currentInterval = fromInterval
        step            = spp
        valueList       = points.times.map{}
        (0..series.length).step(2) do |i|
          pointTime = series[i]
          if pointTime == currentInterval
            pointValue     = series[i+1]
            valueList[i/2] = pointValue
          end
          currentInterval += step
        end

        timeInfo = [fromInterval, untilInterval, step]
      ensure
        @whisper.fh.pos = o_pos
      end
      [timeInfo, valueList]
    end

    def update_many(points)
      step = spp
      alignedPoints = points.map { |ts, v| [(ts - (ts % step)), v] }
      # Create a packed string for each contiguous sequence of points
      packedStrings    = []
      previousInterval = nil
      currentString    = ''
      alignedPoints.each do |interval, value|
        next if interval == previousInterval
        if previousInterval.nil? || (interval == previousInterval + step)
          currentString   += [interval, value].pack(POINT_FMT)
        else
          numberOfPoints = currentString.length / POINT_SIZE
          startInterval  = previousInterval - (step * (numberOfPoints - 1))
          packedStrings << [startInterval, currentString]
          currentString  = [interval, value].pack(POINT_FMT)
        end
        previousInterval = interval
      end
      if !currentString.empty?
        numberOfPoints = currentString.length / POINT_SIZE
        startInterval = previousInterval - (step * (numberOfPoints - 1))
        packedStrings << [startInterval, currentString]
      end

      # Read base point and determine where our writes will start
      @whisper.fh.seek(offset)
      baseInterval, baseValue = @whisper.fh.read(POINT_SIZE).unpack(POINT_FMT)
      baseInterval = packedStrings[0][0] if baseInterval == 0
      packedStrings.each do |interval, packedString|
        timeDistance = interval - baseInterval
        pointDistance = timeDistance / step
        byteDistance = pointDistance * POINT_SIZE
        myOffset = offset + (byteDistance % size)
        @whisper.fh.seek(myOffset)
        archiveEnd = offset + size
        bytesBeyond = (myOffset + packedString.length) - archiveEnd

        if bytesBeyond > 0
          @whisper.fh.write(packedString[0..-bytesBeyond])
          if(@whisper.fh.pos != archiveEnd)
            raise ArchiveBoundaryExceeded.new("archiveEnd=#{archiveEnd} pos=#{@whisper.fh.pos} bytesBeyond=#{bytesBeyond} len(packedString)=#{packedString.length}")
          end
          @whisper.fh.seek(offset)
          @whisper.fh.write(packedString[-bytesBeyond..-1])
        else
          @whisper.fh.write(packedString)
        end
      end # interval, packedString|

      # Now we propagate the updates to the lower-precision archives
      higher = self
      @whisper.archives.select{|a| a.spp > spp }.each do |lower|
        lowerIntervals = alignedPoints.map{|p| p[0] - (p[0] % lower.spp) }
        propagateFurther = false
        lowerIntervals.uniq.each do |interval|
          propagateFuther = @whisper.send(:propagate, interval, higher, lower)
        end
        break unless propagateFurther
        higher = lower
      end
    end



  end
end
