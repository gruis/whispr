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
  end
end
