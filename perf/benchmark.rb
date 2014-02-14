#!/usr/bin/env ruby

require 'bundler/setup'
require 'benchmark'
require 'fileutils'

require 'whispr'

data_dir     = File.expand_path("../data", __FILE__)
FileUtils.mkdir_p data_dir
archive_list = ["30s:6h", "1m:7d", "2m:30d", "4m:120d", "8m:5y"].map{ |a| Whispr.parse_retention_def(a) }
whisprs      = []

time        = Time.new.to_i
one_value   = rand(1000)

fetches = {
  "1m"  => [Time.at(time) - 60, Time.at(time)],
  "10m" => [Time.at(time) - 600, Time.at(time)],
  "1h"  => [Time.at(time) - 3600, Time.at(time)],
  "1d"  => [Time.at(time) - 86400, Time.at(time)],
  "1w"  => [Time.at(time) - 604800, Time.at(time)],
  "1M"  => [Time.at(time) - 2.62974e6, Time.at(time)],
  "6M"  => [Time.at(time) - 1.57785e7, Time.at(time)],
}

# We start from 2 instead of 1 because there is currently an error preventing
# the updating of a point that has already been saved. The call to update in
# the "update one" block will save the first point. If we allow many_values to
# start from 1 instead of 2, the #update call in "udpate many" will update the
# same point. The exception we'd get would be something like:
# #<Whispr::ArchiveBoundaryExceeded: archiveEnd=8716 pos=8717 bytesBeyond=12 len(packedString)=120>
many_values = (2..87660).map { |i| [time - (i * 30), rand(1000) ]}

REPS = 20

FileUtils.rm_rf("#{data_dir}/.")
puts "Benchmark data at #{REPS} repetitions"
begin
  Benchmark.bm(15) do |x|
    x.report("create") do
      REPS.times do |i|
        Whispr.create(File.join(data_dir, "data-#{i}.wsp"), archive_list)
      end
    end

    x.report("open") do
      REPS.times do |i|
        whisprs << Whispr.new(File.join(data_dir, "data-#{i}.wsp"))
      end
    end

    x.report("update one") do
      whisprs.each do |w|
        w.update([time, one_value])
      end
    end

    x.report("update many") do
      whisprs.each do |w|
        many_values.each_slice(1000) do |values|
          w.update(*values)
        end
      end
    end

    fetches.each do |label, interval|
      x.report("fetch #{label}") do
        whisprs.each do |w|
          w.fetch(*interval)
        end
      end
    end

  end
ensure
  FileUtils.rm_rf("#{data_dir}/.")
end
