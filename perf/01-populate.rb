#!/usr/bin/env ruby

require 'bundler/setup'
require 'benchmark'

require 'whispr'

data_dir     = File.expand_path("../data", __FILE__)
archive_list = ["30s:6h", "1m:7d", "2m:30d", "4m:120d", "8m:5y"].map{ |a| Whispr.parse_retention_def(a) }
whisprs      = []

time        = Time.new.to_i
one_value   = rand(1000)
#many_values = 87660.times.map { |i| [time - (i * 30), rand(1000) ]}
many_values = 20.times.map { |i| [time - (i * 30), rand(1000) ]}

REPS = 1#00

FileUtils.rm_rf("#{data_dir}/.")
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
        many_values.each_slice(2) do |values|
          w.update(*values)
          #values.each { |v| w.update(v) }
        end
      end
    end

  end
ensure
  FileUtils.rm_rf("#{data_dir}/.")
end
