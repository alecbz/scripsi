require 'rubygems'
require 'rake'
require 'echoe'

Echoe.new('scripsi','0.0.1') do |p|
  p.description = "a flexible text-searching library built on top of redis"
  p.url = "https://github.com/alecbenzer/scripsi"
  p.author = "Alec Benzer"
  p.email = "alecbenzer@gmail.com"
  p.ignore_pattern = ["*.rdb"]
  p.development_dependencies = ["redis >=2.1.1"]
end

require './lib/scripsi'
require 'benchmark'
Scripsi.connect

namespace :bm do
  task :clear do
    indx = Scripsi.indexer "test"
    puts Benchmark.measure { indx.destroy } if indx
  end

  task :add do
    file = File.open("./tests/war_and_peace.txt")
    idxr = Scripsi::Indexer.new "test"
    i = 0
    t = Benchmark::Tms.new
    file.each_line do |line|
      t += Benchmark.measure{ idxr.index(i,line) }
      i += 1
      if i % 1000 == 0
        puts "line #{i}", t
        puts "\t#{Scripsi.redis.info["used_memory_human"]}"
      end
    end
  end

  task :populate => [:clear, :add]

end
