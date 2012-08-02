require File.expand_path('../lib/whispr/version', __FILE__)

Gem::Specification.new do |s|
  s.name          = 'whispr'
  s.version       = Whispr::VERSION
  s.summary       = 'Read and write Graphite Whisper round-robin files'
  s.description   = ''
  s.homepage      = 'http://github.com/simulacre/whispr'
  s.email         = 'whispr@simulacre.org'
  s.authors       = ['Caleb Crane']
  s.files         = Dir["lib/**/*.rb", "bin/*", "*.md"]
  s.require_paths = ["lib"]
  s.executables   = Dir['bin/whisper-info']
end
