Gem::Specification.new do |s|  
  s.name        = 'rnats'  
  s.version     = '0.0.0'  
  s.date        = '2012-10-09'  
  s.summary     = "rnats!"  
  s.description = "A simple rnats gem"  
  s.authors     = ["HarryZhang"]  
  s.email       = 'resouer@163.com'  
  s.homepage    = 'http://github.com/resouer/rnats'
  s.rubyforge_project = "rnats"

  s.files = Dir["lib/**/*.rb"]
  s.require_paths = ["lib/rnats"]

  # dependencies
  s.add_dependency('eventmachine', '>= 0.12.4')
  s.add_dependency('amqp', '0.9.8')
  s.add_dependency('amq-client', '0.9.5')
  s.add_dependency('amq-protocol', '0.9.5')
  s.add_development_dependency('rspec', '2.1.0')
end  
