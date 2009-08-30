require 'rubygems'
#require 'gearman'
require '../lib/gearman'
Gearman::Util.debug = true

servers = ['localhost:4730', 'localhost:4731']

client = Gearman::Client.new(servers)

task = Gearman::Task.new('sleep', 20, :background => true, :poll_status_interval => 1)
task.on_complete {|d| puts d } #never called
task.on_status {|d| puts "Status: #{d}"}

client.run(task)
