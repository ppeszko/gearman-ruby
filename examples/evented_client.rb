require 'rubygems'
require '../lib/gearman'

Gearman::Util.debug = true

servers = ['localhost:4730', 'localhost:4731']
client = Gearman::Client.new(servers)

taskset = Gearman::Taskset.new

task = Gearman::Task.new('sleep', 2)
task.on_status {|numerator, denominator| puts "TASK 1: Completed #{numerator} of #{denominator}"}
task.on_complete {|d| puts "TASK 1: #{d}" }
taskset << task

task = Gearman::Task.new('sleep', 3)
task.on_status {|numerator, denominator| puts "TASK 2: Completed #{numerator} of #{denominator}"}
task.on_complete {|d| puts "TASK 2: #{d}" }
taskset << task

client.run(taskset)
