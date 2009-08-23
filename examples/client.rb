require 'rubygems'
#require 'gearman'
require '../lib/gearman'
Gearman::Util.debug = true

servers = ['localhost:4730', 'localhost:4731']
  
client = Gearman::Client.new(servers)
taskset = Gearman::TaskSet.new(client)

task = Gearman::Task.new('sleep', 2)
task.on_complete {|d| puts "TASK 1: #{d}" }
taskset.add_task(task)

task = Gearman::Task.new('sleep', 2)
task.on_complete {|d| puts "TASK 2: #{d}" }
taskset.add_task(task)

taskset.wait(100)
