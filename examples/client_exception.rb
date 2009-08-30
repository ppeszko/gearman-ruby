require 'rubygems'
require '../lib/gearman'
Gearman::Util.debug = true

servers = ['localhost:4730']

client = Gearman::Client.new(servers)

task = Gearman::Task.new('fail_with_exception', "void")
task.retries = 2
task.on_complete {|d| puts d }
task.on_exception {|ex| puts "This should never be called" }
task.on_warning {|warning| puts "WARNING: #{warning}" }
task.on_retry { puts "PRE-RETRY HOOK: retry no. #{task.retries_done}" }
task.on_fail { puts "TASK FAILED, GIVING UP" }

client.run task