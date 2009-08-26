module Gearman
  class Taskset

    def initialize(tasks = [])
      @tasks = tasks
    end

    def add(task)
      @tasks << task
      self
    end
    alias :<< :add

    def each
      @tasks.each {|task| yield task }
    end

    def pop
      @tasks.pop
    end

    def shift
      @tasks.shift
    end

    def size
      @tasks.size
    end
  end
end
