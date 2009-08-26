module Gearman
  class Client
    attr_accessor :uniq, :jobs

    def initialize(job_servers, opts = {})
      @reactors = []
      @jobs = {}

      @job_servers = if job_servers.is_a?(String)
        [ job_servers ]
      else
        job_servers
      end

      @uniq = opts.delete(:uniq)
      @opts = opts
    end

    def run(task_or_taskset)
      EM.run do
        @taskset = task_or_taskset.is_a?(Task) ? Taskset.new(task_or_taskset) : task_or_taskset

        @job_servers.each do |hostport|
          host, port = hostport.split(":")
          reactor = Gearman::Evented::ClientReactor.connect(host, port, self, @opts)
          reactor.callback { create_job(@taskset.shift, reactor) }
          @reactors << reactor
        end
      end
    end

    def create_job(task, reactor = nil)
      return unless task
      server = reactor || @reactors[rand(@reactors.size)]
      unless server.connected?
        log "create_job: server #{server} not connected"
        create_job(task)
        return
      end

      server.submit_job(task) do |handle|
        log "create_job succeeded: #{handle}"
        @jobs[handle] = task
        create_job(@taskset.shift)
        server.dispatch_packet_callback do |type, handle, data|
          dispatch(type, handle, data)
        end
      end
    end

    def dispatch(type, handle, args)
      log "dispatch #{type}, #{handle}, #{args}"
      return unless type
      task = @jobs[handle]
      raise ProtocolError, "No task by that name: #{handle}" unless task

      if :work_fail == type && task.should_retry?
        task.dispatch(:on_retry, task.retries_done)
        create_job(task)
        return
      end

      task.dispatch(type.to_s.sub("work", "on").to_sym, *args)
      @jobs.delete(handle) unless [:work_status, :work_exception].include?(type)
      EM.stop if @jobs.empty?
    end

    private

      def log(msg)
        Gearman::Util.log msg
      end
  end
end
