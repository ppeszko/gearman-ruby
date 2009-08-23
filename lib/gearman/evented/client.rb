module Gearman
  module Evented

    module ClientReactor
      include Gearman::Evented::Reactor

      def self.connect(host, port, client, opts = {})
        EM.connect(host, (port || 4730), self) do |c|
          c.instance_eval do
            @host = host
            @port = port || 4730
            @opts = opts
            @client = client
          end
        end
      end

      def receive_data(data)
        Gearman::Protocol.decode_response(data).each do |type, handle, data|
          dispatch_packet(type, handle, data)
        end
      end

      def dispatch_packet(type, handle, data)
        known_types = [ :work_complete,
                        :work_fail,
                        :work_status,
                        :work_exception,
                        :work_warning,
                        :work_data ]
        if known_types.include?(type)
          __send__("handle_#{type}".to_sym, handle, *data)
        elsif type == :job_created
          @client.job_created(handle)
        else
          log "Got #{type.to_s}, #{handle}, #{data} from #{server}"
        end

        succeed [handle, data]
      end

      def submit_job(task)
        cmd = "submit_job"
        cmd << "_#{task.priority}" if [ :high, :low ].include?(task.priority)
        cmd << "_bg" if task.background

        log "submit_job #{task.name}"
        send cmd.to_sym, [ task.name, task.hash, task.payload ].join("\0")
      end

      def handle_work_complete(handle, data)
        log "Got work_complete with handle #{handle} and #{data ? data.size : '0'} byte(s) of data from #{server}"
        call_back(handle, :on_complete, true, data)
      end

      def handle_work_fail(handle)
        log "Got work_fail with handle #{handle} from #{server}"
        task = task_in_progress(handle, true)
        if task.should_retry?
          call_back(handle, :on_retry, true, task.retries_done)
          run(task)
        else
          call_back(handle, :on_fail, true)
        end
      end

      def handle_work_status(handle, data)
        num, den = data.split("\0", 3)
        log "Got work_status with handle #{handle} from #{server}: #{num}/#{den}"
        call_back(handle, :on_status, false, num, den)
      end

      def handle_work_exception(hande, exception)
        log "Got work_exception with handle #{handle} from #{server}: '#{exception}'"
        call_back(handle, :on_exception, true, exception)
      end

      def handle_work_warning(handle, message)
        log "Got work_warning with handle #{handle} from #{server}: '#{message}'"
        call_back(handle, :on_warning, false, message)
      end

      def handle_work_data(handle, data)
        log "Got work_data with handle #{handle} and #{data ? data.size : '0'} byte(s) of data from #{server}"

        call_back(handle, :on_data, false, data)
      end

      def call_back(handle, callback, completed, *args)
        @client.task_in_progress(handle, completed).dispatch(callback.to_sym, *args)
      end
    end


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
          @job_servers.each do |hostport|
            host, port = hostport.split(":")
            @reactors << ClientReactor.connect(host, port, self, @opts)
          end

          @taskset = task_or_taskset.is_a?(Task) ? Taskset.new(task_or_taskset) : task_or_taskset
          @jobqueue = []
          create_job(@taskset.pop)
        end
      end

      def create_job(task)
        return unless task
        server = @reactors[rand(@reactors.size)]
        @jobqueue.push(task)
        server.errback { create_job(@jobqueue.pop) }
        server.submit_job(task)
      end

      def job_created(handle)
        task = @jobqueue.pop
        @jobs[handle] = task
        puts "Added handle #{handle} for job #{task}"
        create_job(@taskset.pop)
      end

      def task_in_progress(handle, remove = true)
        task = remove ? @jobs.delete(handle) : @jobs[handle]
        raise ProtocolError, "No task by that name: #{handle}" unless task
        EM.stop if @jobs.empty?
        task
      end

    end

  end
end
