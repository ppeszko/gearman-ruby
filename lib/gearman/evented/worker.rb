module Gearman
  module Evented

    module WorkerReactor
      include Gearman::Evented::Reactor

      def self.connect(host, port, worker, opts = {})
        EM.connect(host, (port || 4730), self) do |c|
          c.instance_eval do
            @host = host
            @port = port || 4730
            @opts = opts
            @worker = worker
          end
        end
      end

      def connection_completed
        send :set_client_id, client_id
      end

      def announce_ability(name, timeout)
        cmd = timeout ? :can_do_timeout : :can_do
        arg = timeout ? "#{name}\0#{timeout.to_s}" : name
        send cmd, arg
      end

      def announce_disability(name)
        send :cant_do, name
      end

      def grab_job
        log "Grab Job"
        send :grab_job
      end

      def receive_data(data)
        Gearman::Protocol.decode_response(data).each do |type, handle, data|
          dispatch_packet(type, handle, data)
        end
      end

      def dispatch_packet(type, handle, data)
        success = true
        timer   = 0
        case type
        when :no_job
          send :pre_sleep
          timer = @opts[:reconnect_sec] || 10
        when :job_assign
          handle_job_assign(handle, data)
        when :noop
          log "NOOP"
        when :error
          log "[ERROR]: error from server #{server}: #{data}"
          success = false
        else
          log "Got unknown #{type} from #{server}"
          success = false
        end

        EM.add_timer(timer) { grab_job }
        success ? succeed([handle, data]) : fail([handle, data])
      end

      def client_id
        @client_id ||= begin
          chars = ('a'..'z').to_a
          Array.new(30) { chars[rand(chars.size)] }.join
        end
      end


      private

        def handle_job_assign(handle, data)
          func, data = data.split("\0", 3)
          if not func
            log "ERROR: Ignoring job_assign with no function from #{server}"
            return
          end

          log "Got job_assign with handle #{handle} and #{data.size} byte(s) from #{server}"

          if !@worker.has_ability?(func)
            log "Ignoring job_assign for unsupported func #{func} with handle #{handle} from #{server}"
            send :work_fail, handle
            return
          end

          exception = nil
          begin
            ret = @worker.abilities[func].call(data, Job.new(self, handle))
          rescue Exception => e
            exception = e
          end

          if ret && exception.nil?
            ret = ret.to_s
            log "Sending work_complete for #{handle} with #{ret.size} byte(s) to #{server}"
            send :work_complete, "#{handle}\0#{ret}"
          elsif exception.nil?
            log "Sending work_fail for #{handle} to #{server}"
            send :work_fail, handle
          elsif exception
            log "Sending work_warning, work_fail for #{handle} to #{server}"
            send :work_warning, "#{handle}\0#{exception.message}"
            send :work_fail, handle
          end
        end

    end

    class Worker

      attr_reader :abilities

      def initialize(job_servers, opts = {})
        @reactors = []
        @abilities = {}

        @job_servers = if job_servers.is_a?(String)
          [ job_servers ]
        else
          job_servers
        end

        @opts = opts
      end

      def add_ability(name, timeout = nil, &f)
        remove_ability(name) if @abilities.has_key?(name)
        @abilities[name] = f
        # @reactors.each {|reactor| reactor.announce_ability(name, timeout) }
      end

      def remove_ability(name)
        @abilities.delete(name)
        # @reactors.each {|reactor| reactor.announce_disability(name) }
      end

      def has_ability?(name)
        @abilities.has_key?(name)
      end

      def work
        EM.run {
          @job_servers.each do |hostport|
            host, port = hostport.split(":")
            @reactors << WorkerReactor.connect(host, port, self, @opts)
          end
          @reactors.each do |reactor|
            @abilities.keys.each do |ability|
              reactor.announce_ability(ability, nil)
            end
            reactor.grab_job
          end
        }
      end
    end

  end
end
