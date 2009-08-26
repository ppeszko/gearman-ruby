module Gearman
  module Evented

    module WorkerReactor
      include Gearman::Evented::Reactor

      def worker
        @callback_handler
      end

      def connection_completed
        send :set_client_id, client_id
        @cbs_job_assign = []
        super
      end

      def announce_ability(name, timeout)
        cmd = timeout ? :can_do_timeout : :can_do
        arg = timeout ? "#{name}\0#{timeout.to_s}" : name
        log "announce_ability #{name} #{timeout}"
        send cmd, arg
      end

      def announce_disability(name)
        send :cant_do, name
      end

      def grab_job(&cb_job_assign)
        log "Grab Job"
        send :grab_job
        @cb_job_assign = cb_job_assign if cb_job_assign
      end

      def work_fail(handle)
        send :work_fail, handle
      end

      def work_complete(handle, data)
        send :work_complete, "#{handle}\0#{data}"
      end

      def work_warning(handle, message)
        send :work_warning, "#{handle}\0#{message}"
      end

      def receive_data(data)
        Gearman::Protocol.decode_response(data).each do |type, handle, *data|
          dispatch_packet(type, handle, *data)
        end
      end

      def dispatch_packet(type, handle, *data)
        success = true
        timer   = 0
        case type
        when :no_job
          send :pre_sleep
          timer = @opts[:reconnect_sec] || 10
        when :job_assign
          # handle_job_assign(handle, *data)
          log "job assign #{handle}, #{data}"
          @cb_job_assign.call(handle, data[0], data[1])
        when :noop
          log "NOOP"
        when :error
          log "[ERROR]: error from server #{server}: #{data}"
        else
          log "Got unknown #{type} from #{server}"
        end

        EM.add_timer(timer) { grab_job }
        succeed [handle, data]
      end

      def client_id
        @client_id ||= begin
          chars = ('a'..'z').to_a
          Array.new(30) { chars[rand(chars.size)] }.join
        end
      end

    end

  end
end
